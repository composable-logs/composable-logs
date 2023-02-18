import os, uuid

# -
from fastapi import FastAPI, Request

# -
import ray
from pathlib import Path

from fastapi import FastAPI, Request, Depends, HTTPException

from ray import serve
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

# -
from composable_logs.tasks.task_opentelemetry_logging import get_task_context
from composable_logs.wrappers import _get_traceparent


# ---- start ML Flow server to catch requests ----


COMPOSABLE_LOGS_MLFLOW_FTP_SERVER_PORT = 2449

MLFLOW_PORT = 4942
MLFLOW_HOST = "localhost"


def get_api(ftp_server_ip: str, ftp_server_port: int):

    app = FastAPI()
    _security = HTTPBasic()

    def _get_traceparent(credentials: HTTPBasicCredentials = Depends(_security)):
        """
        ML Flow clients use username + password to authenticate.

        The username is the span-id to use as parent-span-id when logging data.

        https://fastapi.tiangolo.com/advanced/security/http-basic-auth/
        """
        traceparent: str = credentials.username
        assert isinstance(traceparent, str)
        # raise Exception("_get_traceparent" + traceparent)
        return traceparent

    def _run_response_json(traceparent: str):
        return {
            "run": {
                "info": {
                    "run_id": traceparent,
                    "run_uuid": traceparent,  # deprecated
                    # We return endpoint to our ftp-server that will accept artifacts.
                    # From the path we can deduce which task is the parent for the file.
                    "artifact_uri": f"ftp://{ftp_server_ip}:{ftp_server_port}/{traceparent}/",
                }
            }
        }

    @serve.deployment(route_prefix="/")
    @serve.ingress(app)
    class ServerToCaptureMLFlowData:

        # --- runs related endpoints ---

        @app.post("/api/2.0/mlflow/runs/create")
        async def post_runs_create(
            self, request: Request, traceparent: str = Depends(_get_traceparent)
        ):
            """
            When client starts a new id, the server responds with id for that run.

            API Documentation:
            https://mlflow.org/docs/latest/rest-api.html#mlflowruninfo
            """
            print("CL: /api/2.0/mlflow/runs/create : POST REQ ", await request.json())
            response = _run_response_json(traceparent)
            print("CL: /api/2.0/mlflow/runs/create : RESP ", response)
            return response

        @app.post("/api/2.0/mlflow/runs/update")
        async def post_runs_update(self, request: Request):
            print("POST >>> /api/2.0/mlflow/runs/update", await request.json())
            return {}

        @app.get("/api/2.0/mlflow/runs/get")
        async def mlflow_runs_get(
            self, request: Request, traceparent: str = Depends(_get_traceparent)
        ):
            print("CL: /api/2.0/mlflow/runs/get: GET")
            response = _run_response_json(traceparent)
            print("CL: /api/2.0/mlflow/runs/get: RESP ", response)
            return response

        # --- experiment data ---

        @app.post("/api/2.0/mlflow/runs/log-parameter")
        async def post_runs_log_parameter(
            self, request: Request, traceparent: str = Depends(_get_traceparent)
        ):
            # https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_param
            request_json = await request.json()
            print("CL: /api/2.0/mlflow/runs/log-parameter (post) REQ ", request_json)

            assert request_json.keys() == {"run_uuid", "run_id", "key", "value"}
            assert isinstance(request_json["key"], str)
            assert isinstance(request_json["value"], str)

            ctx = get_task_context(P={"_opentelemetry_traceparent": traceparent})

            # Logged as string; as noted in MLFlow log parameter accepts Any input
            # but content is stringified before storage.
            ctx.log_string(request_json["key"], request_json["value"])
            return {}

        @app.post("/api/2.0/mlflow/runs/log-batch")
        async def post_runs_log_batch(
            self, request: Request, traceparent: str = Depends(_get_traceparent)
        ):
            # Batch ingestion of key-value parameters and metrics
            # https://mlflow.org/docs/latest/rest-api.html#log-batch
            request_json = await request.json()
            print("CL: /api/2.0/mlflow/runs/log-batch (post) REQ ", request_json)
            ctx = get_task_context(P={"_opentelemetry_traceparent": traceparent})

            assert request_json.keys() <= {"run_id", "metrics", "params"}

            assert isinstance(request_json.get("params", []), list)
            for param in request_json.get("params", []):
                assert isinstance(param["key"], str)
                assert isinstance(param["value"], str)
                # Logged as string; as noted in MLFlow log parameter accepts Any input
                # but content is stringified before storage.
                ctx.log_string(param["key"], param["value"])

            assert isinstance(request_json.get("metrics", []), list)
            # metrics not yet supported.

            return {}

        # --- tags ---

        @app.post("/api/2.0/mlflow/runs/set-tag")
        async def mlflow_runs_set_tag(
            self, request: Request, traceparent: str = Depends(_get_traceparent)
        ):
            # Server REST API:
            # https://mlflow.org/docs/latest/rest-api.html#set-tag
            #
            # Client SDK API:
            # https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.set_tag
            request_json = await request.json()
            print("CL: /api/2.0/mlflow/runs/set-tag (post) REQ ", request_json)
            ctx = get_task_context(P={"_opentelemetry_traceparent": traceparent})

            assert request_json.keys() == {"run_uuid", "run_id", "key", "value"}
            assert isinstance(request_json["key"], str)
            assert isinstance(request_json["value"], str)
            ctx.log_string("tags." + request_json["key"], request_json["value"])
            return {}

        # --- metrics ---

        @app.post("/api/2.0/mlflow/runs/log-metric")
        async def post_runs_log_metric(
            self, request: Request, traceparent: str = Depends(_get_traceparent)
        ):
            # Server REST API:
            # https://mlflow.org/docs/latest/rest-api.html#log-metric
            #
            # Client SDK API:
            # https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_metric
            request_json = await request.json()
            print("CL: /api/2.0/mlflow/runs/log-metric (post) REQ ", request_json)

            assert request_json.keys() == {
                "run_uuid",
                "run_id",
                "key",
                "value",
                "timestamp",
                "step",
            }
            assert isinstance(request_json["key"], str)
            assert isinstance(request_json["value"], float)

            ctx = get_task_context(P={"_opentelemetry_traceparent": traceparent})
            ctx.log_float(request_json["key"], request_json["value"])
            return {}

        # --- the below endpoints are not part of the API but useful for testing ---

        @app.get("/status")
        def is_alive(self):
            return {"status": "OK"}

        @app.get("/{rest_of_path:path}")
        def catch_all_get(self, rest_of_path):
            print("Recieved unknown GET request", rest_of_path)
            raise HTTPException(
                status_code=501,
                detail=f"GET {rest_of_path} not supported in state-less "
                "mlflow-to-opentelemetry log collector",
            )

        @app.post("/{rest_of_path:path}")
        def catch_all_post(self, rest_of_path):
            print("Recieved unknown POST request", rest_of_path)
            raise HTTPException(
                status_code=501,
                detail=f"POST {rest_of_path} not supported in state-less "
                "mlflow-to-opentelemetry log collector",
            )

    return ServerToCaptureMLFlowData


# --- FTP Server ---


def _split_path(p: Path):
    """
    split non-absolute Path into a tuple:
      "first directory" (as string) and "rest of path" (as Path)

    Eg.,
      _split_path(Path(abc/<somepath>)) == "abc", Path(<somepath)
    """
    assert not p.is_absolute()
    parts = p.parts
    assert len(parts) >= 2

    first_part, rest = parts[0], parts[1:]

    return first_part, Path("/".join(rest))


@ray.remote(num_cpus=0)
class ArtifactFTPServer:
    def __init__(self, tmp_dir: Path):
        assert isinstance(tmp_dir, Path)
        assert str(tmp_dir).startswith("/")

        self.tmp_dir = tmp_dir

    def get_server_ip_address(self):
        # With Ray 2.2 this method should not be needed if we install ray[default].
        #
        # https://github.com/ray-project/ray/issues/7431
        #
        # ray.experimental.state.api.get_actor
        # https://docs.ray.io/en/latest/ray-observability/state/ray-state-api-reference.html
        #
        return ray.get_runtime_context().worker.node_ip_address

    def start_ftp_server(self):

        # ensure work directory exist
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

        # v---
        #
        # The below is based on example codes provided by pyftpdlib
        # (MIT licensed, 2/2023)
        #
        # https://pyftpdlib.readthedocs.io/en/latest/tutorial.html
        authorizer = DummyAuthorizer()

        # Give anonymous user permissions: write (w) + mkdir (m) + cd (e)
        authorizer.add_anonymous(str(self.tmp_dir), perm="wme")

        class CustomFTPHandler(FTPHandler):
            def __repr__(self):
                # Temp fix: FTPHandler has a custom __repr__ handler: When logging from
                # on_file_received (when files are already closed) this seems to fail
                # with exception "ValueError: I/O operation on closed file".
                # Unless we something like the below:
                try:
                    return super().__repr__()
                except Exception as e:
                    return "__repr__ generated Exception: " + str(e)

            def on_file_received(_self, file: str):
                # do something when a file has been received
                assert isinstance(file, str)

                # get path of form <traceparent>/<path of artifact in ml flow>
                relative_path: Path = Path(file).relative_to(self.tmp_dir)

                traceparent, relative_path = _split_path(relative_path)
                ctx = get_task_context(P={"_opentelemetry_traceparent": traceparent})
                ctx.log_artefact(str(relative_path), Path(file).read_bytes())
                os.remove(file)

            def on_incomplete_file_received(self, file: str):
                os.remove(file)

        handler = CustomFTPHandler
        handler.authorizer = authorizer

        address = ("", COMPOSABLE_LOGS_MLFLOW_FTP_SERVER_PORT)
        server = FTPServer(address, handler)

        server.serve_forever()
        # ^---


# --- control functions ---


def is_running() -> bool:
    try:
        deployments = serve.list_deployments()
        return "ServerToCaptureMLFlowData" in deployments
    except:
        return False


def ensure_running():
    if is_running():
        return

    # start ftp-server
    ftp_actor = (
        ArtifactFTPServer
        # -
        .remote(tmp_dir=Path(f"/tmp/composable-logs/ftp-{uuid.uuid4()}"))  # type: ignore
    )
    ftp_ip: str = ray.get(ftp_actor.get_server_ip_address.remote())
    ftp_actor.start_ftp_server.remote()  # blocking call and actor will no longer respond

    # start ML Flow API
    serve.run(
        get_api(
            ftp_server_ip=ftp_ip,
            ftp_server_port=COMPOSABLE_LOGS_MLFLOW_FTP_SERVER_PORT,
        ).bind(),
        host=MLFLOW_HOST,
        port=MLFLOW_PORT,
    )


def shutdown() -> bool:
    # note: this will shutdown all services
    return serve.shutdown()


def configure_mlflow_connection_variables():
    """
    To be run inside Ray workflow.

    Set up ML Flow connection variables so client identifes as correct task,
    and ML Flow data is captured to the correct task.
    """
    traceparent: str = _get_traceparent()

    if not isinstance(traceparent, str):
        raise ValueError(
            f"Expected OpenTelemetry traceparent to be string. Got {traceparent}"
        )
    os.environ["GIT_PYTHON_REFRESH"] = "quiet"

    # Note this works assuming all tasks are run in different Python processes
    os.environ["MLFLOW_TRACKING_USERNAME"] = traceparent
    os.environ["MLFLOW_TRACKING_PASSWORD"] = "no-password"
    os.environ["MLFLOW_TRACKING_URI"] = f"http://{MLFLOW_HOST}:{MLFLOW_PORT}"

    # Avoid ML Flow client hanging due to configuration errors.
    # We are running ML Flow in same setup/cluster as all other tasks.
    #
    # https://mlflow.org/docs/latest/python_api/mlflow.environment_variables.html
    os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "1"  # default 5
    os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "5"  # default 120
