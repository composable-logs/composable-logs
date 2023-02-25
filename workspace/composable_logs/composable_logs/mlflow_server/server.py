import os, uuid
from pathlib import Path

# -
import ray

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer

# -
from composable_logs.tasks.task_opentelemetry_logging import get_task_context
from composable_logs.wrappers import _get_traceparent


# ---- start ML Flow server to catch requests ----

# MLFlow server runs on some node in the Ray cluster
MLFLOW_PORT = 5001
MLFLOW_HOST = "localhost"

# OpenTelemetry context propagation to the MLFlow server is done using username and
# password data. Thus, the below static password is not providing security. Rather it
# ensures that requests are correctly configured. Similarly, the ftp server allow
# anonymous write access.
MLFLOW_SERVER_PASSWORD = "composable-logs-login"

# Clients request the IP address to the MLFlow server from named actors in the
# cluster, and port defined above.
MLFLOW_SERVER_ACTOR_NAME = "composable-logs-mlflow-server"

# The FTP server runs on potentially another node in the Ray cluster. Clients upload
# artifacts to the ftp server as redirected by the MLFlow server.
COMPOSABLE_LOGS_MLFLOW_FTP_SERVER_PORT = 5002


def get_api(ftp_server_ip: str, ftp_server_port: int) -> FastAPI:

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
        if credentials.password != MLFLOW_SERVER_PASSWORD:
            raise Exception(
                "MLFlow client is not correctly set up! Please set environment "
                "variables by calling configure_mlflow_connection_variables before "
                "logging using the MLFlow client."
            )
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

    # --- runs related endpoints ---

    @app.post("/api/2.0/mlflow/runs/create")
    async def post_runs_create(
        request: Request, traceparent: str = Depends(_get_traceparent)
    ):
        """
        When client starts a new id, the server responds with id for that run.

        REST API Documentation:
        https://mlflow.org/docs/latest/rest-api.html#mlflowruninfo
        """
        request_json = await request.json()
        tags = request_json.get("tags", [])
        if "mlflow.parentRunId" in [entry["key"] for entry in tags]:
            raise HTTPException(
                status_code=501,
                detail=f"POST /api/2.0/mlflow/runs/create --- nested runs are not supported",
            )

        print("CL: /api/2.0/mlflow/runs/create : POST REQ ", request_json)
        response = _run_response_json(traceparent)
        print("CL: /api/2.0/mlflow/runs/create : POST RESP ", response)
        return response

    @app.post("/api/2.0/mlflow/runs/update")
    async def post_runs_update(request: Request):
        print("POST >>> /api/2.0/mlflow/runs/update", await request.json())
        return {}

    @app.get("/api/2.0/mlflow/runs/get")
    async def mlflow_runs_get(
        request: Request, traceparent: str = Depends(_get_traceparent)
    ):
        print("CL: /api/2.0/mlflow/runs/get: GET")
        response = _run_response_json(traceparent)
        print("CL: /api/2.0/mlflow/runs/get: RESP ", response)
        return response

    # --- experiment data ---

    @app.post("/api/2.0/mlflow/runs/log-parameter")
    async def post_runs_log_parameter(
        request: Request, traceparent: str = Depends(_get_traceparent)
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
        request: Request, traceparent: str = Depends(_get_traceparent)
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
        # TODO: batch ingestion of metrics not yet supported.

        return {}

    # --- tags ---

    @app.post("/api/2.0/mlflow/runs/set-tag")
    async def mlflow_runs_set_tag(
        request: Request, traceparent: str = Depends(_get_traceparent)
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
        request: Request, traceparent: str = Depends(_get_traceparent)
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

    # --- the below endpoints are not part of the MLFlow API but useful for testing ---

    @app.get("/status")
    def status():
        return {"status": "OK"}

    @app.get("/{rest_of_path:path}")
    def catch_all_get(rest_of_path):
        print("Recieved unknown GET request", rest_of_path)
        raise HTTPException(
            status_code=501,
            detail=f"GET {rest_of_path} not supported in state-less "
            "mlflow-to-opentelemetry log collector",
        )

    @app.post("/{rest_of_path:path}")
    def catch_all_post(rest_of_path):
        print("Recieved unknown POST request", rest_of_path)
        raise HTTPException(
            status_code=501,
            detail=f"POST {rest_of_path} not supported in state-less "
            "mlflow-to-opentelemetry log collector",
        )

    return app


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


@ray.remote(num_cpus=0)
class MLFlowServer:
    def __init__(self):
        pass

    def get_server_ip_address(self):
        return ray.get_runtime_context().worker.node_ip_address

    def start_server(self):

        # --- start ftp-server ---
        ftp_actor = (
            ArtifactFTPServer
            # -
            .remote(tmp_dir=Path(f"/tmp/composable-logs/ftp-{uuid.uuid4()}"))  # type: ignore
        )
        ftp_ip: str = ray.get(ftp_actor.get_server_ip_address.remote())
        ftp_actor.start_ftp_server.remote()  # blocking call and actor will no longer respond

        import uvicorn

        app = get_api(
            ftp_server_ip=ftp_ip,
            ftp_server_port=COMPOSABLE_LOGS_MLFLOW_FTP_SERVER_PORT,
        )

        uvicorn.run(app, host="0.0.0.0", port=MLFLOW_PORT)


# --- control functions ---


def _get_actor_ip(actor_id: str):

    from ray.experimental.state.api import get_actor, get_node

    actor = get_actor(actor_id)
    if actor is None:
        raise Exception(f"Unable to find actor with id={actor}")

    node = get_node(actor["node_id"])
    return node["node_ip"]  # type: ignore


def get_actor_ip(actor_id: str):
    # Ray reports internal API error without this retry logic
    e = None
    for _ in range(5):
        try:
            return _get_actor_ip(actor_id)
        except BaseException as _e:
            import time

            time.sleep(1)
            e = _e

    raise e  # type: ignore


def get_mlflow_server_ip() -> str:
    # return IP for MLFlow server on this Ray cluster.
    #
    # Raise an exception if server is not running
    actor = ray.get_actor(MLFLOW_SERVER_ACTOR_NAME, namespace="pydar-ray-cluster")
    return get_actor_ip(actor._actor_id.hex())


def mlflow_server_is_running() -> bool:
    try:
        mlflow_ip = get_mlflow_server_ip()
        return True
    except:
        return False


def ensure_mlflow_server_is_running():
    if mlflow_server_is_running():
        return

    # --- start ML Flow server ---
    mlflow_server_actor = (
        MLFlowServer
        # -
        .options(name=MLFLOW_SERVER_ACTOR_NAME)  # type: ignore
        # -
        .remote()  # type: ignore
    )

    # The below is non-blocking call to actor, but actor-method is blocking
    # and actor will no longer respond. But will server API requests.
    mlflow_server_actor.start_server.remote()

    # Poll the ML Flow /status API until the server has started and starts to respond
    # See: https://stackoverflow.com/a/35504626
    import requests
    from requests.adapters import HTTPAdapter, Retry

    session = requests.Session()
    session.mount(
        "http://", HTTPAdapter(max_retries=Retry(total=20, backoff_factor=0.1))
    )
    status_url = f"http://{get_mlflow_server_ip()}:{MLFLOW_PORT}/status"
    print("Checking MLFlow server is running using {status_url}")
    response = session.get(url=status_url)
    if response.json() == {"status": "OK"}:
        return True

    raise Exception(
        "Could not start MLFlow."
        "Status endpoint {status_url} returns {response}."
        "Response JSON = {response.json()}"
    )


def shutdown_mlflow_server():
    ray.kill(ray.get_actor(MLFLOW_SERVER_ACTOR_NAME, namespace="pydar-ray-cluster"))


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
    #
    # https://mlflow.org/docs/latest/tracking.html?highlight=mlflow_tracking_username#logging-to-a-tracking-server
    os.environ["MLFLOW_TRACKING_USERNAME"] = traceparent
    os.environ["MLFLOW_TRACKING_PASSWORD"] = MLFLOW_SERVER_PASSWORD

    # get IP to named actor running the MLFlow server on this Ray cluster.
    # So all clients connect to the same MLFlow server on the Ray cluster.
    mlflow_server_ip = get_mlflow_server_ip()
    os.environ["MLFLOW_TRACKING_URI"] = f"http://{mlflow_server_ip}:{MLFLOW_PORT}"

    # Avoid ML Flow client hanging due to configuration errors.
    # We are running ML Flow in same setup/cluster as all other tasks.
    #
    # https://mlflow.org/docs/latest/python_api/mlflow.environment_variables.html
    os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "1"  # default 5
    os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "5"  # default 120
