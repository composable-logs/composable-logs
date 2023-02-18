import os

# -
from fastapi import FastAPI, Request

# ---- start ML Flow server to catch requests ----
from fastapi import FastAPI, Request, Depends, HTTPException

from ray import serve
from fastapi.security import HTTPBasic, HTTPBasicCredentials

# -
from composable_logs.tasks.task_opentelemetry_logging import get_task_context
from composable_logs.wrappers import _get_traceparent


MLFLOW_PORT = 4942
MLFLOW_HOST = "localhost"


def get_api():

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
            print("CL: /api/2.0/mlflow/runs/create (post) REQ ", await request.json())
            response = {
                "run": {"info": {"run_id": traceparent, "run_uuid": traceparent}}
            }
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
            # Implementing this seems necessary since the client uses this to fetch
            # artifact_uri, and this determines storage location for temp files used
            # by client.
            assert await request.body() == b""
            print("CL: /api/2.0/mlflow/runs/get (get)")
            response = {
                "run": {
                    "info": {
                        "run_id": traceparent,
                        "run_uuid": traceparent,
                        # strangely this seems to influence where client write temp
                        # files on local file system before sending them to server (!)
                        "artifact_uri": f"/tmp/{traceparent}/",
                    }
                }
            }
            print("CL: /api/2.0/mlflow/runs/create : RESP ", response)
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

        @app.post("/api/2.0/mlflow/runs/set-tag")
        async def mlflow_runs_get(
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

        # --- tags ---

        # --- the below endpoints are not part of the API but useful for testing ---

        @app.get("/status")
        def is_alive(self):
            # this is not part of ML Flow API, but useful to testing API
            return {"status": "OK"}

        @app.get("/{rest_of_path:path}")
        def catch_all_get(self, rest_of_path):
            print("Recieved GET query", rest_of_path)
            # https://mlflow.org/docs/latest/rest-api.html#set-experiment-tag
            return HTTPException(
                status_code=501,
                detail="query {rest_of_path} not supported in state-less "
                "mlflow-to-opentelemetry log collector",
            )
            return {}

        @app.post("/{rest_of_path:path}")
        def catch_all_post(self, rest_of_path):
            print("Recieved POST query", rest_of_path)
            return {}

    return ServerToCaptureMLFlowData


def is_running() -> bool:
    try:
        deployments = serve.list_deployments()
        return "ServerToCaptureMLFlowData" in deployments
    except:
        return False


def ensure_running():
    if is_running():
        return

    serve.run(get_api().bind(), host=MLFLOW_HOST, port=MLFLOW_PORT)


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
    os.environ["MLFLOW_TRACKING_PASSWORD"] = "a-weak-password"
    os.environ["MLFLOW_TRACKING_URI"] = f"http://{MLFLOW_HOST}:{MLFLOW_PORT}"

    # Avoid ML Flow client hanging due to configuration errors.
    # We are running ML Flow in same setup/cluster as all other tasks.
    #
    # https://mlflow.org/docs/latest/python_api/mlflow.environment_variables.html
    os.environ["MLFLOW_HTTP_REQUEST_MAX_RETRIES"] = "1"  # default 5
    os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "5"  # default 120
