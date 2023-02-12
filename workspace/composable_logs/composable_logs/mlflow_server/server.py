import os

# -
from fastapi import FastAPI, Request

# ---- start ML Flow server to catch requests ----
from fastapi import FastAPI, Request, Depends
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
        @app.post("/api/2.0/mlflow/runs/create")
        async def post_runs_create(
            self, request: Request, traceparent: str = Depends(_get_traceparent)
        ):
            print(
                "POST >>> /api/2.0/mlflow/runs/create :: reqs json = ",
                await request.json(),
            )
            print("POST >>> /api/2.0/mlflow/runs/create :: creds = ", traceparent)

            # When client starts a new id, the server responds with id for that run.
            # see https://mlflow.org/docs/latest/rest-api.html#mlflowruninfo
            new_run_id = "a-new-run-id"
            return {"run": {"info": {"run_id": new_run_id, "run_uuid": new_run_id}}}

        @app.post("/api/2.0/mlflow/runs/log-parameter")
        async def post_runs_log_parameter(
            self, request: Request, traceparent: str = Depends(_get_traceparent)
        ):
            request_json = await request.json()
            print("POST >>> /api/2.0/mlflow/runs/log-parameter", request_json)

            assert request_json.keys() == {"run_uuid", "run_id", "key", "value"}

            ctx = get_task_context(P={"_opentelemetry_traceparent": traceparent})
            ctx.log_string(request_json["key"], request_json["value"])
            return {}

        @app.post("/api/2.0/mlflow/runs/update")
        async def post_runs_update(self, request: Request):
            print("POST >>> /api/2.0/mlflow/runs/update", await request.json())
            return {}

        @app.get("/status")
        def is_alive(self):
            # this is not part of ML Flow API, but useful to testing API
            return {"status": "OK"}

        @app.get("/{rest_of_path:path}")
        def catch_all_get(self, rest_of_path):
            print("Recieved GET query", rest_of_path)
            return {}

        @app.post("/{rest_of_path:path}")
        def catch_all_post(self, rest_of_path):
            print("Recieved POST query", rest_of_path)
            return {}

    return ServerToCaptureMLFlowData


def is_running() -> bool:
    try:
        deployments = serve.list_deployments()
    except:
        deployments = {}
    return "ServerToCaptureMLFlowData" in deployments


def ensure_running() -> bool:
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
