from pathlib import Path

#
from pynb_dag_runner.tasks.tasks import make_jupytext_task_ot
from pynb_dag_runner.notebooks_helpers import JupytextNotebook

TEST_NOTEBOOK_PATH = (Path(__file__).parent) / "jupytext_test_notebooks"


def make_test_nb_task(nb_name: str, parameters={}, timeout_s: float = 10.0):
    return make_jupytext_task_ot(
        notebook=JupytextNotebook(TEST_NOTEBOOK_PATH / nb_name),
        tmp_dir=TEST_NOTEBOOK_PATH,
        timeout_s=timeout_s,
        parameters=parameters,
    )
