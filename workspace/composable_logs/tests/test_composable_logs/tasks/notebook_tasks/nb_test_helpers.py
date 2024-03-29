from pathlib import Path

# -
from composable_logs.notebooks_helpers import JupytextNotebookContent


def get_test_jupytext_nb(test_nb: str) -> JupytextNotebookContent:
    return JupytextNotebookContent(
        filepath=test_nb,
        content=(
            Path(__file__).parent / "jupytext_test_notebooks" / test_nb
        ).read_text(),
    )
