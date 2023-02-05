from pathlib import Path
import json

#
import pytest

#
from composable_logs.notebooks_helpers import JupytextNotebookContent

# Test Jupyter notebooks stored in the Jupytext py-percent file format.
TEST_JUPYTEXT_OK_NOTEBOOK = """# %%
# %% tags=["parameters"]
# %%
# Example comment
print(1 + 12 + 123)
# %%
print(f"variable_a={variable_a}")
# %%
"""

TEST_JUPYTEXT_FAIL_NOTEBOOK = """# %%
# %% tags=["parameters"]
# %%
# Example comment
print(1234 + 234 + 54 + 6)
# %%
raise Exception("Failed notebook123")
# %%
"""


@pytest.fixture
def ok_jupytext_notebook() -> JupytextNotebookContent:
    return JupytextNotebookContent(
        filepath=Path("nb/ok_notebook.py"),
        content=TEST_JUPYTEXT_OK_NOTEBOOK,
    )


def test_nnb_convert_jupytext_content_to_ipynb_and_html_content(
    ok_jupytext_notebook: JupytextNotebookContent,
):
    ipynb_nb = ok_jupytext_notebook.to_ipynb()

    # check jupytext -> ipynb conversion output
    assert ipynb_nb.filepath == Path("nb/ok_notebook.ipynb")
    json.loads(ipynb_nb.content)
    assert "print(1 + 12 + 123)" in ipynb_nb.content
    assert len(ipynb_nb.content) > len(ok_jupytext_notebook.content)

    # check ipynb -> html conversion output
    nb_html: str = ipynb_nb.to_html()
    assert len(nb_html) > len(ok_jupytext_notebook.content)
    assert "<html>" in nb_html


def test_nnb_evaluate_jupytext_content_notebook(
    ok_jupytext_notebook: JupytextNotebookContent,
    tmp_path: Path,
):
    parameters = {"variable_a": "aaaaabbbbbbccccccc"}

    err, evaluated_ipynb_nb = ok_jupytext_notebook.to_ipynb().evaluate(
        tmp_path, parameters=parameters
    )
    assert err is None

    # check jupytext -> ipynb conversion output
    assert evaluated_ipynb_nb.filepath == Path("nb/ok_notebook.ipynb")
    json.loads(evaluated_ipynb_nb.content)

    assert "print(1 + 12 + 123)" in evaluated_ipynb_nb.content
    assert parameters["variable_a"] in evaluated_ipynb_nb.content
    assert str(1 + 12 + 123) in evaluated_ipynb_nb.content


def test_nnb_evaluate_failing_jupytext_notebook(tmp_path: Path):
    err, partially_evaluated_notebook = (
        JupytextNotebookContent(
            filepath=Path("nb/fail_notebook.py"),
            content=TEST_JUPYTEXT_FAIL_NOTEBOOK,
        )
        .to_ipynb()
        .evaluate(tmp_path, parameters={"variable_xyz": "foobarbaz"})
    )

    assert "Failed notebook123" in str(err)

    assert partially_evaluated_notebook.filepath == Path("nb/fail_notebook.ipynb")
    for s in ["variable_xyz", "foobarbaz", str(1234 + 234 + 54 + 6)]:
        assert s in partially_evaluated_notebook.content
