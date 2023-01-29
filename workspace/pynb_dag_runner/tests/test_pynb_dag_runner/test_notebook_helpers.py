from pathlib import Path
import os, json

#
import pytest

#
from pynb_dag_runner.notebooks_helpers import (
    JupytextNotebook,
    JupyterIpynbNotebook,
    convert_ipynb_to_html,
    JupytextNotebookContent,
)
from pynb_dag_runner.helpers import read_json

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


def write_test_jupytext_OK_notebook(path: Path) -> JupytextNotebook:
    output_path = path / "notebook.py"
    output_path.write_text(TEST_JUPYTEXT_OK_NOTEBOOK)

    assert os.path.getsize(output_path) == len(TEST_JUPYTEXT_OK_NOTEBOOK)
    return JupytextNotebook(output_path)


def test_nb_can_convert_jupytext_notebook_to_ipynb_and_html(tmp_path: Path):
    notebook_py: JupytextNotebook = write_test_jupytext_OK_notebook(tmp_path)

    # Convert py-percent jupytext file into ipynb-notebook file format
    notebook_ipynb = notebook_py.to_ipynb()

    # assert that generated ipynb file exists and parses as json
    assert notebook_ipynb.filepath.is_file()
    assert isinstance(read_json(notebook_ipynb.filepath)["cells"], list)

    # assert that this ipynb can be converted into html
    filepath_html = notebook_ipynb.to_html()
    assert filepath_html.is_file()
    assert "# Example comment" in filepath_html.read_text()


def test_nb_can_convert_jupytext_notebook_to_ipynb_and_evaluate(tmp_path: Path):
    notebook_py: JupytextNotebook = write_test_jupytext_OK_notebook(tmp_path)

    # Convert py-percent jupytext file into ipynb-notebook file format
    notebook_ipynb = notebook_py.to_ipynb()

    # Evaluation ipynb notebook
    notebook_eval_ipynb = JupyterIpynbNotebook(tmp_path / "notebook_evaluated.ipynb")
    notebook_ipynb.evaluate(
        output=notebook_eval_ipynb, cwd=tmp_path, parameters={"variable_a": "hello"}
    )

    assert notebook_eval_ipynb.filepath.is_file()
    assert "variable_a=hello" in notebook_eval_ipynb.filepath.read_text()


def test_nb_random_ipynb_notebook_path(tmp_path: Path):
    notebook_ipynb = JupyterIpynbNotebook.temp(tmp_path)

    assert not notebook_ipynb.filepath.is_file()
    assert str(notebook_ipynb.filepath).startswith(str(tmp_path))


def test_nb_evaluate_jupytext_notebook(tmp_path: Path):
    output_path = tmp_path / "output"
    output_path.mkdir()

    notebook_py: JupytextNotebook = write_test_jupytext_OK_notebook(tmp_path)

    output_ipynb = JupyterIpynbNotebook(output_path / "foo.ipynb")

    evaluated_notebook: JupyterIpynbNotebook = notebook_py.evaluate(
        output=output_ipynb,
        parameters={"variable_a": "baz"},
    )

    assert output_ipynb.filepath.is_file()
    assert str(output_ipynb.filepath).startswith(str(output_path))
    assert "variable_a=baz" in output_ipynb.filepath.read_text()


def test_nb_convert_ipynb_string_to_html(tmp_path: Path):
    notebook_py: JupytextNotebook = write_test_jupytext_OK_notebook(tmp_path)

    output_ipynb = JupyterIpynbNotebook(tmp_path / "foo.ipynb")

    _ = notebook_py.evaluate(
        output=output_ipynb,
        parameters={"variable_a": "123xyz456"},
    )

    html_string: str = convert_ipynb_to_html(output_ipynb.filepath.read_text())

    for test_string in ["<html", "123xyz456"]:
        assert test_string in html_string.lower()


@pytest.fixture
def failing_jupytext_notebook(tmp_path: Path) -> JupytextNotebook:
    output_path: Path = tmp_path / "failing_notebook.py"
    output_path.write_text(TEST_JUPYTEXT_FAIL_NOTEBOOK)

    assert len(output_path.read_text()) == len(TEST_JUPYTEXT_FAIL_NOTEBOOK)
    return JupytextNotebook(output_path)


def test_nb_evaluate_jupytext_notebook_that_fails(
    failing_jupytext_notebook: JupytextNotebook,
):

    output_ipynb = JupyterIpynbNotebook(
        failing_jupytext_notebook.filepath.with_suffix(".ipynb")
    )

    try:
        _: JupyterIpynbNotebook = failing_jupytext_notebook.evaluate(
            output=output_ipynb,
            parameters={"variable_xyz": "foobarbaz"},
        )

    except BaseException as e:
        assert "Failed notebook123" in str(e)

        assert output_ipynb.filepath.is_file()
        for s in ["variable_xyz", "foobarbaz", str(1234 + 234 + 54 + 6)]:
            assert s in output_ipynb.filepath.read_text()


# --- New notebook interface ---


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
