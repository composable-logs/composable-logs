from pathlib import Path
import os

#
from pynb_dag_runner.notebooks_helpers import JupytextNotebook, JupyterIpynbNotebook
from pynb_dag_runner.helpers import read_json

# Example content of a Jupyter notebook stored in the py-percent file format.
TEST_JUPYTEXT_NOTEBOOK = """# %%
# %% tags=["parameters"]
# %%
# Example comment
print(1 + 12 + 123)
# %%
print(f"variable_a={variable_a}")
# %%
"""


def write_test_jupytext_notebook(path: Path) -> JupytextNotebook:
    output_path = path / "notebook.py"
    output_path.write_text(TEST_JUPYTEXT_NOTEBOOK)

    assert os.path.getsize(output_path) == len(TEST_JUPYTEXT_NOTEBOOK)
    return JupytextNotebook(output_path)


def write_failing_test_jupytext_notebook(path: Path) -> JupytextNotebook:
    output_path = path / "failing_notebook.py"

    fail_jupytext_notebook = """# %%
# %% tags=["parameters"]
# %%
# Example comment
print(1234 + 234 + 54 + 6)
# %%
raise Exception("Failed notebook")
# %%
"""
    output_path.write_text(fail_jupytext_notebook)

    assert os.path.getsize(output_path) == len(fail_jupytext_notebook)
    return JupytextNotebook(output_path)


def test_can_convert_jupytext_notebook_to_ipynb_and_html(tmp_path: Path):
    notebook_py: JupytextNotebook = write_test_jupytext_notebook(tmp_path)

    # Convert py-percent jupytext file into ipynb-notebook file format
    notebook_ipynb = notebook_py.to_ipynb()

    # assert that generated ipynb file exists and parses as json
    assert notebook_ipynb.filepath.is_file()
    assert isinstance(read_json(notebook_ipynb.filepath)["cells"], list)

    # assert that this ipynb can be converted into html
    filepath_html = notebook_ipynb.to_html()
    assert filepath_html.is_file()
    assert "# Example comment" in filepath_html.read_text()


def test_can_convert_jupytext_notebook_to_ipynb_and_evaluate(tmp_path: Path):
    notebook_py: JupytextNotebook = write_test_jupytext_notebook(tmp_path)

    # Convert py-percent jupytext file into ipynb-notebook file format
    notebook_ipynb = notebook_py.to_ipynb()

    # Evaluation ipynb notebook
    notebook_eval_ipynb = JupyterIpynbNotebook(tmp_path / "notebook_evaluated.ipynb")
    notebook_ipynb.evaluate(
        output=notebook_eval_ipynb, cwd=tmp_path, parameters={"variable_a": "hello"}
    )

    assert notebook_eval_ipynb.filepath.is_file()
    assert "variable_a=hello" in notebook_eval_ipynb.filepath.read_text()


def test_random_ipynb_notebook_path(tmp_path: Path):
    notebook_ipynb = JupyterIpynbNotebook.temp(tmp_path)

    assert not notebook_ipynb.filepath.is_file()
    assert str(notebook_ipynb.filepath).startswith(str(tmp_path))


def test_evaluate_jupytext_notebook(tmp_path: Path):
    output_path = tmp_path / "output"
    output_path.mkdir()

    notebook_py: JupytextNotebook = write_test_jupytext_notebook(tmp_path)

    output_ipynb = JupyterIpynbNotebook(output_path / "foo.ipynb")

    evaluated_notebook: JupyterIpynbNotebook = notebook_py.evaluate(
        output=output_ipynb,
        parameters={"variable_a": "baz"},
    )

    assert output_ipynb.filepath.is_file()
    assert str(output_ipynb.filepath).startswith(str(output_path))
    assert "variable_a=baz" in output_ipynb.filepath.read_text()


def test_evaluate_jupytext_notebook_that_fails(tmp_path: Path):
    output_path = tmp_path / "output"
    output_path.mkdir()

    notebook_py: JupytextNotebook = write_failing_test_jupytext_notebook(tmp_path)

    output_ipynb = JupyterIpynbNotebook(output_path / "foo.ipynb")

    try:
        evaluated_notebook: JupyterIpynbNotebook = notebook_py.evaluate(
            output=output_ipynb,
            parameters={"variable_a": "baz"},
        )

    except BaseException as e:
        assert "Failed notebook" in str(e)

        assert output_ipynb.filepath.is_file()
        assert str(1234 + 234 + 54 + 6) in output_ipynb.filepath.read_text()
