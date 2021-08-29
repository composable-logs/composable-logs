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

    evaluated_notebook: JupyterIpynbNotebook = notebook_py.evaluate(
        output_path=output_path,
        parameters={"variable_a": "baz"},
    )

    assert evaluated_notebook.filepath.is_file()
    assert str(evaluated_notebook.filepath).startswith(str(output_path))
    assert "variable_a=baz" in evaluated_notebook.filepath.read_text()
