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
    notebook_py = JupytextNotebook(path / "notebook.py")

    with open(notebook_py.filepath, "tw") as f:
        f.write(TEST_JUPYTEXT_NOTEBOOK)

    assert os.path.getsize(notebook_py.filepath) == len(TEST_JUPYTEXT_NOTEBOOK)
    return notebook_py


def test_can_convert_jupytext_notebook_to_ipynb_and_html(tmp_path: Path):
    notebook_py: JupytextNotebook = write_test_jupytext_notebook(tmp_path)

    # Convert py-percent jupytext file into ipynb-notebook file format
    notebook_ipynb = JupyterIpynbNotebook(tmp_path / "notebook.ipynb")
    notebook_py.to_jupyter_ipynb_notebook(notebook_ipynb)

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
    notebook_ipynb = JupyterIpynbNotebook(tmp_path / "notebook.ipynb")
    notebook_py.to_jupyter_ipynb_notebook(notebook_ipynb)

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
