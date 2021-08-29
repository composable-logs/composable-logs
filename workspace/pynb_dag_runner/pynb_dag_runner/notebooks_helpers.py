from pathlib import Path
import tempfile, os
from typing import Any, Dict, Optional

#
import jupytext, papermill
from nbconvert import HTMLExporter


class JupyterIpynbNotebook:
    # ipynb is the default file format used by Jupyter for storing notebooks.
    def __init__(self, filepath: Path):
        assert filepath.suffix == ".ipynb"
        self.filepath = filepath

    def evaluate(
        self,
        output: "JupyterIpynbNotebook",
        cwd: Path,
        parameters: Dict[str, Any],
    ):
        """
        Evaluate this Jupyter notebook and write evaluated notebook to output_path.

        Evaluation and parameters are injected using papermill (BSD licensed)
        """
        assert self.filepath.is_file()
        assert not output.filepath.is_file()
        assert cwd.is_dir()

        # For all parameters, see
        # https://github.com/nteract/papermill/blob/main/papermill/cli.py
        # https://github.com/nteract/papermill/blob/main/papermill/execute.py

        papermill.execute_notebook(
            input_path=self.filepath,
            output_path=output.filepath,
            parameters=parameters,
            request_save_on_cell_execute=True,
            kernel_name="python",
            language="python",
            progress_bar=True,
            stdout_file=None,
            stderr_file=None,
            log_output=True,
            cwd=cwd,
        )

    def to_html(self) -> Path:
        """
        Convert this ipynb notebook into an html file.

        Return Path of output html file.

        See unit tests in nbconvert (BSD licensed):
        https://github.com/jupyter/nbconvert/blob/main/nbconvert/exporters/tests/test_html.py
        """
        assert self.filepath.is_file()

        output_filepath = self.filepath.with_suffix(".html")

        output, _ = HTMLExporter(template_name="classic").from_filename(self.filepath)
        output_filepath.write_text(output)

        return output_filepath

    @staticmethod
    def temp(path: Path) -> "JupyterIpynbNotebook":
        """
        Return a JupyterIpynbNotebook with a (random) non-existent filename in the
        provided path.
        """
        assert path.is_dir()

        fp, tmp_filepath = tempfile.mkstemp(
            dir=path,
            prefix="temp-notebook-",
            suffix=".ipynb",
        )
        os.close(fp)
        os.remove(tmp_filepath)

        return JupyterIpynbNotebook(Path(tmp_filepath))


class JupytextNotebook:
    """
    For details about the Jupytext Jupyter-plugin and file format, see:
       https://jupytext.readthedocs.io
    """

    def __init__(self, filepath: Path):
        assert filepath.suffix == ".py"
        self.filepath = filepath

    def to_ipynb(self, output: Optional[JupyterIpynbNotebook] = None):
        """
        Notes:
        This method only converts one file format to another, and no code cells are
        evaluated. Thus all cells outputs in the output ipynb file will be empty.

        However, cell inputs (code and tags) from input Jupytext notebook are
        included in the output ipynb notebook.
        """
        if output is None:
            output = JupyterIpynbNotebook(self.filepath.with_suffix(".ipynb"))

        # see https://jupytext.readthedocs.io/en/latest/using-library.html
        nb = jupytext.read(self.filepath, fmt="py:percent")
        jupytext.write(nb, fp=output.filepath, fmt="notebook")

        return output

    def evaluate(
        self, output_path: Path, parameters: Dict[str, Any] = {}
    ) -> JupyterIpynbNotebook:
        """
        Evaluate a Jupytext notebook, and inject provided parameters using Papermill.

        Output written to an ipynb file in output_path-directory. The base filename
        is determined from the input Python file.

        Eg., if we are asked to evaluate /path/to/jupytext/data_tables.py into output
        path /tmp, then the output file is /tmp/data_tables.ipynb.

        Returns
          JupyterIpynbNotebook instance of evaluate notebook.
        """
        assert self.filepath.is_file()
        assert output_path.is_dir()

        # Split input Jupytext filepath into directory and filename parts
        py_notebook_filename: str = self.filepath.name
        py_notebook_path: Path = self.filepath.parent
        assert py_notebook_path.is_dir()

        # Convert input Jupytext notebook to a random ipynb file in output directory
        tmp_notebook_ipynb = JupyterIpynbNotebook.temp(output_path)
        self.to_ipynb(output=tmp_notebook_ipynb)

        # Determine name of output evaluated notebook, evaluate temp notebook, and
        # clean up.
        evaluated_ipynb_notebook = JupyterIpynbNotebook(
            (output_path / py_notebook_filename).with_suffix(".ipynb")
        )

        tmp_notebook_ipynb.evaluate(
            output=evaluated_ipynb_notebook,
            cwd=py_notebook_path,
            parameters=parameters,
        )

        os.remove(tmp_notebook_ipynb.filepath)

        return evaluated_ipynb_notebook
