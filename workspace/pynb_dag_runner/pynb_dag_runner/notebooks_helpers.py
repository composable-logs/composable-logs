from pathlib import Path
import tempfile, os

#
import jupytext
from nbconvert import HTMLExporter


class JupyterIpynbNotebook:
    # ipynb is the default file format used by Jupyter for storing notebooks.
    def __init__(self, filepath: Path):
        assert filepath.suffix == ".ipynb"
        self.filepath = filepath

    def to_html(self):
        """
        Convert this ipynb notebook into an html file

        See unit tests in nbconvert (BSD licensed):
        https://github.com/jupyter/nbconvert/blob/main/nbconvert/exporters/tests/test_html.py
        """
        assert self.filepath.is_file()

        output, _ = HTMLExporter(template_name="classic").from_filename(self.filepath)
        self.filepath.with_suffix(".html").write_text(output)

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

    def to_jupyter_ipynb_notebook(self, output: JupyterIpynbNotebook):
        """
        Notes:
        This method only converts one file format to another, and no code cells are
        evaluated. Thus all cells outputs in the output ipynb file will be empty.

        However, cell inputs (code and tags) from input Jupytext notebook are
        included in the output ipynb notebook.
        """

        # see https://jupytext.readthedocs.io/en/latest/using-library.html
        nb = jupytext.read(self.filepath, fmt="py:percent")
        jupytext.write(nb, fp=output.filepath, fmt="notebook")
