import io, tempfile, os
from typing import Any, Dict, Optional, Tuple, List, Mapping
from pathlib import Path

# -
import pydantic as p
import jupytext, nbformat, papermill
from nbconvert import HTMLExporter

# -

"""
On the command line (bash), a Jupytext notebooks can be evaluated and converted into an
html page as below.

```bash
jupytext --to notebook --output - jupytext_nb.py | \
    papermill <notebook args> --kernel python3 --log-output --cwd $(pwd) - output.ipynb
jupyter nbconvert --to html output.ipynb
```

The below classes allow us to do the same conversion with Python.
"""


def convert_ipynb_to_html(ipynb_notebook_content: str) -> str:
    """
    Convert input string with content of a Jupyter notebook (in .ipynb format)
    into a html-string.

    No evaluation of code cells.
    """
    output, _ = HTMLExporter(template_name="classic").from_file(
        file_stream=io.StringIO(ipynb_notebook_content)
    )

    return output


class JupyterIpynbNotebookContent(p.BaseModel):
    """
    Model a named Jupyter notebook in .ipynb format.

    Cells may or may not have been evaluated.
    """

    filepath: Path

    content: str

    def to_html(self) -> str:
        return convert_ipynb_to_html(self.content)

    def evaluate(
        self,
        tmp_path: Path = Path("/tmp"),
        cwd: Optional[Path] = None,
        parameters: Mapping[str, Any] = {},
    ) -> Tuple[Optional[Exception], "JupyterIpynbNotebookContent"]:
        """
        Evaluate notebook using Papermill with the provided parameters injected.

        If an exception is thrown from a cell in the notebook, execution is stopped
        and we return the tuple: (Exception, the partially evaluated notebook)

        If notebook runs succussfully we return the tuple: (None, the evaluated
        notebook)
        """

        # Determine a temp file for input/output
        fp, tmp_filepath = tempfile.mkstemp(
            dir=tmp_path,
            prefix="temp-notebook-",
            suffix=".ipynb",
        )
        assert tmp_path == Path(tmp_filepath).parent
        os.write(fp, self.content.encode(encoding="utf-8"))
        os.close(fp)

        def get_notebook():
            return JupyterIpynbNotebookContent(
                filepath=self.filepath,
                content=Path(tmp_filepath).read_text(),
            )

        try:
            # For all parameters, see
            # https://github.com/nteract/papermill/blob/main/papermill/cli.py
            # https://github.com/nteract/papermill/blob/main/papermill/execute.py
            papermill.execute_notebook(
                input_path=tmp_filepath,
                output_path=tmp_filepath,
                parameters=parameters,
                request_save_on_cell_execute=True,
                kernel_name="python",
                language="python",
                progress_bar=True,
                stdout_file=None,
                stderr_file=None,
                log_output=True,
                cwd=cwd if cwd is not None else tmp_path,
            )
            return None, get_notebook()

        except BaseException as e:
            # Convert any Papermill custom exceptions into a standard Python
            # Exception-class, see
            #
            # https://github.com/nteract/papermill/blob/main/papermill/exceptions.py
            #
            # Otherwise, we get problems with Ray not able to serialize/deserialize
            # the Exception
            return Exception(str(e)), get_notebook()

        finally:
            # This may not run if the Python process is cancelled by Ray (eg timeout).
            os.remove(tmp_filepath)


class JupytextNotebookContent(p.BaseModel):
    """
    Contain content of Jupyter notebook in Jupytext (non-JSON) file format.

    This file format contains the input code cells, but no output.
    """

    # eg "notebooks/eda.py" or "eda.py"; should end with .py
    filepath: Path

    content: str

    def __str__(self):
        return "\n".join(
            [
                f"JupytextNotebookContent (relative_filepath={self.filepath})",
                80 * "=",
                (self.content)[:1000],
                "...",
                80 * "=",
            ]
        )

    def to_ipynb(self) -> JupyterIpynbNotebookContent:
        # Convert Jupytext input into ipynb file (without executing cells)
        nb = jupytext.reads(self.content, fmt="py:percent")
        ipynb_content: str = jupytext.writes(
            nb,
            version=nbformat.NO_CONVERT,
            fmt="notebook",
        )
        return JupyterIpynbNotebookContent(
            filepath=self.filepath.with_suffix(".ipynb"),
            content=ipynb_content,
        )


# --- old interface, to be deleted ---


class JupyterIpynbNotebook:
    # ipynb is the default file format used by Jupyter for storing notebooks.
    def __init__(self, filepath: Path):
        assert filepath.suffix == ".ipynb"
        self.filepath = filepath

    def evaluate(
        self,
        output: "JupyterIpynbNotebook",
        cwd: Path,
        parameters: Mapping[str, Any],
    ):
        """
        Evaluate this Jupyter notebook and write evaluated notebook to output_path.

        Evaluation and parameters are injected using papermill (BSD licensed)
        """
        assert self.filepath.is_file()
        # assert not output.filepath.is_file()
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

        output, _ = HTMLExporter(template_name="classic").from_filename(
            str(self.filepath)
        )
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
        self, output: JupyterIpynbNotebook, parameters: Mapping[str, Any] = {}
    ):
        """
        Evaluate a Jupytext notebook, and inject provided parameters using Papermill.

        Exceptions thrown (eg from a cell) in the notebook are propagated and thrown by
        this function.
        """
        try:
            # Convert input Jupytext notebook to a random ipynb file in output directory
            tmp_notebook_ipynb = JupyterIpynbNotebook.temp(output.filepath.parent)

            assert self.filepath.is_file()
            # assert not output.filepath.is_file()

            self.to_ipynb(output=tmp_notebook_ipynb)

            tmp_notebook_ipynb.evaluate(
                output=output,
                # Run with jupytext notebook directory as current directory
                cwd=self.filepath.parent,
                parameters=parameters,
            )

        except BaseException as e:
            # Convert any Papermill custom exceptions into a standard Python
            # Exception-class, see
            #
            # https://github.com/nteract/papermill/blob/main/papermill/exceptions.py
            #
            # Otherwise, we get problems with Ray not able to serialize/deserialize
            # the Exception
            raise Exception(str(e))

        finally:
            # Note: this may not run if the Python process is cancelled by Ray
            # (eg by timeout)
            if tmp_notebook_ipynb.filepath.is_file():
                os.remove(tmp_notebook_ipynb.filepath)
