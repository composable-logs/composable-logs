import os
from setuptools import setup, find_packages
from pathlib import Path


def get_long_description():
    # --- Path to markdown that will be shown in PyPI package page ---
    README_FILEPATH = os.environ.get("README_FILEPATH")
    print("setup.py - README_FILEPATH          :", README_FILEPATH)

    if README_FILEPATH is None:
        return {}
    else:
        return {
            "long_description": Path(README_FILEPATH).read_text(),
            "long_description_content_type": "text/markdown",
        }


def get_name_and_version():
    """
    Determine package name and version dynamically for various install targets

    See https://peps.python.org/pep-0440/ for details on Python package version format.
    Versions with postfix "+<some text>" can not be published to PyPI.
    """
    PYTHON_PACKAGE_RELEASE_TARGET = os.environ.get("PYTHON_PACKAGE_RELEASE_TARGET")

    # --- determine package name and version ---
    PYTHON_PACKAGE_NAME: str = "pynb_dag_runner"
    PYTHON_PACKAGE_VERSION: str = (
        Path("PYTHON_PACKAGE_VERSION").read_text().splitlines()[0]
    )

    if PYTHON_PACKAGE_RELEASE_TARGET == "snapshot-release":
        # For each commit to main, publish snapshot release
        PYTHON_PACKAGE_NAME += "_snapshot"
        PYTHON_PACKAGE_VERSION += f".dev{os.environ['LAST_COMMIT_UNIX_EPOCH']}"

    elif PYTHON_PACKAGE_RELEASE_TARGET == "ci-build":
        # CI build only test that we can build the package. No release
        # Mark CI-build as "local builds". They not be published to PyPI (PEP 440)
        PYTHON_PACKAGE_VERSION += f"+ci-build"

    elif PYTHON_PACKAGE_RELEASE_TARGET == "main-release":
        # Package built for a main release eg. 0.12.3 published to pypi.
        # No changes needed to package name and version
        pass

    elif PYTHON_PACKAGE_RELEASE_TARGET is None:
        # Package is installed in edit mode for local dev using "pip -e install ."
        PYTHON_PACKAGE_VERSION += f"+local-install-in-edit-mode"
    else:
        raise ValueError(f"Unknown release target {PYTHON_PACKAGE_RELEASE_TARGET}")

    # write __version__.py except for local editable dev installs
    if PYTHON_PACKAGE_RELEASE_TARGET is not None:
        write_version_py(PYTHON_PACKAGE_VERSION)

    print("setup.py - PYTHON_PACKAGE_NAME      :", PYTHON_PACKAGE_NAME)
    print("setup.py - PYTHON_PACKAGE_VERSION   :", PYTHON_PACKAGE_VERSION)

    return {"name": PYTHON_PACKAGE_NAME, "version": PYTHON_PACKAGE_VERSION}


def write_version_py(PYTHON_PACKAGE_VERSION: str):
    """
    If package might get published to PyPI write package name and version to
    __version__.py
    """
    GIT_SHA: str = os.environ["GITHUB_SHA"]
    print("setup.py - GIT_SHA                  :", GIT_SHA)

    print("setup.py - writing __version__.py")
    Path("./pynb_dag_runner/__version__.py").write_text(
        "\n".join(
            [
                f'__version__ = "{PYTHON_PACKAGE_VERSION}"',
                f'__git_sha__ = "{GIT_SHA}"',
                "",
            ]
        )
    )


setup(
    **get_name_and_version(),
    author="Matias Dahl",
    author_email="matias.dahl@iki.fi",
    license="MIT",
    **get_long_description(),
    classifiers=[
        "License :: OSI Approved :: MIT License",
    ],
    entry_points={
        "console_scripts": [
            "static_builder = otel_output_parser.static_builder.cli:entry_point",
            "pynb_log_parser = otel_output_parser.cli:entry_point",
        ],
    },
    url="https://github.com/pynb-dag-runner/pynb-dag-runner",
    install_requires=(Path("/home/host_user/requirements.txt").read_text().split("\n")),
    packages=find_packages(exclude=["tests", "tests.*"]),
)
