import os
from setuptools import setup, find_packages
from pathlib import Path

# ---
# The full package name and version are determined dynamically as below:
#
# See: https://peps.python.org/pep-0440/ for details on Python package version format

PYTHON_PACKAGE_RELEASE_TARGET: str = os.environ["PYTHON_PACKAGE_RELEASE_TARGET"]

assert PYTHON_PACKAGE_RELEASE_TARGET in [
    # CI build only test that we can build the package. No release
    "ci-build",
    # package built for a main release eg. 0.12.3 published to pypi
    "main-release",
    # nightly snapshot release
    "snapshot-release",
]


# --- determine package name ---
PYTHON_PACKAGE_NAME: str = "pynb_dag_runner"

if PYTHON_PACKAGE_RELEASE_TARGET == "snapshot-release":
    PYTHON_PACKAGE_NAME += "_snapshot"

# --- determine package version ---
PYTHON_PACKAGE_VERSION: str = Path("PYTHON_PACKAGE_VERSION").read_text().splitlines()[0]

if PYTHON_PACKAGE_RELEASE_TARGET == "snapshot-release":
    PYTHON_PACKAGE_VERSION += f".dev{os.environ['LAST_COMMIT_UNIX_EPOCH']}"

# --- determine sha of current git commit ---
GIT_SHA: str = os.environ["GITHUB_SHA"]
print("setup.py - GIT_SHA                  :", GIT_SHA)
print("setup.py - PYTHON_PACKAGE_NAME      :", PYTHON_PACKAGE_NAME)
print("setup.py - PYTHON_PACKAGE_VERSION   :", PYTHON_PACKAGE_VERSION)


print("setup.py - writing __version__.py")
Path("./pynb_dag_runner/__version__.py").write_text(
    "\n".join(
        [
            f'__version__ = "{PYTHON_PACKAGE_VERSION}"',
            f'__git_sha__ = "{GIT_SHA}"',
            '',
        ]
    )
)

# ----

setup(
    name=PYTHON_PACKAGE_NAME,
    author="Matias Dahl",
    author_email="matias.dahl@iki.fi",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
    ],
    entry_points={
        "console_scripts": [
            "pynb_log_parser = pynb_log_parser.cli:entry_point",
        ],
    },
    url="https://github.com/pynb-dag-runner/pynb-dag-runner",
    version=PYTHON_PACKAGE_VERSION,
    install_requires=(Path("/home/host_user/requirements.txt").read_text().split("\n")),
    packages=find_packages(exclude=["tests", "tests.*"]),
)
