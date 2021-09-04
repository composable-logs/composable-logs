from setuptools import setup, find_packages
from pathlib import Path

setup(
    name="pynb_dag_runner",
    author="Matias Dahl",
    author_email="matias.dahl@iki.fi",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
    ],
    url="https://github.com/pynb-dag-runner/pynb-dag-runner",
    version="0.0.1",
    install_requires=(
        Path("/home/host_user/requirements.base.txt").read_text().split("\n")
    ),
    packages=find_packages(exclude=("tests",)),
)
