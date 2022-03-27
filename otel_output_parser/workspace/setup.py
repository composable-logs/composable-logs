from setuptools import setup, find_packages

setup(
    name="otel_output_parser",
    author="Matias Dahl",
    author_email="matias.dahl@iki.fi",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
    ],
    entry_points={
        "console_scripts": [
            "static_builder = static_builder.cli:entry_point",
        ],
    },
    url="https://github.com/pynb-dag-runner",
    version="0.0.1",
    install_requires=[],
    packages=find_packages(exclude=["tests", "tests.*"]),
)
