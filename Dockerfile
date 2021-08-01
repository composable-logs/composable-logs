FROM ubuntu:20.04

ARG HOST_GID
ARG HOST_UID

ENV DEBIAN_FRONTEND "noninteractive"

RUN apt-get -y update && \
    apt-get -y upgrade && \
    apt-get install -y -q \
        python3 \
        python3-pip

RUN groupadd --gid $HOST_GID host_user_group  && \
    useradd --uid $HOST_UID --gid $HOST_GID -rm --shell /bin/bash host_user

USER host_user
ENV PATH "/home/host_user/.local/bin:$PATH"

RUN pip3 install --user \
        notebook==6.3.0 \
        jupytext==1.11.4 \
        pytest==6.2.4 \
        black==21.7b0 \
        mypy==0.910 && \
    : && \
    : Add py-percent format support for Jupyter notebooks, see && \
    : https://jupytext.readthedocs.io/en/latest/install.html && \
    jupyter nbextension install --py jupytext --user && \
    jupyter nbextension enable --py jupytext --user && \
    jupyter serverextension enable jupytext

ENV MYPY_CACHE_DIR=/home/host_user/.cache/mypy
RUN mkdir -p $MYPY_CACHE_DIR

ENV PYTHONPYCACHEPREFIX=/home/host_user/.cache/pycache

ENV PYTEST_ADDOPTS="-vvv -p no:cacheprovider"

ENTRYPOINT ["/bin/bash", "-c"]
