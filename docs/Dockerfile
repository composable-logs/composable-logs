FROM python:3.10.5-buster

ARG HOST_GID
ARG HOST_UID

RUN apt-get -qq -y update && \
    apt-get -qq -y upgrade && \
    apt-get install -qq -y --no-install-recommends \
        make

RUN groupadd --gid $HOST_GID host_user_group  && \
    useradd --uid $HOST_UID --gid $HOST_GID -rm --shell /bin/bash host_user


USER host_user

ENV PYTHONPYCACHEPREFIX=/home/host_user/.cache/python
ENV PATH "/home/host_user/.local/bin:$PATH"

COPY requirements.txt /home/host_user/
RUN pip3 install --user -r /home/host_user/requirements.txt

ENTRYPOINT ["/bin/bash", "-c"]
