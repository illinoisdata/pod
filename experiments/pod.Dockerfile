FROM ubuntu:22.04

RUN apt-get update
RUN apt-get upgrade --yes

# Install Python
RUN apt-get install python3.11 python-is-python3 python3-pip --yes
RUN python -m pip install --upgrade pip

# Install requirements first to improve cachings
COPY ./requirements.txt /pod/requirements.txt
RUN python -m pip install -r /pod/requirements.txt

# Install Pod
COPY ./pod /pod/pod
COPY ./setup.py /pod/setup.py
COPY ./README.md /pod/README.md
RUN python -m pip install /pod/

WORKDIR /
ENTRYPOINT /bin/bash