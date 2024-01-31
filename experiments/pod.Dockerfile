FROM ubuntu:22.04

RUN apt-get update && apt-get upgrade --yes
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install tools
RUN apt-get update && apt-get install -y build-essential make vim tmux git htop sysstat ioping nfs-common vmtouch
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python
RUN apt-get update && apt-get install -y python3.11 python-is-python3 python3-pip
RUN python -m pip install --upgrade pip
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install PostgreSQL client
RUN apt-get update && DEBIAN_FRONTEND="noninteractive" apt-get install -y postgresql
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Redis client
RUN apt-get update && apt-get install -y redis-tools
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Neo4j client
RUN apt-get update && apt --fix-broken install && apt install -y openjdk-17-jre
RUN apt-get clean && rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt install -y wget
RUN wget https://dist.neo4j.org/cypher-shell/cypher-shell_5.15.0_all.deb -O cypher-shell.deb && dpkg -i cypher-shell.deb && rm cypher-shell.deb

# Install MongoDB client
RUN apt-get update && apt-get install -y gnupg
RUN wget -qO- https://www.mongodb.org/static/pgp/server-7.0.asc | tee /etc/apt/trusted.gpg.d/server-7.0.asc
RUN echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-7.0.list
RUN apt-get update && apt-get install -y mongodb-mongosh
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install requirements first to improve caching
COPY ./requirements.txt /pod/requirements.txt
RUN python -m pip install -r /pod/requirements.txt

# Install Pod
COPY ./pod /pod/pod
COPY ./setup.py /pod/setup.py
COPY ./README.md /pod/README.md
RUN python -m pip install -e /pod/

WORKDIR /
ENTRYPOINT /bin/bash
