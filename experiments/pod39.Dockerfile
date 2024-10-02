FROM ubuntu:22.04

RUN apt-get update
RUN apt-get upgrade --yes

# Install tools
RUN apt-get install build-essential make vim tmux git htop sysstat ioping nfs-common vmtouch --yes

# Install Python
RUN apt-get install libffi-dev software-properties-common --yes
RUN add-apt-repository ppa:deadsnakes/ppa --yes && apt update
RUN DEBIAN_FRONTEND="noninteractive" apt-get install python3.9 --yes
RUN ln -sf python3.9 /usr/bin/python
RUN apt-get install python3.9-distutils python3-full python3-pip --yes
RUN python -m pip install --upgrade pip

# Install PostgreSQL client
RUN DEBIAN_FRONTEND="noninteractive" apt-get install postgresql --yes

# Install Redis client
RUN apt-get install redis-tools --yes

# Install Neo4j client
RUN apt --fix-broken install && apt install openjdk-17-jre --yes
RUN apt install wget --yes
RUN wget https://dist.neo4j.org/cypher-shell/cypher-shell_5.15.0_all.deb?_ga=2.72547732.605979457.1705684934-272562543.1705684934 -O cypher-shell.deb && dpkg -i cypher-shell.deb && rm cypher-shell.deb

# Install MongoDB client
RUN apt-get install gnupg --yes
RUN wget -qO- https://www.mongodb.org/static/pgp/server-7.0.asc | tee /etc/apt/trusted.gpg.d/server-7.0.asc && \
    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-7.0.list
RUN apt-get update
RUN apt-get install mongodb-mongosh --yes

# Install CRIU
WORKDIR /
RUN apt install libnet-dev libnl-route-3-dev bsdmainutils build-essential git-core iptables libaio-dev libbsd-dev libcap-dev libgnutls28-dev libgnutls30 libnftables-dev libnl-3-dev libprotobuf-c-dev libprotobuf-dev libselinux-dev iproute2 kmod pkg-config protobuf-c-compiler protobuf-compiler python3-minimal python3-protobuf python3-yaml libdrm-dev gnutls-dev asciidoc --yes
RUN python -m pip install asciidoc protobuf==3.20.*
# Or RUN git clone https://github.com/checkpoint-restore/criu.git
RUN git clone https://github.com/illinoisdata/criu.git
WORKDIR /criu
RUN make -j4
RUN make install
RUN make PREFIX=/build/ install-criu
RUN python -m pip install  ./lib

# Install requirements first to improve cachings
WORKDIR /
COPY ./requirements.txt /pod/requirements.txt
RUN apt-get install libpq-dev libhdf5-dev pkg-config python3.9-dev --yes
RUN python -m pip install -r /pod/requirements.txt

# Install Pod
COPY ./pod /pod/pod
COPY ./setup.py /pod/setup.py
COPY ./Makefile /pod/Makefile
COPY ./README.md /pod/README.md

WORKDIR /pod
RUN python -m pip install -e .
RUN make compile

WORKDIR /
