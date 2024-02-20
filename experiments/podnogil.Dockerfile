FROM ubuntu:22.04

WORKDIR /
RUN apt-get update
RUN apt-get upgrade --yes

# Install tools
RUN apt-get install build-essential make vim tmux git htop sysstat ioping nfs-common vmtouch wget curl --yes
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

# Install Python
RUN DEBIAN_FRONTEND="noninteractive" apt-get install build-essential gdb lcov pkg-config \
      libbz2-dev libffi-dev libgdbm-dev libgdbm-compat-dev liblzma-dev \
      libncurses5-dev libreadline6-dev libsqlite3-dev libssl-dev \
      lzma lzma-dev tk-dev uuid-dev zlib1g-dev --yes
RUN git clone https://github.com/colesbury/nogil-3.12-final /nogil
WORKDIR /nogil
RUN ./configure --enable-optimizations --with-pydebug
RUN make -j16
RUN make install

# Install Python
WORKDIR /
RUN apt-get install python-is-python3 python3-pip --yes
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

# Install requirements first to improve cachings
COPY ./requirements.txt /pod/requirements.txt
RUN apt-get install libpq-dev --yes
RUN python -m pip install -r /pod/requirements.txt

# Install Pod
COPY ./pod /pod/pod
COPY ./setup.py /pod/setup.py
COPY ./README.md /pod/README.md
RUN python -m pip install -e /pod/

WORKDIR /
ENTRYPOINT /bin/bash
