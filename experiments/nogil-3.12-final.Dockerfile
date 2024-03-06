FROM --platform=$BUILDPLATFORM ubuntu:22.04

RUN apt-get update
RUN apt-get upgrade --yes
RUN DEBIAN_FRONTEND="noninteractive" apt-get install build-essential \
      make git gdb lcov pkg-config \
      libbz2-dev libffi-dev libgdbm-dev libgdbm-compat-dev liblzma-dev \
      libncurses5-dev libreadline6-dev libsqlite3-dev libssl-dev \
      lzma lzma-dev tk-dev uuid-dev zlib1g-dev --yes

RUN git clone https://github.com/colesbury/nogil-3.12-final /nogil

WORKDIR /nogil
RUN ./configure --enable-optimizations --with-pydebug
RUN make -j16
RUN make install

WORKDIR /
