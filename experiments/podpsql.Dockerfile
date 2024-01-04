FROM pod

# Prerequisites
RUN apt-get -y install wget

# From https://www.postgresql.org/download/linux/ubuntu/ (using Ubuntu 22.04)
RUN sh -c 'echo "deb https://apt.postgresql.org/pub/repos/apt jammy-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update
RUN DEBIAN_FRONTEND="noninteractive" apt-get -y install postgresql

# TODO: Not really working yet...
