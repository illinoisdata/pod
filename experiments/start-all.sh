#!/bin/bash
if [ "$#" -ne 2 ]
then
    echo "Require 2 arguments (COMPOSE_NAME), (DATA_VOLUME) $# provided"
    echo 'Example: bash experiments/start-all.sh pod_${USER} /data/elastic-notebook/'
    exit 1
fi

COMPOSE_NAME=$1
DATA_VOLUME=$2
echo "Using COMPOSE_NAME= ${COMPOSE_NAME} and DATA_VOLUME= ${DATA_VOLUME}"
sleep 3

export POD_NBDATA=${DATA_VOLUME}
docker build -t podnogil -f experiments/podnogil.Dockerfile . && \
    docker build -t pod -f experiments/pod.Dockerfile . && \
    docker build -t pod39 -f experiments/pod39.Dockerfile . && \
    docker compose -f experiments/docker-compose.yml -p ${COMPOSE_NAME} up
