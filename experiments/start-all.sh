#!/bin/bash
if [ "$#" -ne 1 ]
then
    echo "Require 1 argument (COMPOSE_NAME), $# provided"
    echo 'Example: bash experiments/start-all.sh pod_${USER}'
    exit 1
fi

COMPOSE_NAME=$1
echo "Using COMPOSE_NAME= ${COMPOSE_NAME}"
sleep 3

docker build -t pod -f experiments/pod.Dockerfile . && \
    docker build -t podnogil -f experiments/podnogil.Dockerfile . && \
    docker compose -f experiments/docker-compose.yml -p ${COMPOSE_NAME} up
