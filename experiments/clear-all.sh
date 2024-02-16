#!/bin/bash
if [ "$#" -ne 1 ]
then
    echo "Require 1 argument (COMPOSE_NAME), $# provided"
    echo 'Example: bash experiments/clear-all.sh pod_${USER}'
    exit 1
fi

COMPOSE_NAME=$1
echo "Using COMPOSE_NAME= ${COMPOSE_NAME}"
sleep 3

docker compose -f experiments/docker-compose.yml -p ${COMPOSE_NAME} down
docker compose -f experiments/docker-compose.yml -p ${COMPOSE_NAME} rm -f -s -v
