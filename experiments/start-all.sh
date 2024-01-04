#!/bin/bash
docker build -t pod -f experiments/pod.Dockerfile . && \
docker build -t podpsql -f experiments/podpsql.Dockerfile . && \
    docker compose -f experiments/docker-compose.yml up
