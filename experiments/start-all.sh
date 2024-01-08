#!/bin/bash
docker build -t pod -f experiments/pod.Dockerfile . && \
    docker compose -f experiments/docker-compose.yml up
