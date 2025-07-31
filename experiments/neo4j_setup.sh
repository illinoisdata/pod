#!/bin/bash

apt-get update
apt-get install socat

socat TCP-LISTEN:8082,fork EXEC:"/pod/experiments/neo4j_du_report.sh"
