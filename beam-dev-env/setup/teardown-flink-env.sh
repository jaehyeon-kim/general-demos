#!/usr/bin/env bash

export SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

#### stop kafka cluster in docker
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml down -v

#### stop local flink cluster
${SCRIPT_DIR}/flink-1.16.3/bin/stop-cluster.sh

#### remove all stopped containers
docker container prune -f
