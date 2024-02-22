#!/usr/bin/env bash

export SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

#### start kafka cluster in docker
docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up -d

#### start local flink cluster
## 1. download flink binary and decompress in the same folder
##  wget https://dlcdn.apache.org/flink/flink-1.16.3/flink-1.16.3-bin-scala_2.12.tgz
##  tar -zxf flink-1.16.3-bin-scala_2.12.tgz
## 2. update flink configuration in eg) ./flink-1.16.3/config/flink-conf.yaml
##  rest.port: 8081                   # uncomment
##  rest.address: localhost           # default
##  rest.bind-address: 0.0.0.0        # change from localhost
##  taskmanager.numberOfTaskSlots: 5  # update from 1
${SCRIPT_DIR}/flink-1.16.3/bin/start-cluster.sh

#### start flink job server
## 1. download the driver jar file
## wget https://repo1.maven.org/maven2/org/apache/beam/beam-runners-flink-1.16-job-server/2.53.0/beam-runners-flink-1.16-job-server-2.53.0.jar
java -cp ${SCRIPT_DIR}/beam-runners-flink-1.16-job-server-2.53.0.jar \
  org.apache.beam.runners.flink.FlinkJobServerDriver --flink-master=localhost:8081 --job-host=0.0.0.0
