#!/usr/bin/env bash
shopt -s extglob

SCRIPT_DIR="$(cd $(dirname "$0"); pwd)"

SRC_PATH=${SCRIPT_DIR}/connectors
rm -rf ${SRC_PATH} && mkdir ${SRC_PATH}

## Avien opensearch sink connector
echo "downloading opensearch sink connector..."
DOWNLOAD_URL=https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/releases/download/v3.1.0/opensearch-connector-for-apache-kafka-3.1.0.zip

curl -L -o ${SRC_PATH}/tmp.zip ${DOWNLOAD_URL} \
  && unzip -qq ${SRC_PATH}/tmp.zip -d ${SRC_PATH} \
  && rm -rf $SRC_PATH/!(opensearch-connector-for-apache-kafka-3.1.0) \
  && mv $SRC_PATH/opensearch-connector-for-apache-kafka-3.1.0 $SRC_PATH/opensearch-connector \
  && cd $SRC_PATH/opensearch-connector \
  && zip ../opensearch-connector.zip *

## MSK Data Generator Souce Connector
echo "downloading msk data generator..."
DOWNLOAD_URL=https://github.com/awslabs/amazon-msk-data-generator/releases/download/v0.4.0/msk-data-generator-0.4-jar-with-dependencies.jar

mkdir ${SRC_PATH}/msk-datagen \
  && curl -L -o ${SRC_PATH}/msk-datagen/msk-data-generator.jar ${DOWNLOAD_URL}
