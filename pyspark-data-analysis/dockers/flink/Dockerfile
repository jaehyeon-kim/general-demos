FROM public.ecr.aws/emr-on-eks/flink/emr-6.15.0-flink:latest

ARG FLINK_VERSION
ENV FLINK_VERSION=${FLINK_VERSION:-1.17.1}
ARG KAFKA_VERSION
ENV KAFKA_VERSION=${KAFKA_VERSION:-3.2.3}
ARG FAKER_VERSION
ENV FAKER_VERSION=${FAKER_VERSION:-0.5.3}

##
## add connectors (Kafka and flink faker) and related dependencies
##
RUN curl -o /usr/lib/flink/lib/flink-connector-kafka-${FLINK_VERSION}.jar \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/${FLINK_VERSION}/flink-connector-kafka-${FLINK_VERSION}.jar \
  && curl -o /usr/lib/flink/lib/kafka-clients-${KAFKA_VERSION}.jar \
      https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar \
  && curl -o /usr/lib/flink/lib/flink-sql-connector-kafka-${FLINK_VERSION}.jar \
      https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_VERSION}/flink-sql-connector-kafka-${FLINK_VERSION}.jar \
  && curl -L -o /usr/lib/flink/lib/flink-faker-${FAKER_VERSION}.jar \
      https://github.com/knaufk/flink-faker/releases/download/v${FAKER_VERSION}/flink-faker-${FAKER_VERSION}.jar

##
## fix process startup issues
##
# should be able to write a file in /mnt/tmp as getAvailablePort() in NetUtils class writes to /mnt/tmp instead of /tmp
#   see https://stackoverflow.com/questions/77539526/fail-to-start-flink-sql-client-on-emr-on-eks-docker-image
RUN mkdir -p /mnt/tmp

## add missing jar files
RUN curl -L -o /usr/lib/flink/lib/javax.inject-1.jar \
      https://repo1.maven.org/maven2/javax/inject/javax.inject/1/javax.inject-1.jar \
  && curl -L -o /usr/lib/flink/lib/aopalliance-1.0.jar \
      https://repo1.maven.org/maven2/aopalliance/aopalliance/1.0/aopalliance-1.0.jar

##
## update hadoop configuration for Glue data catalog integration
##
## create /mnt/s3 (value of fs.s3.buffer.dir) beforehand
RUN mkdir -p /mnt/s3

## copy updated core-site.xml
## update credentials providers and value of fs.s3.buffer.dir to /mnt/s3 only
USER root

COPY ./core-site.xml /glue/confs/hadoop/conf/core-site.xml

USER flink