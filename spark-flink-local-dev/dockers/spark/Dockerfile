FROM public.ecr.aws/emr-on-eks/spark/emr-6.15.0:latest

ENV PYTHONPATH="$PYTHONPATH:/usr/lib/spark/python/lib/py4j-src.zip:/usr/lib/spark/python/lib/pyspark.zip"

USER root

## Add hadoop to sudo
RUN yum install -y sudo git \
  && echo "hadoop ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

## Update spark config and log4j properties
COPY ./spark-defaults.conf /usr/lib/spark/conf/spark-defaults.conf
COPY ./log4j2.properties /usr/lib/spark/conf/log4j2.properties

USER hadoop:hadoop