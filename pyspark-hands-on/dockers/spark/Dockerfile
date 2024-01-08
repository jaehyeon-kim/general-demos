FROM public.ecr.aws/emr-on-eks/spark/emr-6.15.0:latest

USER root

COPY ./requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

USER hadoop