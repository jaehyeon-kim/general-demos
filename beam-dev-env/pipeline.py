import argparse
import typing

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.io import kafka
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def kafka_record_processor(kafka_kv_rec):
    # Incoming Kafka records must have a key associated.
    # Otherwise, Beam throws an exception with null keys.
    assert isinstance(kafka_kv_rec, tuple)
    print("Got kafka record value: ", str(kafka_kv_rec[1]))
    rec_key = str(kafka_kv_rec[0])
    rec_val = str(kafka_kv_rec[1])
    # return record as tuple[key, value] in bytes for echoing
    return bytes(rec_key, "utf-8"), bytes(rec_val, "utf-8")


job_server = "localhost"
# Set up this host to point to 127.0.0.1 in /etc/hosts
bootstrap_servers = "host.docker.internal:29092"
# job_server = "jobservice"
# bootstrap_servers = "kafka-0:9092"
kafka_consumer_group_id = "kafka_echo"
input_topic = "echo-input"
output_topic = "echo-output"


def init_pipeline(job_server=job_server):
    pipeline_opts = {
        # MUST BE PortableRunner
        "runner": "PortableRunner",
        "job_name": "kafka_echo_demo",
        "job_endpoint": f"{job_server}:8099",
        "artifact_endpoint": f"{job_server}:8098",
        "environment_type": "LOOPBACK",
        # "environment_type": "EXTERNAL",
        # "environment_config": "workerpool:50000",
        "streaming": True,
        "parallelism": 2,
        "experiments": ["use_deprecated_read"],
        "checkpointing_interval": "60000",
    }
    pipeline_options = PipelineOptions([], **pipeline_opts)
    # Required, else it will complain that when importing worker functions
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline = beam.Pipeline(options=pipeline_options)
    return pipeline


def run_pipeline(pipeline: Pipeline):
    _ = (
        pipeline
        | "ReadMessages"
        >> kafka.ReadFromKafka(
            consumer_config={
                "bootstrap.servers": bootstrap_servers,
                "group.id": kafka_consumer_group_id,
            },
            topics=[input_topic],
        )
        | beam.Map(lambda r: kafka_record_processor(r)).with_output_types(
            typing.Tuple[bytes, bytes]
        )
        | "WriteToKafka"
        >> kafka.WriteToKafka(
            producer_config={"bootstrap.servers": bootstrap_servers},
            topic=output_topic,
        )
    )
    pipeline.run().wait_until_finish()


if __name__ == "__main__":
    p = init_pipeline()
    run_pipeline(p)
