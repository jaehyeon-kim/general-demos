import os
import datetime
import argparse
import json
import logging
import typing

import apache_beam as beam
from apache_beam.io import kafka
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class EventLog(typing.NamedTuple):
    ip: str
    id: str
    lat: float
    lng: float
    user_agent: str
    age_bracket: str
    opted_into_marketing: bool
    http_request: str
    http_response: int
    file_size_bytes: int
    event_datetime: str
    event_ts: int


beam.coders.registry.register_coder(EventLog, beam.coders.RowCoder)


def parse_json(element: str):
    row = json.loads(element)
    # lat/lng sometimes empty string
    if not row["lat"] or not row["lng"]:
        row = {**row, **{"lat": -1, "lng": -1}}
    return EventLog(**row)


def add_timestamp(element: EventLog):
    ts = datetime.datetime.strptime(
        element.event_datetime, "%Y-%m-%dT%H:%M:%S.%f"
    ).timestamp()
    return beam.window.TimestampedValue(element, ts)


class AddWindowTS(beam.DoFn):
    def process(self, element: int, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().isoformat(timespec="seconds")
        window_end = window.end.to_utc_datetime().isoformat(timespec="seconds")
        output = {
            "window_start": window_start,
            "window_end": window_end,
            "page_views": element,
        }
        yield output


def run():
    parser = argparse.ArgumentParser(
        description="Process statistics by minute from website visit event"
    )
    parser.add_argument(
        "--inputs",
        default="inputs",
        help="Specify folder name that event records are saved",
    )
    parser.add_argument(
        "--runner", default="DirectRunner", help="Specify Apache Beam Runner"
    )
    opts = parser.parse_args()
    PARENT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    pipeline_opts = {
        "runner": opts.runner,
        "flink_master": "localhost:8081",
        "job_name": "traffic-aggregation",
        "environment_type": "LOOPBACK",
        "streaming": True,
        "parallelism": 3,
        "experiments": [
            "use_deprecated_read"
        ],  ## https://github.com/apache/beam/issues/20979
        "checkpointing_interval": "60000",
    }
    options = PipelineOptions([], **pipeline_opts)
    # Required, else it will complain that when importing worker functions
    options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=options)
    (
        p
        | "Read messages"
        >> kafka.ReadFromKafka(
            consumer_config={
                "bootstrap.servers": os.getenv(
                    "BOOTSTRAP_SERVERS", "host.docker.internal:29092"
                ),
                "group.id": "traffic-agg",
            },
            topics=["website-visit"],
        )
        | "Parse elements" >> beam.Map(parse_json).with_output_types(EventLog)
        # | "Add event timestamp" >> beam.Map(add_timestamp)
        # | "Tumble window per minute" >> beam.WindowInto(beam.window.FixedWindows(60))
        # | "Count per minute"
        # >> beam.CombineGlobally(CountCombineFn()).without_defaults()
        # | "Add window timestamp" >> beam.ParDo(AddWindowTS())
        # | "Write to file"
        # >> beam.io.WriteToText(
        #     file_path_prefix=os.path.join(
        #         PARENT_DIR,
        #         "outputs",
        #         f"{int(datetime.datetime.now().timestamp() * 1000)}",
        #     ),
        #     file_name_suffix=".out",
        # )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()


if __name__ == "__main__":
    run()
