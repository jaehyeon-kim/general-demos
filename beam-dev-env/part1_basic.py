import argparse
import json
import logging
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


def run():
    parser = argparse.ArgumentParser(description="Process website visit event")
    parser.add_argument(
        "--path", default="inputs", help="Specify path that event records are saved"
    )
    parser.add_argument(
        "--runner", default="DirectRunner", help="Specify Apache Beam Runner"
    )
    opts = parser.parse_args()

    options = PipelineOptions()
    options.view_as(StandardOptions).runner = opts.runner

    p = beam.Pipeline(options=options)
    (
        p
        | "Read from files" >> beam.io.ReadFromText(file_pattern=f"{opts.path}/*.out")
        | "Parse Json" >> beam.Map(lambda line: json.loads(line))
        | "Filter status" >> beam.Filter(lambda d: d["opted_into_marketing"] is True)
        | "Select columns"
        >> beam.Map(
            lambda d: {
                k: v
                for k, v in d.items()
                if k in ["ip", "id", "lat", "lng", "age_bracket"]
            }
        )
        | "Write to file"
        >> beam.io.WriteToText(
            file_path_prefix=f"outputs/{str(uuid.uuid4())}-", file_name_suffix=".out"
        )
    )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Building pipeline ...")

    p.run()


if __name__ == "__main__":
    run()
