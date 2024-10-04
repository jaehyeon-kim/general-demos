import argparse
import logging
import typing
import time

import apache_beam as beam
from apache_beam.transforms.util import Reshuffle
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker


class GenerateQueryFn(beam.DoFn):
    def process(self, _: typing.Any):
        for query in self._generate_queries(None):
            yield query

    def _generate_queries(self, query_config):
        if query_config is None:
            logging.warning("To provide query config...")

        return [
            "SELECT * FROM (SELECT id, first_name, last_name, email FROM staging.users) AS subquery WHERE id >= 1 AND id < 1112",
            "SELECT * FROM (SELECT id, first_name, last_name, email FROM staging.users) AS subquery WHERE id >= 1112 AND id < 2223",
            "SELECT * FROM (SELECT id, first_name, last_name, email FROM staging.users) AS subquery WHERE id >= 2223 AND id < 3334",
            "SELECT * FROM (SELECT id, first_name, last_name, email FROM staging.users) AS subquery WHERE id >= 3334 AND id < 4445",
            "SELECT * FROM (SELECT id, first_name, last_name, email FROM staging.users) AS subquery WHERE id >= 4445 AND id < 5556",
            "SELECT * FROM (SELECT id, first_name, last_name, email FROM staging.users) AS subquery WHERE id >= 5556 AND id < 6667",
            "SELECT * FROM (SELECT id, first_name, last_name, email FROM staging.users) AS subquery WHERE id >= 6667 AND id < 7778",
            "SELECT * FROM (SELECT id, first_name, last_name, email FROM staging.users) AS subquery WHERE id >= 7778 AND id < 8889",
            "SELECT * FROM (SELECT id, first_name, last_name, email FROM staging.users) AS subquery WHERE id >= 8889 AND id <= 10000",
        ]


class ProcessQueryFn(beam.DoFn, RestrictionProvider):
    def process(
        self,
        query: str,
        tracker: OffsetRestrictionTracker = beam.DoFn.RestrictionParam(),
    ):
        restriction = tracker.current_restriction()
        for current_position in range(restriction.start, restriction.stop):
            if tracker.try_claim(current_position):
                print(query)
                time.sleep(2)
                yield query
            else:
                return

    def create_tracker(self, restriction: OffsetRange) -> OffsetRestrictionTracker:
        return OffsetRestrictionTracker(restriction)

    def initial_restriction(self, _: str) -> OffsetRange:
        return OffsetRange(start=0, stop=1)

    def restriction_size(self, _: str, restriction: OffsetRange) -> int:
        return restriction.size()


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Beam pipeline arguments")
    known_args, pipeline_args = parser.parse_known_args(argv)

    # # We use the save_main_session option because one or more DoFn's in this
    # # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    print(f"known args - {known_args}")
    print(f"pipeline options - {pipeline_options.display_data()}")

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | beam.Create([0])
            | beam.ParDo(GenerateQueryFn())
            | Reshuffle()
            | beam.ParDo(ProcessQueryFn())
        )

        logging.getLogger().setLevel(logging.WARN)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    """
    python pipeline_reshuffle.py --direct_num_workers=3 --direct_running_mode=multi_threading

    python pipeline_reshuffle.py --job_name=sql-query --runner FlinkRunner \
        --flink_master=localhost:8081 --environment_type=LOOPBACK --parallelism=3
    """
    run()
