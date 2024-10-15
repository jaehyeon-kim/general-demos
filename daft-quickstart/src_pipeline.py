import argparse
import logging
import typing

import daft
from daft import DataType
import apache_beam as beam
from apache_beam.transforms.util import Reshuffle
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker

from utils_db import create_sqlite, create_postgres


class GenerateQueryFn(beam.DoFn):
    def process(self, _: typing.Any):
        for query in self._generate_queries(None):
            yield query

    def _generate_queries(self, query_config):
        if query_config is None:
            logging.warning("To provide query config...")

        return [
            "SELECT * FROM (SELECT * FROM demo LIMIT 770) AS subquery WHERE id >= 0 AND id < 109.85714285714286",
            "SELECT * FROM (SELECT * FROM demo LIMIT 770) AS subquery WHERE id >= 109.85714285714286 AND id < 219.71428571428572",
            "SELECT * FROM (SELECT * FROM demo LIMIT 770) AS subquery WHERE id >= 219.71428571428572 AND id < 329.57142857142856",
            "SELECT * FROM (SELECT * FROM demo LIMIT 770) AS subquery WHERE id >= 329.57142857142856 AND id < 439.42857142857144",
            "SELECT * FROM (SELECT * FROM demo LIMIT 770) AS subquery WHERE id >= 439.42857142857144 AND id < 549.2857142857143",
            "SELECT * FROM (SELECT * FROM demo LIMIT 770) AS subquery WHERE id >= 549.2857142857143 AND id < 659.1428571428571",
            "SELECT * FROM (SELECT * FROM demo LIMIT 770) AS subquery WHERE id >= 659.1428571428571 AND id <= 769",
        ]


class ProcessQueryFn(beam.DoFn, RestrictionProvider):
    def __init__(self, engine: str = "sqlite", db_name: str = "develop"):
        self.engine = engine
        self.db_name = db_name
        self.schema = self._schema()

    def process(
        self,
        query: str,
        tracker: OffsetRestrictionTracker = beam.DoFn.RestrictionParam(),
    ):
        restriction = tracker.current_restriction()
        for current_position in range(restriction.start, restriction.stop):
            if tracker.try_claim(current_position):
                print(query)
                df = daft.read_sql(
                    sql=query,
                    conn=self._conn_fn(),
                    schema=self.schema,
                )
                for item in df.to_pylist():
                    yield item
            else:
                return

    def create_tracker(self, restriction: OffsetRange) -> OffsetRestrictionTracker:
        return OffsetRestrictionTracker(restriction)

    def initial_restriction(self, _: str) -> OffsetRange:
        return OffsetRange(start=0, stop=1)

    def restriction_size(self, _: str, restriction: OffsetRange) -> int:
        return restriction.size()

    def _conn_fn(self):
        conn_fn = create_postgres if self.engine == "postgres" else create_sqlite
        return lambda: conn_fn(self.db_name)

    def _schema(self):
        {
            "id": DataType.int64(),
            "val": DataType.int64(),
            "name": DataType.string(),
            "created_at": DataType.date(),
        }


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
            | beam.Map(print)
        )

        logging.getLogger().setLevel(logging.WARN)
        logging.info("Building pipeline ...")


if __name__ == "__main__":
    """
    python src_pipeline.py --direct_num_workers=3 --direct_running_mode=multi_threading

    # not working due to sql execution fails, to test with postgresql
    python src_pipeline.py --job_name=sql-query --runner FlinkRunner \
        --flink_master=localhost:8081 --environment_type=LOOPBACK --parallelism=3
    """
    run()
