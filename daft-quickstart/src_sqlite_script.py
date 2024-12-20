import json
import unittest

import daft

from utils_db import create_sqlite


class TestSqlClient(unittest.TestCase):
    def test_this(self):
        df = daft.read_sql(
            sql="SELECT * FROM demo LIMIT 770",
            conn=lambda: create_sqlite("develop"),
            partition_col="id",
            num_partitions=7,
        )

        physical_plan_scheduler = df._builder.to_physical_plan_scheduler(
            daft.context.get_context().daft_execution_config
        )
        print(physical_plan_scheduler)
        physical_plan_dict = json.loads(physical_plan_scheduler.to_json_string())

        sql_queries = []
        for task in physical_plan_dict["TabularScan"]["scan_tasks"]:
            sql_queries.append(task["file_format_config"]["Database"]["sql"])
        print(sql_queries)
