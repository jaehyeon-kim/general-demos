from typing import Callable, Union
from sqlalchemy import Connection
from daft import context
from daft.datatype import DataType
from daft.sql.sql_scan import SQLScanOperator
from daft.daft import (
    PythonStorageConfig,
    StorageConfig,
)
from daft.sql.sql_connection import SQLConnection


class QueryBuilder:
    def __init__(
        self,
        sql: str,
        conn: Union[Callable[[], "Connection"], str],
        disable_pushdowns_to_sql: bool,
        infer_schema: bool,
        infer_schema_length: int,
        schema: dict[str, DataType] | None,
        partition_col: str | None = None,
        num_partitions: int | None = None,
    ) -> None:
        self.sql = sql
        self.conn = conn
        self.disable_pushdowns_to_sql = disable_pushdowns_to_sql
        self.infer_schema = infer_schema
        self.infer_schema_length = infer_schema_length
        self.schema = schema
        self.projection = None  # projection
        self.predicate = None  # predicate
        self.limit = None  # limit
        self.partition_col = partition_col
        self.num_partitions = num_partitions
        self.sql_conn = self._set_sql_conn()
        self.sql_operator = self._set_sql_operator()

    def _set_sql_conn(self):
        return (
            SQLConnection.from_url(self.conn)
            if isinstance(self.conn, str)
            else SQLConnection.from_connection_factory(self.conn)
        )

    def _set_sql_operator(self):
        io_config = context.get_context().daft_planning_config.default_io_config
        storage_config = StorageConfig.python(PythonStorageConfig(io_config))
        return SQLScanOperator(
            sql=self.sql,
            conn=self.sql_conn,
            storage_config=storage_config,
            disable_pushdowns_to_sql=self.disable_pushdowns_to_sql,
            infer_schema=self.infer_schema,
            infer_schema_length=self.infer_schema_length,
            schema=self.schema,
            partition_col=self.partition_col,
            num_partitions=self.num_partitions,
        )

    def build_query_stmts(self):
        query_stmts = []
        partition_bounds, _ = self.sql_operator._get_partition_bounds_and_strategy(
            num_scan_tasks=self.num_partitions
        )
        partition_bounds = [int(p) for p in partition_bounds]
        partition_tuples = list(zip(partition_bounds[::1], partition_bounds[1::1]))
        for i, tup in enumerate(partition_tuples):
            left_clause = f"{self.partition_col} >= {tup[0]}"
            right_clause = (
                f"{self.partition_col} < {tup[1]}"
                if len(partition_tuples) - i > 1
                else f"{self.partition_col} <= {tup[1]}"
            )
            query_stmt = self.sql_conn.construct_sql_query(
                sql=self.sql,
                projection=self.projection,
                predicate=self.predicate,
                limit=self.limit,
                partition_bounds=(left_clause, right_clause),
            )
            query_stmts.append(query_stmt)
        return query_stmts
