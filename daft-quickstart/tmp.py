from typing import Union, Callable
from sqlalchemy import Connection, create_engine

from daft.daft import PyExpr, Pushdowns
from daft.datatype import DataType
from utils import QueryBuilder, create_connection


class MySQLScanOperator:
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
        pass


USER_STMT = "SELECT * FROM staging.users LIMIT 7700"
builder = QueryBuilder(
    sql=USER_STMT,
    conn=create_connection,
    disable_pushdowns_to_sql=False,
    infer_schema=False,
    infer_schema_length=10,
    schema=None,
    projection=None,
    predicate=None,
    limit=None,
    partition_col="id",
    num_partitions=7,
)
sql_op = builder._set_sql_operator()

pp: Pushdowns = Pushdowns
pp.columns = ["first_name"]
pp.filters = None
pp.limit = None
pp.partition_filters = None

predicate_sql = pp.filters.to_sql() if pp.filters is not None else None

sql_op.conn.construct_sql_query(
    USER_STMT,
    projection=pp.columns,
    predicate=predicate_sql,
    limit=pp.limit,
    partition_bounds=pp,
)

st = sql_op.to_scan_tasks(pushdowns=pp)

for t in st:
    print(t)


t = next(iter(st))

t.__dict__
