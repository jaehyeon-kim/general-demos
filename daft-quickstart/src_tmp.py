from typing import Union, Callable
from sqlalchemy import Connection

from daft.daft import Pushdowns
from daft.datatype import DataType
from daft.table import Table

from utils_builder import QueryBuilder
from utils_db import create_sqlite, create_postgres

engine = "sqlite"
db_name = "develop"
conn_fn = create_postgres if engine == "postgres" else create_sqlite

SCHEMA = {
    "id": DataType.int64(),
    "val": DataType.int64(),
    "name": DataType.string(),
    "created_at": DataType.date(),
}

my_builder = QueryBuilder(
    sql="SELECT * FROM demo LIMIT 770",
    conn=lambda: conn_fn(db_name),
    disable_pushdowns_to_sql=False,
    infer_schema=False,
    infer_schema_length=10,
    schema=SCHEMA,
    partition_col="created_at",
    num_partitions=7,
)
sql_op = my_builder.sql_operator

num_scan_tasks = 7
sql_op._get_partition_bounds_and_strategy(num_scan_tasks)

pa_table, strategy = sql_op._attempt_partition_bounds_read(num_scan_tasks)
pydict = Table.from_arrow(pa_table).to_pydict()

USER_STMT = "SELECT * FROM staging.users LIMIT 7700"
builder = QueryBuilder(
    sql=USER_STMT,
    conn=create_postgres,
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
