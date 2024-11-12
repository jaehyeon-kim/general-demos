import daft
from daft import DataType
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

df = daft.read_sql(
    sql="SELECT * FROM demo LIMIT 770",
    conn=lambda: conn_fn(db_name),
    partition_col="id",
    num_partitions=7,
    schema=SCHEMA,
)
df._builder.optimize().to_physical_plan_scheduler(
    daft.context.get_context().daft_execution_config
)
df.collect()
df.to_pylist()
df.iter_rows()

for r in df.iter_rows():
    print(r)

df = daft.read_sql(
    sql="SELECT * FROM demo LIMIT 770",
    conn=lambda: conn_fn(db_name),
    partition_col="created_at",
    num_partitions=7,
    schema=SCHEMA,
)
df.show(776)

df = daft.read_sql(
    sql="SELECT * FROM demo LIMIT 770",
    conn=lambda: conn_fn(db_name),
    partition_col="val",
    num_partitions=7,
    schema=SCHEMA,
)
df.show(776)
