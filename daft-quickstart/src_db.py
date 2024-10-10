import daft
from daft import DataType
from utils_db import create_sqlite, create_postgres

engine = "postgres"
db_name = "develop"
conn_fn = create_postgres if engine == "postgres" else create_sqlite

SCHEMA = {
    "index": DataType.int16(),
    "val": DataType.int16(),
    "name": DataType.string(),
    "created_at": DataType.date(),
}

df = daft.read_sql(
    sql="SELECT * FROM demo LIMIT 770",
    conn=lambda: conn_fn(db_name),
    partition_col="index",
    num_partitions=7,
    infer_schema=True,
)
df.show(776)

df = daft.read_sql(
    sql="SELECT * FROM demo LIMIT 770",
    conn=lambda: conn_fn(db_name),
    partition_col="created_at",
    num_partitions=7,
    infer_schema=True,
)
df.show(776)

df = daft.read_sql(
    sql="SELECT * FROM demo LIMIT 770",
    conn=lambda: conn_fn(db_name),
    partition_col="val",
    num_partitions=7,
    infer_schema=True,
)
df.show(776)
