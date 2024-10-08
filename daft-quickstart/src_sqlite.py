import daft
from daft import DataType
from utils_db import create_sqlite

SCHEMA = {
    "id": DataType.int16(),
    "name": DataType.string(),
    "created_at": DataType.date(),
}

df = daft.read_sql(
    sql="SELECT id, name, num, ROW_NUMBER() OVER() AS rn FROM example LIMIT 770",
    conn=create_sqlite,
    partition_col="rn",
    num_partitions=7,
    infer_schema=True,
)
df.show(776)

df = daft.read_sql(
    sql="SELECT id, name, num, ROW_NUMBER() OVER() AS rn FROM example LIMIT 770",
    conn=create_sqlite,
    partition_col="id",
    num_partitions=7,
    infer_schema=True,
    # schema=SCHEMA,
)
df.show(776)
