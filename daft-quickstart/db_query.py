import daft
from daft import DataType
from db_utils import create_connection

SCHEMA = {
    "id": DataType.int16(),
    "name": DataType.string(),
    "created_at": DataType.date(),
}

df = daft.read_sql(
    sql="SELECT *FROM users",
    conn=create_connection,
    partition_col="id",
    num_partitions=9,
    infer_schema=False,
    schema=SCHEMA,
)
df.show(123)

df = daft.read_sql(
    sql="SELECT *FROM users",
    conn=create_connection,
    partition_col="created_at",
    num_partitions=9,
    infer_schema=False,
    schema=SCHEMA,
)
df.show(123)
