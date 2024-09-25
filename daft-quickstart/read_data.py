import daft
from sqlalchemy import create_engine
from daft import context
from daft.sql.sql_scan import SQLScanOperator
from daft.daft import PythonStorageConfig, StorageConfig


def create_connection():
    return create_engine("sqlite:///example.db", echo=True).connect()


df = daft.read_sql("SELECT * FROM books", "sqlite://example.db")

df = daft.read_sql("SELECT *, ROW_NUMBER () OVER() AS rn FROM books", create_connection)


df = daft.read_sql(
    sql="SELECT *, ROW_NUMBER () OVER() AS rn FROM books",
    conn=create_connection,
    partition_col="rn",
    num_partitions=3,
)

stmt = "SELECT *, ROW_NUMBER () OVER() AS rn FROM books"

io_config = context.get_context().daft_planning_config.default_io_config
storage_config = StorageConfig.python(PythonStorageConfig(io_config))


sql_op = SQLScanOperator(
    sql=stmt,
    conn=create_connection,
    storage_config=storage_config,
    disable_pushdowns_to_sql=False,
    infer_schema=False,
    infer_schema_length=10,
    schema=None,
    partition_col="rn",
    num_partitions=3,
)
