from utils_db import create_sqlite
from utils_builder import QueryBuilder

USER_STMT = "SELECT id, name, num, ROW_NUMBER() OVER() AS rn FROM example LIMIT 770"
builder = QueryBuilder(
    sql=USER_STMT,
    conn=create_sqlite,
    disable_pushdowns_to_sql=False,
    infer_schema=False,
    infer_schema_length=10,
    schema=None,
    partition_col="rn",
    num_partitions=7,
)

for stmt in builder.build_query_stmts():
    print(stmt)
