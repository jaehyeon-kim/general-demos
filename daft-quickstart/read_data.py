import daft
from utils import QueryBuilder, create_connection

#### 1. basic examples
USER_STMT = "SELECT id, first_name, last_name, email FROM staging.users"

## lasy evaluation
df = daft.read_sql(sql=USER_STMT, conn=create_connection)
df.show(1300)

## parallel reads - numeric or temporal type
df = daft.read_sql(
    sql=USER_STMT, conn=create_connection, partition_col="id", num_partitions=9
)
df.show(2347)

# create on the fly
df = daft.read_sql(
    sql="SELECT first_name, last_name, email, row_number() over() AS rn FROM staging.users",
    conn=create_connection,
    partition_col="rn",
    num_partitions=9,
)
df.show(2347)

#### 2. join tables
JOIN_STMT = """
WITH cte_orders_expanded AS (
    SELECT id, user_id, jsonb_array_elements(items) AS order_item
    FROM staging.orders
), cte_orders_processed AS (
    SELECT 
        id AS order_id
        , user_id
        , jsonb_extract_path(order_item, 'product_id')::int AS product_id
        , jsonb_extract_path(order_item, 'quantity')::int AS quantity
    FROM cte_orders_expanded
)
SELECT
    order_id
    , o.user_id
    , u.first_name
    , u.last_name
    , o.product_id
    , p.name
    , o.quantity
    , p.price
FROM cte_orders_processed o
JOIN staging.users u ON o.user_id = u.id
JOIN staging.products p ON o.product_id = p.id
"""

df = daft.read_sql(sql=JOIN_STMT, conn=create_connection)
df.show(1500)

#### 3. parallel processing idea
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

for stmt in builder.build_query_stmts():
    print(stmt)


USER_STMT = "SELECT * FROM staging.users"
builder = QueryBuilder(
    sql=USER_STMT,
    conn=create_connection,
    disable_pushdowns_to_sql=False,
    infer_schema=False,
    infer_schema_length=10,
    schema=None,
    projection=None,
    predicate=None,
    limit=7700,
    partition_col="id",
    num_partitions=7,
)

for stmt in builder.build_query_stmts():
    print(stmt)
