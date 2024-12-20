# import polars as pl

# uri = "sqlite://develop.db"
# query = "SELECT * FROM demo"
# df = pl.read_database_uri(query, uri)


# df = pl.read_database_uri(
#     query, uri, partition_on="id", partition_num=5, engine="connectorx"
# )


import connectorx as cx

uri = "sqlite://develop.db"
query = "SELECT * FROM demo LIMIT 770"

df = cx.read_sql(uri, query, partition_on="id", partition_num=4)
df.to_dict("records")

cx.partition_sql(conn=uri, query=query, partition_on="id", partition_num=4)
