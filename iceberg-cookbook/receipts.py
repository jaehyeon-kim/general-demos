from pyspark.sql import SparkSession
from pyspark.sql.functions import col, days

spark: SparkSession = (
    SparkSession.builder.appName("iceberg-cookbook").master("local[*]").getOrCreate()
)

#### create a table from parquet

## loading source data from parquet
df = spark.read.parquet("s3://nyc-tlc/trip data/yellow_tripdata_2020-02.parquet")
df.printSchema()

updated_df = df.withColumn("PULocationID", col("PULocationID").cast("int"))

## creating the iceberg table
spark.sql("CREATE DATABASE IF NOT EXISTS nyc")
updated_df.orderBy(col("tpep_pickup_datetime")).writeTo("nyc.taxis").partitionedBy(
    days("tpep_pickup_datetime")
).createOrReplace()

spark.sql("DESCRIBE TABLE nyc.taxis").show(truncate=False)
spark.sql("SELECT * FROM nyc.taxis LIMIT 5").show()

#### create a table from csv
csv_df = spark.read.csv(
    "s3://nyc-tlc/csv_backup/yellow_tripdata_2022-02.csv", header=True, inferSchema=True
)
# csv_df = spark.read.csv(
#     's3://nyc-tlc/csv_backup/yellow_tripdata_2022-02.csv',
#     schema='vendor_id int, tpep_pickup_ts timestamp, ...'
# )
csv_df.printSchema()

#### create a table from json
csv_df.coalesce(1).write.json("/tmp/yellow_tripdata_2022-02/")

spark.read.json("/tmp/yellow_tripdata_2022-02/").printSchema()
# datetime columns are shown as strings
# try infer schema
spark.read.option("inferTimestamp", "True").json(
    "/tmp/yellow_tripdata_2022-02/"
).printSchema()

# json_df = spark.read.json(
#     '/tmp/yellow_tripdata_2022-02/json/',
#     schema='VendorID int, tpep_pickup_datetime timestamp, ...'
# )
