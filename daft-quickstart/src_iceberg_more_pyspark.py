import os
from pyspark.sql import SparkSession

from utils_iceberg import clean_up, get_spark_session

CATALOG_NAME = "more"
CATALOG_PATH = f"{os.getcwd()}/{CATALOG_NAME}_catalog.db"
WAREHOUSE_PATH = f"{os.getcwd()}/{CATALOG_NAME}_warehouse"

clean_up(CATALOG_PATH, WAREHOUSE_PATH)

# Initialize Spark session with Iceberg configurations
spark: SparkSession = get_spark_session(CATALOG_NAME, CATALOG_PATH, WAREHOUSE_PATH)

# Verify Spark session creation
# spark.sql("SHOW DATABASES").show()

# Create and insert records to a staging table
spark.sql(f"""
    CREATE TABLE {CATALOG_NAME}.demo.staging (
        id bigint,
        data string,
        category string,
        ts string)
    USING iceberg;
  """)

spark.sql(f"""
  INSERT INTO {CATALOG_NAME}.demo.staging VALUES
    (1, 'A', 'C1', '2024-01-01'),
    (3, 'B', 'C2', '2024-01-02'),
    (8, 'C', 'C1', '2024-01-03'),
    (9, 'D', 'C2', '2024-01-01'),
    (2, 'E', 'C3', '2024-01-02'),
    (7, 'F', 'C1', '2024-01-01'),
    (12, 'G', 'C2', '2024-01-02')
    """)

spark.sql(f"SELECT * FROM {CATALOG_NAME}.demo.staging").show()

spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.demo.sample")

# Create an iceberg table
spark.sql(f"""
  CREATE TABLE {CATALOG_NAME}.demo.sample
  USING iceberg
  PARTITIONED BY (id, days(ts), category)
  AS SELECT id, data, category, to_timestamp_ltz(ts) AS ts
    FROM {CATALOG_NAME}.demo.staging""")

spark.sql(f"SELECT * FROM {CATALOG_NAME}.demo.sample").show()

spark.sql(f"""
  SELECT * 
  FROM {CATALOG_NAME}.demo.sample
  WHERE id <= 3""").show()

spark.sql(f"""
  SELECT * 
  FROM {CATALOG_NAME}.demo.sample
  WHERE id <= 3 AND ts <= to_date('2024-01-01')""").show()

spark.sql(f"""
  CREATE TABLE {CATALOG_NAME}.demo.sample1
  USING iceberg
  PARTITIONED BY (bucket(16, id), days(ts), category)
  AS SELECT id, data, category, to_timestamp(ts) AS ts
    FROM {CATALOG_NAME}.demo.staging""")

df = spark.sql(f"""SELECT * FROM {CATALOG_NAME}.demo.sample1 WHERE id <= 3""")
df.explain(True)

df1 = spark.sql(f"""SELECT * FROM {CATALOG_NAME}.demo.sample1 WHERE id = 3""")
df1.explain(True)

spark.sql(f"""SELECT partition FROM {CATALOG_NAME}.demo.sample1.files""").collect()

r = spark.sql(
    f"""EXPLAIN EXTENDED SELECT * FROM {CATALOG_NAME}.demo.sample1 WHERE id <= 3"""
).show(truncate=False)
print(r[0].plan)

spark.sql(f"DESCRIBE EXTENDED {CATALOG_NAME}.demo.sample1").show(100, truncate=False)

r = spark.sql(
    f"""EXPLAIN FORMATTED SELECT * FROM {CATALOG_NAME}.demo.sample1 WHERE id_bucket <= 3"""
).collect()
print(r[0].plan)
