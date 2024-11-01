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
spark.sql("SHOW DATABASES").show()

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

# Create an iceberg table
spark.sql(f"""
  CREATE TABLE {CATALOG_NAME}.demo.sample
  USING iceberg
  PARTITIONED BY (bucket(16, id), days(ts), category)
  AS SELECT id, data, category, to_timestamp(ts) AS ts
    FROM {CATALOG_NAME}.demo.staging""")

spark.sql(f"SELECT * FROM {CATALOG_NAME}.demo.sample").show()
