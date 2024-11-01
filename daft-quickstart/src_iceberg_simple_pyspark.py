import os
from pyspark.sql import SparkSession

from utils_iceberg import clean_up, get_spark_session

CATALOG_NAME = "simple"
CATALOG_PATH = f"{os.getcwd()}/{CATALOG_NAME}_catalog.db"
WAREHOUSE_PATH = f"{os.getcwd()}/{CATALOG_NAME}_warehouse"

clean_up(CATALOG_PATH, WAREHOUSE_PATH)

# Initialize Spark session with Iceberg configurations
spark: SparkSession = get_spark_session(CATALOG_NAME, CATALOG_PATH, WAREHOUSE_PATH)

# Verify Spark session creation
spark.sql("SHOW DATABASES").show()

# Create an Iceberg table
spark.sql(f"""
  CREATE TABLE {CATALOG_NAME}.demo.sample (
    id INT,
    name STRING,
    value INT,
    age INT) 
    USING iceberg
    PARTITIONED BY (value)
    """)

# Insert some sample data
spark.sql(f"""
  INSERT INTO {CATALOG_NAME}.demo.sample VALUES
    (1, 'A', 1, 34),
    (2, 'B', 2, 20),
    (3, 'C', 1, 32),
    (4, 'D', 2, 50),
    (5, 'E', 2, 35)""")

# Query the data
result = spark.sql(f"SELECT * FROM {CATALOG_NAME}.demo.sample")
result.show()

spark.sql(f"SELECT * FROM {CATALOG_NAME}.demo.sample WHERE value=2").show()

spark.sql(f"""
  UPDATE {CATALOG_NAME}.demo.sample
    SET value = 2
  WHERE id = 1""")

spark.sql(f"""
  DELETE FROM {CATALOG_NAME}.demo.sample
  WHERE id = 5""")
