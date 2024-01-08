import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    BooleanType,
    IntegerType,
)
from pyspark.sql.functions import col, udf

# https://www.kaggle.com/datasets/Cornell-University/arxiv

DATA_PATH = "/home/hadoop/project/data/"

spark: SparkSession = (
    SparkSession.builder.appName("JSON practice RDD").master("local[*]").getOrCreate()
)

df = spark.read.json(
    os.path.join(DATA_PATH, "arvix", "arxiv-metadata-oai-snapshot.json")
)
df.printSchema()

df.rdd.getNumPartitions()
# df.repartition()

## create a new schema
schema = StructType(
    [
        StructField("authors", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("license", StringType(), True),
        StructField("comments", StringType(), True),
        StructField("abstract", StringType(), True),
        StructField("versions", ArrayType(StringType()), True),
    ]
)

## binding data to a schema
df = spark.read.json(
    os.path.join(DATA_PATH, "arvix", "arxiv-metadata-oai-snapshot.json"), schema=schema
)
df.show()

## missing values for comments and license attributes
df = df.dropna(subset=["comments"]).fillna(value="unknown", subset=["license"])

## get the author names who published a paper in the math category
df.createOrReplaceTempView("archive")

sql_query = """
SELECT authors 
FROM archive
WHERE categories LIKE 'math%'
"""
spark.sql(sql_query).count()

df.filter(col("categories").startswith("math")).count()


## get licenses with 5 letters in the abstract
def get_abb(s):
    return re.search(r"\([a-zA-Z]{5,}?\)", s) is not None


get_abb_udf = udf(get_abb, BooleanType())
df.filter(get_abb_udf(col("abstract"))).count()


## extract the statistic of the number of pages for unknown licences
def get_pages(s):
    if not isinstance(s, str):
        s = str(s)
    result = re.search(r"[0-9]+ pages", s)
    if result is not None:
        return int(re.sub(" pages", "", result.group()))
    else:
        return 0


get_pages_udf = udf(get_pages, IntegerType())
df.withColumn("pages", get_pages_udf(col("comments"))).filter(
    col("license") == "unknown"
).select("pages").describe().show()
