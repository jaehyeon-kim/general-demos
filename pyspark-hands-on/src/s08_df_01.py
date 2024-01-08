import os

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

DATA_PATH = "/home/hadoop/project/data/"

spark: SparkSession = (
    SparkSession.builder.appName("DataFrame practice 01")
    .master("local[*]")
    .getOrCreate()
)

df1: DataFrame = spark.read.format("csv").load(
    os.path.join(DATA_PATH, "CompleteDataset.csv"), inferSchema=True, header=True
)

## operations
df1.show()
df1.rdd.getNumPartitions()  # default 2
df1 = df1.repartition(4)
df1.rdd.getNumPartitions()
# df1.coalesce(1)

df1.printSchema()
col_map = {
    c: c.lower().replace(" ", "") if c != "_c0" else "rec_id" for c in df1.columns
}
df1 = df1.withColumnsRenamed(col_map)
# df1.dtypes
df1.na.fill({"ram": 10, "rb": 1}).select("ram", "rb").show()


df1.select("name", "overall").distinct().show()
df1.filter(F.col("overall") > 70).select("name", "overall").show()

df1.select("overall", "name", "age").filter(F.col("overall") > 70).show()

df1.filter(F.col("overall") > 70).groupBy("age").count().sort("age").show()
pandas_df = (
    df1.filter(F.col("overall") > 70).groupBy("age").count().sort("age").toPandas()
)
pandas_df.plot(x="age", y="count", kind="bar")

## advanced operations
df1.createOrReplaceTempView("df_football")
sql_query = """
SELECT age, count(*) AS count
FROM df_football
WHERE overall > 70
GROUP BY age
ORDER BY age
"""
res = spark.sql(sql_query)
res.show()


# udf
def uppercase_converter(record: str):
    return record.upper() if len(record) > 10 else record.lower()


spark.udf.register("CUSTOM_UPPER", uppercase_converter)
sql_query = """
SELECT name, CUSTOM_UPPER(name) as c_name
FROM df_football
LIMIT 20
"""

spark.sql(sql_query).show()
