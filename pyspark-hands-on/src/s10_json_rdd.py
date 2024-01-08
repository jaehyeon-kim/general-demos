import os
import json
import re

from pyspark.sql import SparkSession

# https://www.kaggle.com/datasets/Cornell-University/arxiv

DATA_PATH = "/home/hadoop/project/data/"

spark: SparkSession = (
    SparkSession.builder.appName("JSON practice RDD").master("local[*]").getOrCreate()
)
sc = spark.sparkContext

rdd_json = sc.textFile(
    os.path.join(DATA_PATH, "arvix", "arxiv-metadata-oai-snapshot.json"), 100
)
rdd = rdd_json.map(lambda r: json.loads(r))
rdd.persist()

## count number of records
sc.defaultParallelism
rdd.getNumPartitions()
rdd.count()
rdd.take(2)

## get attributes
rdd.flatMap(lambda r: r.keys()).distinct().collect()

## get the name of licenses
rdd.map(lambda r: r.get("license", None)).distinct().collect()

## get the shorted and longest titles
rdd.map(lambda r: r["title"]).reduce(lambda x, y: min(x, y))
rdd.map(lambda r: r["title"]).reduce(lambda x, y: max(x, y))


## find abbreviations with 5 or more letters in the abstract eg) (aaaaa)
def get_abb(s):
    return re.search(r"\([a-zA-Z]{5, }?\)", s) is not None


rdd.filter(lambda r: get_abb(r["abstract"])).count()

## get the number of archive records per month (update_date attribute)
rdd.map(lambda r: (r["update_date"].split("-")[1], 1)).countByKey()


## get average number of pages
def get_pages(s):
    if not isinstance(s, str):
        s = str(s)
    result = re.search(r"[0-9]+ pages", s)
    if result is not None:
        return int(re.sub(" pages", "", result.group()))
    else:
        return None


rdd.map(lambda r: get_pages(r["comments"])).filter(lambda r: r is not None).mean()
