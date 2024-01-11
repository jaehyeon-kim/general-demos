import os
import random

from pyspark.sql import SparkSession
from pyspark import StorageLevel

os.environ["PYTHONHASHSEED"] = str(232)
# prevent the following error for lookup
#   RuntimeError: Randomness of hash of string should be disabled via PYTHONHASHSEED

spark: SparkSession = (
    SparkSession.builder.appName("RDD practice 02").master("local[*]").getOrCreate()
)
spark.conf.set("spark.executorEnv.PYTHONHASHSEED", "321")

sc = spark.sparkContext
print(sc)

random_list = random.sample(range(0, 40), 10)

rdd1 = sc.parallelize(random_list, 4)
rdd2 = sc.parallelize([1, 14, 20, 20, 28, 10, 13, 3], 2)

rdd3 = sc.parallelize(
    [(1, 4), (7, 10), (5, 7), (1, 12), (7, 12), (7, 1), (9, 1), (7, 4)], 2
)

## more operations
rdd_union = rdd1.union(rdd2)
rdd_union.getNumPartitions()
rdd_union.collect()

rdd_intersction = rdd1.intersection(rdd2)
rdd_intersction.getNumPartitions()
rdd_intersction.collect()

# get # of empty partitions
len([p for p in rdd_intersction.glom().collect() if len(p) == 0])
rdd_intersction.glom().collect()
rdd_intersction.coalesce(1).glom().collect()

rdd1.takeSample(False, 5)
rdd1.takeOrdered(5)
rdd1.takeOrdered(5, lambda x: -x)

rdd3.collect()
rdd3.keys().collect()
rdd3.values().collect()
rdd3.reduceByKey(lambda x, y: x + y).collect()
rdd3.reduceByKey(lambda x, y: x + y).sortByKey().collect()
rdd3.countByKey()
sorted(rdd3.countByKey().items())

for item in rdd3.groupByKey().collect():
    print(f"key - {str(item[0])}, values - {', '.join([str(v) for v in item[1]])}")

rdd3.groupByKey().lookup(7)

rdd3.persist()
rdd1.persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
