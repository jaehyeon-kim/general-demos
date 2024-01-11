import numpy as np
import random

from pyspark.sql import SparkSession

spark: SparkSession = (
    SparkSession.builder.appName("JSON practice RDD").master("local[*]").getOrCreate()
)
sc = spark.sparkContext

data_sample = [
    (1, 4),
    (2, 2),
    (2, 1),
    (3, 5),
    (2, 5),
    (2, 10),
    (2, 7),
    (3, 4),
    (2, 1),
    (2, 4),
    (4, 4),
]
rdd_sample = sc.parallelize(data_sample, 3)

rdd_sample.glom().collect()

rdd_sample_grouped = rdd_sample.groupByKey()
for item in rdd_sample_grouped.collect():
    print(item[0], [v for v in item[1]])
# <P1> 3 [5, 4]
# <P2> 1 [4]
# <P2> 4 [4]
# <P3> 2 [2, 1, 5, 10, 7, 1, 4]

rdd_sample_grouped.glom().collect()

## loading numpy as np
key_1 = ["a"] * 10
key_2 = ["b"] * 6000000
key_3 = ["c"] * 800
key_4 = ["d"] * 10000

keys = key_1 + key_2 + key_3 + key_4
random.shuffle(keys)

values_1 = np.random.randint(low=1, high=100, size=len(key_1))
values_2 = np.random.randint(low=1, high=100, size=len(key_2))
values_3 = np.random.randint(low=1, high=100, size=len(key_3))
values_4 = np.random.randint(low=1, high=100, size=len(key_4))

values = np.concatenate((values_1, values_2, values_3, values_4))

pair_skew = list(zip(keys, values))

rdd = sc.parallelize(pair_skew, 8)
rdd_grouped = rdd.groupByKey().cache()

rdd_grouped.map(lambda pair: (pair[0], [(i + 10) for i in pair[1]])).count()


## salting
def salting(val):
    return val + "_" + str(random.randint(0, 5))
