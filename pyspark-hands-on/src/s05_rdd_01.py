import random

from pyspark.sql import SparkSession

spark: SparkSession = (
    SparkSession.builder.appName("RDD practice 01").master("local[*]").getOrCreate()
)
sc = spark.sparkContext
print(sc)

random_list = random.sample(range(0, 40), 10)

rdd1 = sc.parallelize(random_list, 4)

## actions
rdd1.collect()
rdd1.getNumPartitions()
rdd1.glom().collect()
rdd1.glom().take(2)
rdd1.glom().collect()[-1]
rdd1.count()
rdd1.first()
rdd1.top(2)
rdd1.reduce(lambda x, y: x + y)

## transformations
rdd1.distinct().collect()
rdd1.map(lambda r: (r + 1) * 3).collect()
rdd1.filter(lambda r: r % 2 == 0).collect()
# rdd1.filter(lambda r: r % 2 == 0).glom().collect()
# rdd1.filter(lambda r: r % 2 == 0).repartition(1).glom().collect()
rdd1.map(lambda r: [r + 2, r + 5]).collect()
rdd1.flatMap(lambda r: [r + 2, r + 5]).collect()

## descriptive statistics
rdd1.max()
rdd1.min()
rdd1.mean()
rdd1.stdev()
rdd1.sum()


## mapPartitions()
def f(iterator):
    yield sum(iterator)


rdd1.mapPartitions(f).collect()
