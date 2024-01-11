## SparkContext vs SparkSession

[Exploring SparkContext and SparkSession](https://medium.com/@akhilasaineni7/exploring-sparkcontext-and-sparksession-8369e60f658e)

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("test1").setMaster("local[*]")
sc = SparkContext(conf=conf)

spark: SparkSession = (
    SparkSession.builder.appName("test2").master("local[*]").getOrCreate()
)
sc = spark.sparkContext
```

## Resources

- [PySpark Full Course 2023 | PySpark Tutorial | Apache Spark Tutorial | Intellipaat](https://www.youtube.com/watch?v=0pnCIrv1a9M)
- [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
  - [RDD Persistence](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [PySpark Reference](https://spark.apache.org/docs/latest/api/python/index.html)
