# PySpark Overview
# https://spark.apache.org/docs/latest/api/python/index.html

export SCRIPT_PATH=/home/hadoop/project/src/s10_json_rdd.py

## spark submit
docker exec spark spark-submit \
  --master local[*] --deploy-mode client $SCRIPT_PATH

## pyspark
docker exec -it spark pyspark --master local[*] --deploy-mode client

export PYTHONPATH="${SPARK_HOME}/python:${PYTHONPATH}" \
  && export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}" \
  && export PYSPARK_PYTHONPATH_SET=1


# spark shuffling (manage skew and spill)

# solve data skew first (eg with salting technique)
# repartitioning with known number of partitions <-- reduce spill
# increase the memory of workers

# spark.conf.set("spark.executor.memory", 75g)
# spark.conf.set("spark.driver.memory", 100g)

# update # of shuffle partitions
# spark.conf.set("spark.sql.shuffle.partitions", 8)

# ---

# broadcast hash join