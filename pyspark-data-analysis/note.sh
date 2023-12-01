# PySpark Overview
# https://spark.apache.org/docs/latest/api/python/index.html

## spark submit
docker exec spark spark-submit \
  --master local[*] --deploy-mode client <path-to-script>

## pyspark
docker exec -it spark pyspark --master local[*] --deploy-mode client 

export PYTHONPATH="${SPARK_HOME}/python:${PYTHONPATH}" \
  && export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}" \
  && export PYSPARK_PYTHONPATH_SET=1