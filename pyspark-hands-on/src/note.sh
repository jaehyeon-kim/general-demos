# PySpark Overview
# https://spark.apache.org/docs/latest/api/python/index.html

export SCRIPT_PATH=/home/hadoop/project/data/src/s08_df_01.py

## spark submit
docker exec spark spark-submit \
  --master local[*] --deploy-mode client $SCRIPT_PATH

## pyspark
docker exec -it spark pyspark --master local[*] --deploy-mode client 

export PYTHONPATH="${SPARK_HOME}/python:${PYTHONPATH}" \
  && export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}" \
  && export PYSPARK_PYTHONPATH_SET=1