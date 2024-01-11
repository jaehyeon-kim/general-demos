from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
<<<<<<< HEAD
    docker exec spark /usr/lib/spark/bin/spark-submit \
        --master local[*] --deploy-mode client /home/hadoop/project/apps/spark/consumer.py
=======
    ## spark submit
    docker exec spark spark-submit \
        --master local[*] \
        --deploy-mode client \
        /home/hadoop/project/apps/spark/consumer.py
    
    ## pyspark
    docker exec -it spark pyspark \
        --master local[*] \
        --deploy-mode client 
>>>>>>> master
    """
    spark = SparkSession.builder.appName("Consume Orders").getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")
    spark.sql("MSCK REPAIR TABLE demo.sink_tbl")
    spark.sql("SELECT * FROM demo.sink_tbl").show()
