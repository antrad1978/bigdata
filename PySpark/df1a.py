#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json
from pyspark.sql.types import StringType

sc = SparkContext(appName="spark1")
sc.setLogLevel("WARN")

df = sc.createDataFrame(["10","11","13"], "string").toDF("age")



df2 = sc.createDataFrame(["10", "11", "13"], StringType()).toDF("age")