#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json

sc = SparkContext(appName="spark6")
sc.setLogLevel("WARN")

playData = sc.wholeTextFiles('shakespeare.txt',2)

print(playData.keys().collect())

playData.values().collect()