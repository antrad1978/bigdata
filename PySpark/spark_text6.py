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

playData = sc.textFile('shakespeare.txt',2)
playDataList = playData.collect()
print(type(playDataList))

print(playDataList[0:4])

lenghts = playData.map(lambda x: len(x))
print(lenghts.collect())

lenghts.saveAsTextFile('savedData1')