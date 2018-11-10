#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json

sc = SparkContext(appName="spark2")
sc.setLogLevel("WARN")

pythonList  =  ['b' , 'd', 'm', 't', 'e', 'u']
RDD1 = sc.parallelize(pythonList,2)
RDD1.collect()

def vowelCheckFunction( data) :
    if data in ['a','e','i','o','u']:
       return 1
    else :
       return 0

RDD2 = RDD1.map( lambda data : (data, vowelCheckFunction(data)))
print(RDD2.collect())

RDD2Keys = RDD2.keys()
RDD2Keys.collect()