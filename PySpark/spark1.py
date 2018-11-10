#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json

sc = SparkContext(appName="spark1")
sc.setLogLevel("WARN")

#ssc = StreamingContext(sc, 10)

my_list = [i for i in range(1000)]

data = list(filter(lambda x: x % 5 == 0, my_list))

values = sc.parallelize(data, 2)

val = values.first()
print(values.collect())
print(values.getNumPartitions())

#ssc.start()
#ssc.awaitTermination()
