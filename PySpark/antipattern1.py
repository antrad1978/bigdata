from pyspark import StorageLevel

from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json
from pyspark.sql import SQLContext

sc = SparkContext(appName="spark1")
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)

df2 = sqlContext.createDataFrame(map(lambda x: (x, ), range(1, 51))).withColumnRenamed("_1", "numb")

df2.persist(StorageLevel.DISK_ONLY)
# cache only first partition in DF
df2.first()
# change storage level
df2.persist(StorageLevel.MEMORY_ONLY)
# force full cache of DF
df2.count()