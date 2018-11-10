#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json

sc = SparkContext(appName="spark5")
sc.setLogLevel("WARN")

filDataSingle = [['filamentA','100W',605],
                  ['filamentB','100W',683],
                  ['filamentB','100W',691],
                  ['filamentB','200W',561],
                  ['filamentA','200W',530],
                  ['filamentA','100W',619],
                  ['filamentB','100W',686],
                  ['filamentB','200W',600],
                  ['filamentB','100W',696],
                  ['filamentA','200W',579],
                  ['filamentA','200W',520],
                  ['filamentA','100W',622],
                  ['filamentA','100W',668],
                  ['filamentB','200W',569],
                  ['filamentB','200W',555],
                  ['filamentA','200W',541],
                 ['filamentC', '200W', 541],
                 ['filamentC', '200W', 541]]

filDataSingleRDD = sc.parallelize(filDataSingle,2)
filDataSingleRDD.take(3)

filDataPairedRDD1 = filDataSingleRDD.map(lambda x : (x[0], x[2]))
filDataPairedRDD1.take(4)

keys = filDataSingleRDD.keys()
keys.collect()

filDataPairedRDD11 = filDataPairedRDD1.map(lambda x : (x[0], [x[1], 1]))
filDataPairedRDD11.take(4)

filDataSumandCount = filDataPairedRDD11.reduceByKey(lambda l1, l2: [l1[0] + l2[0], l1[1]+l2[1]])
print(filDataSumandCount.collect())

filDataSumandCount = filDataPairedRDD11.reduceByKey(lambda l1, l2: [l1[0] + l2[0], l1[1]+l2[1]])


filDataComplexKeyData = filDataSingleRDD.map( lambda val : [(val[0],
val[1]),val[2]])

filDataComplexKeyData1 = filDataComplexKeyData.map(lambda val : [val[0]
,[val[1],1]])

filDataComplexKeySumCount = filDataComplexKeyData1.reduceByKey(lambda
l1,l2 : [l1[0]+l2[0], l1[1]+l2[1]])
filDataComplexKeySumCount.collect()

filDataComplexKeyMeanCount = filDataComplexKeySumCount.map(lambda val :
[val[0],[float(val[1][0])/val[1][1],val[1][1]]])
filDataComplexKeyMeanCount.collect()




