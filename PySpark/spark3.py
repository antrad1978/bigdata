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

airVelocityKMPH = [12,13,15,12,11,12,11]
parVelocityKMPH = sc.parallelize(airVelocityKMPH,2)

countValue =  parVelocityKMPH.count()

sumValue = parVelocityKMPH.sum()

meanValue = parVelocityKMPH.mean()

varianceValue = parVelocityKMPH.variance()

sampleVarianceValue =  parVelocityKMPH.sampleVariance()

stdevValue = parVelocityKMPH.stdev()

sampleStdevValue = parVelocityKMPH.sampleStdev()

parVelocityKMPH.stats().asDict()

parVelocityKMPH.stats().mean()












