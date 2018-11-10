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

data2001 = ['RIN1', 'RIN2', 'RIN3', 'RIN4', 'RIN5', 'RIN6', 'RIN7']
data2002 = ['RIN3', 'RIN4', 'RIN7', 'RIN8', 'RIN9']
data2003 = ['RIN4', 'RIN8', 'RIN10', 'RIN11', 'RIN12']

parData2001 = sc.parallelize(data2001,2)
parData2002 = sc.parallelize(data2002,2)
parData2003 = sc.parallelize(data2003,2)

unionOf20012002 = parData2001.union(parData2002)
print(unionOf20012002.collect())

allResearchs = unionOf20012002.union(parData2003)
print(allResearchs.collect())

allUniqueResearchs = allResearchs.distinct()
print(allUniqueResearchs.collect())

allResearchs.distinct().count()

print(unionOf20012002.collect())

def func(x):
    print(x)

def contains(x):
    return str(x).startswith("RIN1")

unionOf20012002.foreach(func)

res = unionOf20012002.filter(contains)

print(res.collect())

print(unionOf20012002.collect())

countTotal = parData2001.union(parData2002).union(parData2003).distinct().count()

firstYearCompletion = parData2001.subtract(parData2002)
firstYearCompletion.collect()

unionTwoYears = parData2001.union(parData2002)
unionTwoYears.subtract(parData2003).collect()

projectsInTwoYear = parData2001.intersection(parData2002)
projectsInTwoYear.collect()

#ssc.start()
#ssc.awaitTermination()
