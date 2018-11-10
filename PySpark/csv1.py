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

import csv
import io

def parseCSV(csvRow) :
    data = io.StringIO(csvRow)
    dataReader =  csv.reader(data)
    return(dataReader.next())

csvRow = "p,s,r,p"
parseCSV(csvRow)

filamentRDD =sc.textFile('sample.csv',4)
filamentRDDCSV = filamentRDD.map(parseCSV)
filamentRDDCSV.take(4)

import csv
import io
def createCSV(dataList):
    data = io.StringIO.StringIO()
    dataWriter = csv.writer(data,lineterminator='')
    dataWriter.writerow(dataList)
    return (data.getvalue())

listData = ['p','q','r','s']
createCSV(listData)

simpleData = [['p',20],
               ['q',30],
               ['r',20],
               ['m',25]]
simpleRDD = sc.parallelize(simpleData,4)
simpleRDD.take(4)

simpleRDDLines = simpleRDD.map( createCSV)
simpleRDDLines.take(4)
simpleRDDLines.saveAsTextFile('csvData/')
