#    Spark
from pyspark import SparkContext
#    Spark Streaming
from pyspark.streaming import StreamingContext
#    Kafka
from pyspark.streaming.kafka import KafkaUtils
#    json parsing
import json

sc = SparkContext(appName="json")
sc.setLogLevel("WARN")

import json
def jsonParse(dataLine):
    parsedDict = json.loads(dataLine)
    valueData = parsedDict.values()
    return(valueData)

jsonData = '{"Time":"6AM", "Temperature":15}'
jsonParsedData = jsonParse(jsonData)

tempData = sc.textFile("sample.json",4)
tempData.take(4)

def createJSON(data):
    dataDict = {}
    dataDict['Name'] = data[0]
    dataDict['Age'] = data[1]
    return(json.dumps(dataDict))
