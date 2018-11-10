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

hostName = 'localhost'
tableName = 'pysparkBookTable'
ourInputFormatClass='org.apache.hadoop.hbase.mapreduce.TableInputFormat'
ourKeyClass='org.apache.hadoop.hbase.io.ImmutableBytesWritable'
ourValueClass='org.apache.hadoop.hbase.client.Result'
ourKeyConverter='org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter'
ourValueConverter='org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter'

configuration = {}
configuration['hbase.mapreduce.inputtable'] = tableName
configuration['hbase.zookeeper.quorum'] = hostName

tableRDDfromHBase = sc.newAPIHadoopRDD(
                       inputFormatClass = ourInputFormatClass,
                       keyClass = ourKeyClass,
                       valueClass = ourValueClass,
                       keyConverter = ourKeyConverter,
                       valueConverter = ourValueConverter,
                       conf = configuration
)

tableRDDfromHBase.take(2)