from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("json1").getOrCreate()
df = spark.read.option("multiline", "true").json("sample3.json")

df.show()