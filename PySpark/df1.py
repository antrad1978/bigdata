from pyspark.sql import SQLContext

df = SQLContext.read.format('csv').options(header='true').load("sample.csv")
df.count()
df.dtypes