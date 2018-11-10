import pyspark
conf = pyspark.SparkConf()

sc = pyspark.SparkContext('local[4]', conf=conf)

bucket = "ngtemr"
prefix = "SalesJan2009.csv"
filename = "s3a://{}/{}".format(bucket, prefix)

output = "s3a://{}/{}".format(bucket, "res.csv")

myRdd = sc.textFile(filename)
myRdd.count()

sql = pyspark.SQLContext(sc)
df = sql.read.csv(filename)
df.collect()
print(df.count)
df.stat
df.collect()

df.write.csv(output)

