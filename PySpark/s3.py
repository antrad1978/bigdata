#https://s3.amazonaws.com/ngtemr/SalesJan2009.csv
#secret:zVUCH+nUNG4ztJcfq9sijstNgWyxfLpzW5rWLnvh
#key:AKIAIQG34AFQ7PN6P2SQ


import pyspark
conf = pyspark.SparkConf()

sc = pyspark.SparkContext('local[4]', conf=conf)


#sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAIQG34AFQ7PN6P2SQ")
#sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")

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

