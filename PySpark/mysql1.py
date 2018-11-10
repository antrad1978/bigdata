from pyspark import StorageLevel
from pyspark import SparkContext
import json
from pyspark.sql import SQLContext

sc = SparkContext(appName="mysql1")
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)

url = 'jdbc:mysql://localhost/essedieffe'
user = "root"
password = "root"

df = sqlContext.read.format('jdbc').options(
          url=url,
          driver='com.mysql.jdbc.Driver',
          dbtable='computers',
          user=user,
          password=password).load()

# Looks the schema of this DataFrame.
df.printSchema()

# Counts people by age
countsBySocieta = df.groupBy("id_societa").count()
countsBySocieta.show()

# Saves countsByAge to S3 in the JSON format.
#countsByAge.write.format("json").save("s3a://...")

countsBySocieta.write.format('jdbc').options(
          url=url,
          driver='com.mysql.jdbc.Driver',
          dbtable='societa_computers',
          user=user,
          password=password).mode('append').save()

jdbcUrl = "jdbc:mysql://{0}:{1}/{2}".format("localhost", "3306", "essedieffe")
connectionProperties = {
  "user" : user,
  "password" : password,
  "driver" : "com.mysql.jdbc.Driver"
}

pushdown_query = "(select * from allegati) emp_alias"
df = sqlContext.read.jdbc(url=url, table="allegati", properties=connectionProperties)

df.printSchema()
