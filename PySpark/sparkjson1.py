from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("json1").getOrCreate()
df = spark.read.json("sample.json")
# Displays the content of the DataFrame to stdout
df.show()

df.printSchema()

# Select only the "name" column
df.select("Temperature").show()

# Select everybody, but increment the age by 1
df.select(df['Time'], df['Temperature'] + 1).show()

# Select people older than 21
df.filter(df['Temperature'] > 21).show()

# Count people by age
df.groupBy("Temperature").count().show()

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("temps")

sqlDF = spark.sql("SELECT * FROM temps")
sqlDF.show()
