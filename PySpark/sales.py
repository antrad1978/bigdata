from pyspark.sql import Row
from pyspark import SparkContext
import pyspark
from pyspark.sql import SQLContext
import pandas as pd
from pandas.plotting import scatter_matrix
import matplotlib
from pyspark.sql.functions import year, month, to_timestamp

sc = SparkContext(appName="sales")

sqlContext = SQLContext(sc)
sales = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('sales.csv')

sales.cache()
sales.printSchema()

res = sales.withColumn('years',year(to_timestamp('Order Date', 'MM/dd/yyyy')))

res.show()

res = res.groupBy('years').agg({'Total Revenue':'sum'})
res = res.withColumnRenamed('sum(Total Revenue)','Revenue')
res.show()

sales.describe().toPandas().transpose()

import six
for i in sales.columns:
    if not( isinstance(sales.select(i).take(1)[0][0], six.string_types)):
        print("Correlation to Total Profit for ", i, sales.stat.corr('Total Profit',i))

sales.dtypes

import pandas as pd
from pandas.plotting import scatter_matrix

numeric_features = [t[0] for t in sales.dtypes if t[1] == 'double']
sampled_data = sales.select(numeric_features).sample(False, 0.8).toPandas()

from pyspark.ml.feature import VectorAssembler

vectorAssembler = VectorAssembler(inputCols = ['Units Sold','Unit Price', 'Unit Cost','Total Revenue','Total Cost'], outputCol = 'features')
vsales_df = vectorAssembler.transform(sales)
vsales_df.take(1)

vsales_df = vsales_df.select(['features', 'Total Profit'])
vsales_df.show(3)

splits = vsales_df.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol = 'features', labelCol='Total Profit', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

train_df.describe().show()

lr_predictions = lr_model.transform(test_df)
lr_predictions.select("prediction","Total Profit","features").show(5)

from pyspark.ml.evaluation import RegressionEvaluator
lr_evaluator = RegressionEvaluator(predictionCol="prediction", \
                 labelCol="Total Profit",metricName="r2")
print("R Squared (R2) on test data = %g" % lr_evaluator.evaluate(lr_predictions))

test_result = lr_model.evaluate(test_df)
print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)

print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()

predictions = lr_model.transform(test_df)
predictions.select("prediction","Total Profit","features").show()