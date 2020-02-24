from pyspark import SparkContext
import matplotlib.pyplot as plt
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import year, to_timestamp, DataFrame
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import PolynomialFeatures
from ngt_utils import get_sum_grouped_by_date

sc = SparkContext(appName="sales")

sqlContext = SQLContext(sc)
sales = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('sales.csv')

res = get_sum_grouped_by_date(sales, 'years', 'Revenue', 'Order Date', 'MM/dd/yyyy', 'Total Revenue')

revenues = res.select("Revenue").rdd.map(lambda x: x[0]).collect()
years = res.select("years").rdd.map(lambda x: x[0]).collect()

y_train = revenues
start = min(years)
end = max(years)
x_plot = np.linspace(start, end, (end-start)+1)
plt.scatter(x_plot, y_train, color='navy', s=30, marker='o', label="training points")
colors = ['teal', 'yellowgreen', 'gold', 'red','green','violet','grey']
x_plot = x_plot.reshape(-1,1)
plt.plot(years, revenues)
for count, degree in enumerate([2, 3]):
  model = make_pipeline(PolynomialFeatures(degree), Ridge())
  model.fit(x_plot, y_train)
  y_plot = model.predict(x_plot)
  plt.plot(x_plot, y_plot, color=colors[count], linewidth=2,
           label="degree %d" % degree)
plt.legend(loc='upper right')
plt.show()

numeric_features = [t[0] for t in sales.dtypes if t[1] == 'double']
sampled_data = sales.select(numeric_features).sample(False, 0.8).toPandas()

from pyspark.ml.feature import VectorAssembler

vectorAssembler = VectorAssembler(inputCols = ['Units Sold','Unit Price', 'Unit Cost','Total Revenue','Total Cost'], outputCol = 'features')
vsales_df = vectorAssembler.transform(sales)

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