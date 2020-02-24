import matplotlib.pyplot as plt
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SQLContext
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


