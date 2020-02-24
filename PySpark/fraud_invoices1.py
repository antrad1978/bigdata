import pandas
from sklearn import tree
import pydotplus
from sklearn.tree import DecisionTreeClassifier
import matplotlib.pyplot as plt
import matplotlib.image as pltimg

df = pandas.read_csv("invoices.csv")

print(df)

d = {'GB': 0, 'USA': 1, 'NIGERIA': 2, 'ITALY': 3, 'GERMANY': 4, 'CHINA': 5, 'VENEZUELA': 6, 'UKRAINE': 7, 'PANAMA': 8, 'FRANCE': 9, 'LIBIA': 10, 'SPAIN': 11}
df['Country'] = df['Country'].map(d)
d = {'YES': 1, 'NO': 0}
df['Fraud'] = df['Fraud'].map(d)
df['Registered'] = df['Registered'].map(d)
df['Expired'] = df['Expired'].map(d)
df['InDatabase'] = df['InDatabase'].map(d)
df['Expired'] = df['Expired'].map(d)
df['Approved'] = df['Approved'].map(d)

print(df)

features = ['InvoiceNumber', 'Registered', 'InDatabase','Country','Total','Approved']

X = df[features]
y = df['Fraud']

print(X)
print(y)

dtree = DecisionTreeClassifier()
dtree = dtree.fit(X, y)
data = tree.export_graphviz(dtree, out_file=None, feature_names=features)
graph = pydotplus.graph_from_dot_data(data)
graph.write_png('mydecisiontree.png')

img=pltimg.imread('mydecisiontree.png')
imgplot = plt.imshow(img)
plt.show()

res = dtree.predict([[85,0,1,8,10000,0]])
print(res)

res = dtree.predict_proba([[85,0,1,8,10000,0]])
print(res)

res = dtree.predict([[35,1,1,0,52440,0]])
print(res)

res = dtree.predict_proba([[35,1,1,0,52440,0]])
print(res)


res = dtree.predict([[35,1,1,0,52440,0]])
print(res)

res = dtree.predict_proba([[123,5,5,5,1000,0]])
print(res)