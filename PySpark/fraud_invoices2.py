import pandas
from sklearn.naive_bayes import GaussianNB

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

gnb = GaussianNB()
y_pred = gnb.fit(X, y).predict([[123,5,5,5,1000,0],[35,1,1,0,52440,0],[85,0,1,8,10000,0]])
print(y_pred)
#print("Number of mislabeled points out of a total %d points : %d" % (X_test.shape[0], (y_test != y_pred).sum()))