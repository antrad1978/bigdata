# import pyspark class Row from module sql
from pyspark.sql import *
from pyspark import *
from pyspark import SparkContext
import pyspark

sc = SparkContext(appName="csv2")

# Create Example Data - Departments and Employees

# Create the Departments
department1 = Row(id='123456', name='Computer Science')
department2 = Row(id='789012', name='Mechanical Engineering')
department3 = Row(id='345678', name='Theater and Drama')
department4 = Row(id='901234', name='Indoor Recreation')

# Create the Employees
Employee = Row("firstName", "lastName", "email", "salary")
employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)

# Create the DepartmentWithEmployees instances from Departments and Employees
departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
departmentWithEmployees3 = Row(department=department3, employees=[employee1, employee4])
departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])

print(department1)
print(departmentWithEmployees1.employees[0].email)

departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
df1 = sc.createDataFrame(departmentsWithEmployeesSeq1)


departmentsWithEmployeesSeq2 = [departmentWithEmployees3, departmentWithEmployees4]
df2 = sc.createDataFrame(departmentsWithEmployeesSeq2)


unionDF = df1.unionAll(df2)

#dbutils.fs.rm("/tmp/databricks-df-example.parquet", True)
unionDF.write.parquet("databricks-df-example.parquet")

explodeDF = unionDF.selectExpr("e.firstName", "e.lastName", "e.email", "e.salary")

explodeDF.show()

filterDF = explodeDF.filter(explodeDF.firstName == "xiangrui").sort(explodeDF.lastName)

