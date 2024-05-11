# PySpark_TamingBigDataCourse

documenting the exercises done in course https://hella.udemy.com/course/taming-big-data-with-apache-spark-hands-on/learn/lecture/3688138#overview

## 14. Key/value RDD

simple mape of pair of data into rdd
rdd row --> 4
ex : new_rdd = rdd.map(lambda x : (x,1))
new_rdd row --> (4,1)
Now 4 is key and 1 is value
you can have any compex data struct in the value field

### Special methods

mapvalues()
flatmapvalues()
reduceByKey()
groupByKey()
sortByKey()
subtractByKey()
keys(),values()

## 16. Filtering on RDD

create smaller rdd by filtering original one
rdd.filter(func)
rdd.filter(lambda x : "TMAX" in x[1])

## 19. FlatMap on RDD

# Section 3 : SparkSQL, DataFrames, and DataSets

## 25. intro to SparkSQL

the most imp component in Spark because it contains Dataframes (new API instead of rdd)
ppl use DF whenever possible instead of rdd
DFs --> Structured DATA - contain row objects - run SQL queries - can have schema (effective storage) - read/write to json,csv,parquet,Hive - comm with Tableau, ODBC/JDBC

Example

```python
from pyspark.sql import SparkSession, Row
spark = SparkSession.builder.appName("SparkSQLEx").getOrCreate()
inp_df = spark.read.json(dataFilePath)
# Use SQL commands
inp_df.createOrReplaceTempView("myStructTable")
res_df = spark.sql("SELECT foo FROM myStructTable ORDER BY foobar")
# Use pandas-like Commands
cols=["col1", "col2"]
inp_df.select(cols)
inp_df.filter(df.col1 > 200)
inp_df.groupBy(df.col1).mean()
inp_df.show(3)  # printing first 3 rows
# convert it back to rdd
inp_df.rdd().map(func_a)
```
