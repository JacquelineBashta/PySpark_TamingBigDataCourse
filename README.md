# PySpark_TamingBigDataCourse
documenting the exercises done in course https://hella.udemy.com/course/taming-big-data-with-apache-spark-hands-on/learn/lecture/3688138#overview


## 14. Key/value RDD
simple mape of pair of data into rdd
rdd row --> 4
ex : new_rdd = rdd.map(lambda x : (x,1))
new_rdd row --> (4,1)
Now 4 is key and 1 is value
you can have any compex data struct in the value field

###  Special methods 
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