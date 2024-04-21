from pyspark import SparkConf, SparkContext
import collections
# Ideally Here is the right point to config executer number and resources
# But for the first example we will keep it simple , it will run on one process
# App name used to identify the job on Spark Web UI to see what happeing while job is running
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

# textFile --> split every line to 1 row in a rdd  (lines is rdd)
lines = sc.textFile(
    "D:/_1_Technicals/GitHubs/Repos/PySpark_TamingBigDataCourse/01_ml-100k/u.data")

# Rating is a new rdd
ratings = lines.map(lambda x: x.split()[2])

# do action on ratings , will create a dict "results"
results = ratings.countByValue()

# no more spark thingy as the vars are all not rdd anymore.
sortedResults = collections.OrderedDict(sorted(results.items()))
for key, value in sortedResults.items():
    print(f"{key} {value}")
