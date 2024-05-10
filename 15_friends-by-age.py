from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parse_Line(line):
    '''
    split 1 string line to fields based on comma separator
    '''
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


lines = sc.textFile(
    "D:/_1_Technicals/GitHubs/Repos/PySpark_TamingBigDataCourse/15_fakefriends.csv")
my_rdd = (
    lines
    .map(parse_Line)
    .mapValues(lambda x: (x, 1))
    .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    .mapValues(lambda x: round(x[0]/x[1], 1))
    .collect()
)
for result in my_rdd:
    print(result)
