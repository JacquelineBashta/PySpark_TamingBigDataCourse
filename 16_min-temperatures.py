from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)


lines = sc.textFile("16_1800.csv").map(parseLine)
my_rdd = (
    lines
    .filter(lambda x: "TMIN" in x[1])
    .map(lambda x: (x[0], x[2]))
    .reduceByKey(lambda x, y: min(x, y))
)

my_max_rdd = (
    lines
    .filter(lambda x: "TMAX" in x[1])
    .map(lambda x: (x[0], x[2]))
    .reduceByKey(lambda x, y: max(x, y))
)

for result in my_rdd.collect():
    print(f"{result[0]} Station reported minimum of {round(result[1],2)}C")

for result in my_max_rdd.collect():
    print(f"{result[0]} Station reported maximum of {round(result[1],2)}C")
