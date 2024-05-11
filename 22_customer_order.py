from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerOrders")
sc = SparkContext(conf=conf)


def split_order(line):
    fields = line.split(",")
    return (int(fields[0]), float(fields[2]))


my_orders = sc.textFile("22_customer-orders.csv")
my_orders_rdd = (my_orders
                 .map(split_order)
                 .reduceByKey(lambda x, y: round(x+y, 2))
                 .sortBy(lambda x: x[1], ascending=False)
                 )

for order in my_orders_rdd.collect():
    print(f"{order[0]}, {order[1]}")
