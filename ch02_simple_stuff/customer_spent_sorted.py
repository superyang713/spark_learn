import os
from pyspark import SparkConf, SparkContext


def parse_data(row):
    row = row.split(",")
    return (int(row[0]), float(row[2]))


filename = "customer-orders.csv"
filepath = os.path.join(
    os.path.expanduser("~"), "Code", "spark_learn", "datasets", filename
)
conf = SparkConf().setMaster("local").setAppName("CustomerSpentSort")
sc = SparkContext(conf=conf)

data = sc.textFile(filepath)
data = data.map(parse_data)

total_per_customer = data.reduceByKey(lambda x, y: (x + y))\
                     .map(lambda x: (x[1], x[0]))\
                     .sortByKey()

results = total_per_customer.collect()
sc.stop()

for result in results:
    print("{}\t\t{:.2f}".format(result[1], result[0]))
