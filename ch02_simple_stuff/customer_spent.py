from pyspark import SparkConf, SparkContext


def parse_data(row):
    row = row.split(",")
    customer_id = int(row[0])
    amount = float(row[2])
    return (customer_id, amount)


conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf=conf)

data = sc.textFile("customer-orders.csv")
data = data.map(parse_data)
results = data.reduceByKey(lambda x, y: (x + y)).sortByKey()
results = results.collect()

for result in results:
    print("{}\t\t{:.2f}".format(result[0], result[1]))
