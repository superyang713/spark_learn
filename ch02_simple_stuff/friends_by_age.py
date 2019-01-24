import os
from pyspark import SparkConf, SparkContext

filename = "fakefriends.csv"
filepath = os.path.join(
    os.path.expanduser("~"), "Code", "spark_learn", "datasets", filename
)

conf = SparkConf().setMaster('local').setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(",")
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


lines = sc.textFile(filepath)
rdd = lines.map(parse_line)

total_by_age = rdd.mapValues(lambda x: (x, 1))\
                  .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

average_by_age = total_by_age.mapValues(lambda x: x[0] / x[1])

results = average_by_age.collect()
results = sorted(results, key=lambda x: x[0])
for result in results:
    print(result)
