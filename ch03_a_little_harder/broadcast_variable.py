"""
Broadcast objects to the executors, such that they're always there whenever
needed.
Just use sc.broadcast() to ship off whatever you want
Then use .value() to get the object back.
"""

import os

from pyspark import SparkConf, SparkContext


def load_movie_name():
    movie_names = {}
    with open(reference_path, encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split("|")
            movie_names[fields[0]] = fields[1]
    return movie_names


datafile_name = "u.data"
reference_name = "u.item"
datafile_path = os.path.join(
    os.path.expanduser("~"), "Code", "spark_learn", "datasets",
    "ml-100k", datafile_name,
)
reference_path = os.path.join(
    os.path.expanduser("~"), "Code", "spark_learn", "datasets",
    "ml-100k", reference_name,
)

conf = SparkConf().setMaster("local").setAppName("BestMovie")
sc = SparkContext(conf=conf)

name_dict = sc.broadcast(load_movie_name())

results = sc.textFile(datafile_path)\
            .map(lambda x: (x.split()[1], 1))\
            .reduceByKey(lambda x, y: (x + y))\
            .map(lambda x: (x[1], x[0]))\
            .sortByKey()\
            .map(lambda x: (name_dict.value[x[1]], x[0]))\
            .collect()

for result in results[-10:]:
    print(result)
