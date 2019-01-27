import os
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf=conf)

datafile_name = "marvel-graph.txt"
reference_name = "marvel-names.txt"
datafile_path = os.path.join(
    os.path.expanduser("~"),
    "Code",
    "spark_learn",
    "datasets",
    datafile_name,
)
reference_path = os.path.join(
    os.path.expanduser("~"),
    "Code",
    "spark_learn",
    "datasets",
    reference_name,
)


def get_counts(line):
    records = line.split()
    return (records[0], len(records) - 1)


def parse_name(line):
    records = line.split('\"')
    return (records[0].strip(), records[1].strip())


reference_rdd = sc.textFile(reference_path)\
                  .map(parse_name)

results = sc.textFile(datafile_path)\
            .map(get_counts)\
            .reduceByKey(lambda x, y: x + y)\
            .map(lambda x: (x[1], x[0]))\
            .max()

most_popular_name = reference_rdd.lookup(results[1])[0]

print("{} is the most popular superhero, with {} co-appearance.".format(
    most_popular_name, results[0]))
