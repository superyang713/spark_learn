from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster('local').setAppName("MinTemp")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1 * 9.0 / 5.0 + 32.0
    return (station_id, entry_type, temperature)


lines = sc.textFile("1800.csv")
parsed_lines = lines.map(parse_line)

min_temp = parsed_lines.filter(lambda x: "TMAX" in x[1])
station_temp = min_temp.map(lambda x: (x[0], x[2]))

# Find the minimum temperature by station_id
min_temp = station_temp.reduceByKey(lambda x, y: max(x, y))
results = min_temp.collect()

for result in results:
    print("{}\t{:.2f}F".format(result[0], result[1]))
