import os
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf=conf)

start_character_id = 5306  # Spiderman
target_character_id = 14  # ADAM

hit_counter = sc.accumulator(0)
reference_name = "marvel-names.txt"
reference_path = os.path.join(
    os.path.expanduser("~"),
    "Code",
    "spark_learn",
    "datasets",
    reference_name,
)


def convert_to_BFS(line):
    fields = line.split()
    hero_id = int(fields[0])
    connections = [int(connection) for connection in fields[1:]]

    color = "WHITE"
    distance = 9999

    if hero_id == start_character_id:
        color = "GRAY"
        distance = 0

    return (hero_id, (connections, distance, color))


def create_starting_rdd():
    datafile_name = "marvel-graph.txt"

    datafile_path = os.path.join(
        os.path.expanduser("~"),
        "Code",
        "spark_learn",
        "datasets",
        datafile_name,
    )
    data = sc.textFile(datafile_path)
    return data.map(convert_to_BFS)


def map_BSF(line):
    character_id, data = line
    connections, distance, color = data
    results = []
    if color == "GRAY":
        for connection in connections:
            new_character_id = connection
            new_distance = distance + 1
            new_color = "GRAY"

            if target_character_id == connection:
                hit_counter.add(1)

            new_line = (new_character_id, ([], new_distance, new_color))
            results.append(new_line)

        color = "BLACK"
    results.append((character_id, (connections, distance, color)))
    return results


def reduce_BSF(data1, data2):
    edges1, distance1, color1 = data1
    edges2, distance2, color2 = data2

    distance = 9999
    color = "WHITE"
    edges = []

    if len(edges1) > 0:
        edges.extend(edges1)

    if len(edges2) > 0:
        edges.extend(edges2)

    if distance1 < distance:
        distance = distance1

    if distance2 < distance:
        distance = distance2

    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)


iteration_rdd = create_starting_rdd()
for i in range(10):
    print("Running BFS iteration# {}".format(i + 1))
    mapped = iteration_rdd.flatMap(map_BSF)
    print("Processing {} values.".format(mapped.count()))

    if hit_counter.value > 0:
        print("Hit the target character!" +\
              "From {} different direction(s)".format(hit_counter.value))
        break

    iteration_rdd = mapped.reduceByKey(reduce_BSF)
