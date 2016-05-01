from pyspark import SparkContext
import itertools

sc = SparkContext("spark://spark-master:7077", "PopularItems")

data = sc.textFile("/tmp/data/access.log", 2).distinct()    
pairs = data.map(lambda line: line.split("\t"))   
grouped = pairs.groupByKey()
grouped = grouped.mapValues(lambda group: sorted(group))
combos = grouped.flatMapValues(lambda group: itertools.combinations(group, 2)) 
combos = combos.mapValues(lambda combo: (combo[1], combo[0]) if int(combo[0]) > int(combo[1]) else combo)
combos = combos.map(lambda combo: (combo[1], combo[0]))
combos = combos.groupByKey()
combos = combos.map(lambda combo: (combo[0], len(combo[1])))
combos = combos.filter(lambda combo: combo[1] >= 3)

output = combos.collect()                     # bring the data back to the master node so we can print it out
for page_id, count in output:
	print ("Item pair %s \tcount %s" % (page_id, count))
print ("Grouped items done")

sc.stop()