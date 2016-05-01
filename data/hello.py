from pyspark import SparkContext
import itertools

sc = SparkContext("spark://spark-master:7077", "PopularItems")

data = sc.textFile("/tmp/data/access.log", 2).distinct()     # each worker loads a piece of the data file
pairs = data.map(lambda line: line.split("\t"))   # tell each worker to split each line of it's partition
grouped = pairs.groupByKey()
combos = grouped.flatMapValues(lambda group: itertools.combinations(group, 2)) 
combos = combos.map(lambda combo: (combo[1], combo[0])) 
combos = combos.groupByKey()
combos = combos.map(lambda combo: (combo[0], len(combo[1])))
combos = combos.filter(lambda combo: combo[1] >= 3)

# user_pairs = distinct_pairs.join(distinct_pairs)
# user_pairs = grouped.map(lambda group: (pair[0], (list of pairs)))
# count = pages.reduceByKey(lambda x,y: x+y)        # shuffle the data so that each key is only on one worker
                                                  # and then reduce all the values by adding them together
output = combos.collect()                     # bring the data back to the master node so we can print it out
for page_id, count in output:
	# print ("tuple: %s" % tup)
	# count = list(count)
	print ("page_id %s count %s" % (page_id, count))
print ("Grouped items done")

sc.stop()