#from '/Users/vsubr2/spark-1.6.1-bin-hadoop2.6/bin/pyspark'
import collections
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("Rating Historgram")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///Users/vsubr2/Projects/KaneSpark/ml-100k/u.data")
ratings = lines.map(lambda x:x.split()[2])
result = ratings.countByValue()


sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.iteritems():
    print "%s %i" % ( key, value)

def ex():
    print "This si a test"

