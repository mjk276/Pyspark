#from '/Users/vsubr2/spark-1.6.1-bin-hadoop2.6/bin/pyspark'
import collections
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("PopularMoviesv")
sc = SparkContext(conf=conf)

data = sc.textFile("file:///Users/vsubr2/Projects/KaneSpark/ml-100k/u.data")

ratings = data.map(lambda l:l.split()).map(lambda l:Rating(int(l[0]), int(l[l]), float([2]))).cache()

rank = 10
numIterations == 20


