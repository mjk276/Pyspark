#from '/Users/vsubr2/spark-1.6.1-bin-hadoop2.6/bin/pyspark'
import collections
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("PopularMoviesv")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///Users/vsubr2/Projects/KaneSpark/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]),1))

print movies.collect()
movieCounts = movies.reduceByKey(lambda x,y:x+y)

flipped = movieCounts.map(lambda (x,y):(y,x))
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print result


L = []
for t in range(T):
    args = raw_input().split(" ")
    if args[0] == "append":
        L.append(int(args[1]))
    elif args[0] == "insert":
        L.insert(int(args[1]), int(args[2]))
    elif args[0] == "remove":
        L.remove(int(args[1]))
    elif args[0] == "pop":
        L.pop()
    elif args[0] == "sort":
        L.sort()
    elif args[0] == "reverse":
        L.reverse()
    elif args[0] == "print":
        print L