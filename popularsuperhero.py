#from '/Users/vsubr2/spark-1.6.1-bin-hadoop2.6/bin/pyspark'
import collections
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("PopularHeroes")
sc = SparkContext(conf=conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("file:///Users/vsubr2/Projects/KaneSpark/Marvel-Names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("file:///Users/vsubr2/Projects/KaneSpark/Marvel-Graph.txt")

pairings = lines.map(countCoOccurences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda  (x,y):(y,x))
mostPopular = flipped.max()

mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print mostPopularName + " is the most popular superhero with " + str(mostPopular[0]) + " appearances"
