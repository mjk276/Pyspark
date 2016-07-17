#from '/Users/vsubr2/spark-1.6.1-bin-hadoop2.6/bin/pyspark'
import collections
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("PopularHeroes")
sc = SparkContext(conf=conf)

startCharacterID = 5306
targetCharacterID = 14

hitCounter = sc.accumulator(0)

def convertTOBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connections))

    color = 'WHITE'
    distance = 9999

    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0

    return (heroID, (connections, distance, color))

def createStartingRDD():
    inputFile = sc.textFile("file:///Users/vsubr2/Projects/KaneSpark/Marvel-Graph.txt")
    return inputFile.map(convertTOBFS)

def bfsMap(node):
    characterID = node[0]






iterationRDD = createStartingRDD()

for iteration in range(0, 10):
    print "Running BFS iteration# " + str(iteration+1)

    mapped = iterationRDD.flatMap(bfsMap)

    print "Processing" + str(mapped.count()) +  " values"