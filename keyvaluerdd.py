from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("friendsbyage")
sc = SparkContext(conf=conf)

def parseline(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


lines = sc.textFile("file:///Users/vsubr2/Projects/KaneSpark/fakefriends.csv")
rdd = lines.map(parseline)
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0] + y[0], x[1]+y[1]))
averagesByAge = totalsByAge.mapValues(lambda x:x[0]/x[1])
results = averagesByAge.collect()
for result in results:
    print result