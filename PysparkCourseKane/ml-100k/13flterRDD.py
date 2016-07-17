from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("friendsbyage")
sc = SparkContext(conf=conf)

def parseline(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0/5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("file:///Users/vsubr2/Projects/KaneSpark/1800.csv")
parsedLines = lines.lsmap(parseline)