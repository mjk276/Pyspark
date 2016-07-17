from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Spend by customer")
sc = SparkContext(conf=conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("file:///Users/vsubr2/Projects/KaneSpark/customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
r = mappedInput.collect()
print r
totalByCustomer = mappedInput.reduceByKey(lambda x,y:x+y)
flipped = totalByCustomer.map(lambda (x,y):(y,x))
totalByCustomerSorted = flipped.sortByKey()

results = totalByCustomerSorted.collect()
for result in results:
    print result
