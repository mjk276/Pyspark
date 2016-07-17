from pyspark import SparkConf, SparkContext

def  loadMovieNames():
    movieNames = {}
    with open("/Users/vsubr2/Projects/KaneSpark/ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf=conf)

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("file:///Users/vsubr2/Projects/KaneSpark/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]),1))
movieCounts = movies.reduceByKey(lambda x, y: x+y)
print movieCounts.collect()

flipped = movieCounts.map(lambda (x,y):(y,x))

sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda (count,movie) : (nameDict.value[movie],count))

results = sortedMoviesWithNames.collect()

for results in results:
   print results


from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("bleh")
sc = SparkContext(conf=conf)
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
sql = """
select
distinct date_category
from
ck_membership.date_driver
LIMIt 10
"""
r = sqlContext.sql(sql)
for i in r.collect():
    print(i)
r.saveAsParquetFile("hdfs://nameservice1/data/unmanaged/datascience_ck/vish/mydata")
sqlContext.sql("CREATE EXTERNAL TABLE vsubr2.some_date_driver1 (date_category String) STORED AS PARQUET LOCATION 'hdfs://nameservice1/data/unmanaged/datascience_ck/vish/mydata'")

like rdd
r2 = sqlContext.inferSchema(r)

r.createExternalTable('vsubr2.some_Date_driver',path='hdfs://nameservice1/data/unmanaged/datascience_ck/vish/')

sqlContext.sql("create table if not exists vsubr2.some_date_driver1")
create table if not exists vsubr2.some_date_driver1
LOCATION 'hdfs://nameservice1/data/unmanaged/datascience_ck/vish/'
as