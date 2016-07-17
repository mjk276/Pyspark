from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("bleh")
sc = SparkContext(conf=conf)
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
sql = """
select * from  jhaddo.or_ltd_activity_10d_int_final limit 10
"""

def quiet_logs( sc ):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

quiet_logs( sc )

r = sqlContext.sql(sql)
for i in r.collect():
    print r.dtypes
    print(i)



gexprs = ("num_activities_or_10d", "num_activities_or_20d", "num_activities_or_10_to_20d")
z = r.groupBy("indiv_id").pivot("num_activities_or_10d").sum()


df.groupBy('class').pivot('year').avg('hwy').show()


z.show()
for i in z.collect():
    print i
aggexpr = sum("arr_delay")
z = r.groupBy(*gexprs ).sum()




z = r.groupBy(*gexprs ).agg(aggexpr).sum()

r = sqlContext.createDataFrame(sql)
a = r.groupBy("indiv_id").pivot("num_activities_or_10d")
for b in a:
    print a



flights.count()
## 336776

%timeit -n10 r.groupBy(*gexprs ).pivot("hour").agg(aggexpr).count()


#r.saveAsParquetFile("hdfs://nameservice1/data/unmanaged/datascience_ck/vish/mydata")
#sqlContext.sql("CREATE EXTERNAL TABLE vsubr2.some_date_driver1 (date_category String) STORED AS PARQUET LOCATION 'hdfs://nameservice1/data/unmanaged/datascience_ck/vish/mydata'”