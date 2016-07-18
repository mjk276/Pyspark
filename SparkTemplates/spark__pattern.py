from __future__ import print_function
import argparse
import os
from pyspark import SparkContext
from pyspark.sql import HiveContext
import sys

sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
sc = SparkContext(appName = "apollo", master="yarn-client")
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)
sqlContext = HiveContext(sc)

def get_opts():


    parser = argparse.ArgumentParser(description='ETL process for test')
    parser.add_argument('--database', action='store', required=True, help='Supply a valid schema for queries.',
                        dest='database')
    parser.add_argument('--table', action='store', required=True, help='Supply a valid  table name .',
                        dest='table')
    parser.add_argument('--outhdfs', action='store', required=True, help='Supply valid directory for hdfs output path.', dest='outhdfs')
    opts = vars(parser.parse_args(sys.argv[1:]))
    print("--------------------------------------------------------------------------")
    print("PARAMETERS:")
    for o in opts:
        print(" %-17.17s : [%s]" % (o,opts[o]))
    print("--------------------------------------------------------------------------")
    return opts


opts = get_opts()
print("get the  columns")
sql="""
select
  *
from
  {database}.{table} LIMIT 10
""".format(table=opts['table'], database=opts['database'])

df = sqlContext.sql(sql)
df.printSchema()
print(df.count())
print(df.first())
#print(df.collect())

##df_pivot = df.groupBy('indiv_id').pivot('pivot_type_date_key',values=pivotColumns).sum('value').fillna(0).sort('indiv_id')
#print("saving to [%s]" % (opts['outhdfs']))
##df_pivot.repartition(1).write.format('com.databricks.spark.csv').option('header','true').save(path=opts['outhdfs'], mode='overwrite')

