# Spark SUBMIT command
# spark-submit --master yarn --deploy-mode cluster --num-executors 20 --executor-mem 20G \
#  --executor-memory 15G --executor-cores 4 --driver-memory 15G ----packages com.databricks:spark-avro_2.10:2.0.1 test_spark.py 


from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext

spark_context = SparkContext(conf=SparkConf())
sql_context = SQLContext(spark_context)

df = sql_context.read.parquet('s3://inbound-nike-stage-dev/dev/RPA/WHOLESALE/SoCore_US/write_test/')

df = df.repartition(1000,'ccd_dt')

# Parquet write
# Write to HDFS
df.write.mode('overwrite').parquet('/user/hadoop/so_core/parquet')
# Write to S3
df.write.mode('overwrite').parquet('s3://inbound-nike-stage-dev/dev/RPA/WHOLESALE/SoCore_US/write_test/spark_write/')

# Avro write
# Write to HDFS
df.write.mode('overwrite').format("com.databricks.spark.avro").save("/user/hadoop/so_core/avro")
# S3 write
df.write.mode('overwrite').format("com.databricks.spark.avro").save('s3://inbound-nike-stage-dev/dev/RPA/WHOLESALE/SoCore_US/write_test/spark_write/avro/')
