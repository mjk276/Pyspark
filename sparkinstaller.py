import os
import sys

# Path for spark source folder
os.environ['SPARK_HOME']="/Users/vsubr2/spark-1.6.1-bin-hadoop2.6/"

# Append pyspark  to Python Path
sys.path.append("/Users/vsubr2/spark-1.6.1-bin-hadoop2.6/bin/")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)