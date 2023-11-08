#!pip install PySpark

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Censo escolar 2021").master('local[*]').getOrCreate()
sc = spark.sparkContext

# RDD with columns separated and header filtered
rdd = sc.textFile("censo_escolar_2021.csv").map(
    lambda x: x.split(";")).filter(lambda x: x[0] != "NU_ANO_CENSO")

rdd_region = rdd.map(lambda x: (x[1], 1))
rdd_region = rdd_region.reduceByKey(lambda x, y: x + y)
rdd_region = rdd_region.sortByKey()
rdd_region.collect()
