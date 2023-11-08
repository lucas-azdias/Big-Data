#!pip install PySpark

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Censo escolar 2021").master('local[*]').getOrCreate()
sc = spark.sparkContext

# RDD with columns separated and header filtered
rdd = sc.textFile("censo_escolar_2021.csv").map(
    lambda x: x.split(";")).filter(lambda x: x[0] != "NU_ANO_CENSO")

rdd_fed = rdd.map(lambda x: ((x[4], int(x[15])), 1))
rdd_fed = rdd_fed.reduceByKey(lambda x, y: x + y)
rdd_fed = rdd_fed.sortBy(lambda x: x[1], ascending=False)
rdd_fed.collect()
