#!pip install PySpark

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Censo escolar 2021").master('local[*]').getOrCreate()
sc = spark.sparkContext

# RDD with columns separated and header filtered
rdd = sc.textFile("censo_escolar_2021.csv").map(
    lambda x: x.split(";")).filter(lambda x: x[0] != "NU_ANO_CENSO")

empty_to_zero = lambda x: x if x != "" else 0

rdd_mat = rdd.map(lambda x: (x[1], (int(empty_to_zero(x[311])), 1)))
rdd_mat = rdd_mat.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
rdd_mat = rdd_mat.mapValues(lambda x: x[0] / x[1])
rdd_mat = rdd_mat.sortByKey()
rdd_mat.collect()
