#!pip install PySpark

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Censo escolar 2021").master('local[*]').getOrCreate()
sc = spark.sparkContext

# RDD with columns separated and header filtered
rdd = sc.textFile("censo_escolar_2021.csv").map(
    lambda x: x.split(";")).filter(lambda x: x[0] != "NU_ANO_CENSO")

rdd_mat = rdd.filter(lambda x: x[305] != "")
rdd_mat = rdd_mat.map(lambda x: (x[1], (int(x[305]), 1)))
rdd_mat = rdd_mat.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
rdd_mat = rdd_mat.mapValues(lambda x: x[0] / x[1])
rdd_mat = rdd_mat.sortByKey()
rdd_mat.collect()
