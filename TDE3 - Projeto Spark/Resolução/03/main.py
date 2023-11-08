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

rdd_prof = rdd.map(lambda x: ((x[14], x[6]), (x[338], x[342], x[345])))
rdd_prof = rdd_prof.mapValues(lambda x: int(empty_to_zero(x[0])) + int(empty_to_zero(x[1])) + int(empty_to_zero(x[2])))
higher_rdd_prof = rdd_prof.reduce(lambda x, y: x if x[1] > y[1] else y)
print(higher_rdd_prof)
