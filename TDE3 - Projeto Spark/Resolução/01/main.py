#!pip install PySpark

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
    "Censo escolar 2021").master('local[*]').getOrCreate()
sc = spark.sparkContext

# RDD with columns separated and header filtered
rdd = sc.textFile("censo_escolar_2021.csv").map(
    lambda x: x.split(";")).filter(lambda x: x[0] != "NU_ANO_CENSO")

rdd_cwb = rdd.filter(lambda x: x[4].upper() == "PR" and x[6].upper() == "CURITIBA")
amount_rdd__cwb = rdd_cwb.count()
print(amount_rdd__cwb)
