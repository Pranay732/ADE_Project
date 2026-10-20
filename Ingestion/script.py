import os
import sys


print("Python:", sys.executable)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstSparkJob") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "Joy"),
    (2, "John"),
    (3, "David")
]

df = spark.createDataFrame(data, ["id", "name"])

df.show()

print("Spark version:", spark.version)

spark.stop()