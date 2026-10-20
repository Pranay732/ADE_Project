from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# FUNCTION TO READ SQL FILE
def read_sql_file(sql_path):
    with open(sql_path, "r") as file:
        return file.read()


# CREATE SPARK SESSION
spark = (
    SparkSession.builder
    .appName("Healthcare_Patients_RAW_to_Translate")
    .master("local[*]")
    .getOrCreate()
)


# PROJECT PATH
project_path = r"E:\ADE_Project"


# RAW CSV PATH
raw_path = (
    project_path
    + r"\data\raw\healthcare\patients\patients_20261006_211805.csv"
)


# READ RAW DATA
raw_df = (
    spark.read
    .option("header", "true")
    .csv(raw_path)
)

print("\n========== RAW DATA ==========")
raw_df.show(5, truncate=False)

print("\n========== RAW DATA STRUCTURE ==========")
raw_df.printSchema()


# CREATE TEMPORARY VIEW
raw_df.createOrReplaceTempView("patients_raw")

print("\n========== PATIENTS RAW VIEW CREATED ==========")


# SQL FILE PATH
sql_path = project_path + r"\sql\healthcare\patients.sql"


# READ SQL FILE
patients_sql = read_sql_file(sql_path)


# EXECUTE SQL
patients_df = spark.sql(patients_sql)


print("\n========== TYPE CASTED DATA ==========")
patients_df.show(5, truncate=False)

print("\n========== TYPE CASTED DATA STRUCTURE ==========")
patients_df.printSchema()


spark.stop()