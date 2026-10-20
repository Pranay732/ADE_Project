import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# 1. HADOOP CONFIGURATION
# ============================================================

os.environ["HADOOP_HOME"] = r"E:\Hadoop"

hadoop_bin = r"E:\Hadoop\bin"

if hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = (
        hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    )


# ============================================================
# 2. PROJECT PATHS
# ============================================================

PROJECT_DIR = Path(r"E:\ADE_Project")

RAW_DIR = (
    PROJECT_DIR / "data" / "raw" / "healthcare" / "patients"
)

SQL_FILE = (
    PROJECT_DIR / "sql" / "healthcare" / "patients.sql"
)

TRANSLATE_DIR = (
    PROJECT_DIR / "data" / "translate" / "healthcare" / "patients"
)

REJECTED_DIR = (
    PROJECT_DIR / "data" / "rejected" / "healthcare" / "patients"
)


# ============================================================
# 3. FIND RAW FILES
# ============================================================

csv_files = sorted(RAW_DIR.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError(f"No RAW CSV files found: {RAW_DIR}")

if not SQL_FILE.exists():
    raise FileNotFoundError(f"SQL file not found: {SQL_FILE}")


# ============================================================
# 4. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Healthcare_Patients_RAW_to_Translate")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 5. READ RAW DATA AND CAST DATATYPES
# ============================================================

print("=" * 60)
print("PATIENTS RAW TO TRANSLATE")
print("=" * 60)

print(f"RAW CSV files found: {len(csv_files)}")

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv([str(file) for file in csv_files])
)

raw_count = raw_df.count()

print(f"RAW records: {raw_count}")

raw_df.createOrReplaceTempView("patients_raw")

patients_sql = SQL_FILE.read_text(encoding="utf-8")
patients_df = spark.sql(patients_sql)

if patients_df.count() != raw_count:
    raise ValueError("Record count changed during datatype casting.")


# ============================================================
# 6. STANDARDIZE VALUES
# ============================================================

print("\n========== STANDARDIZATION ==========")

patients_df = (
    patients_df
    .withColumn("first_name", F.trim(F.col("first_name")))
    .withColumn("last_name", F.trim(F.col("last_name")))
    .withColumn("gender", F.upper(F.trim(F.col("gender"))))
    .withColumn("phone", F.trim(F.col("phone")))
    .withColumn("address", F.trim(F.col("address")))
    .withColumn("city", F.trim(F.col("city")))
    .withColumn("state", F.trim(F.col("state")))
)

# Convert blank strings to NULL in optional text columns.
for column_name in ["gender", "email", "address", "city", "state"]:
    patients_df = patients_df.withColumn(
        column_name,
        F.when(
            F.trim(F.col(column_name)) == "",
            F.lit(None).cast("string")
        ).otherwise(F.col(column_name))
    )

# Standardize email: lowercase and trim.
patients_df = patients_df.withColumn(
    "email",
    F.lower(F.trim(F.col("email")))
)

# Invalid or missing email becomes NULL.
valid_email_condition = F.col("email").rlike(
    r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
)

patients_df = patients_df.withColumn(
    "email",
    F.when(valid_email_condition, F.col("email"))
    .otherwise(F.lit(None).cast("string"))
)

print("Text standardization completed.")


# ============================================================
# 7. IDENTIFY INVALID RECORDS
# ============================================================

print("\n========== DATA QUALITY VALIDATION ==========")

rejection_conditions = [
    (
        F.col("patient_id").isNull(),
        "MISSING_PATIENT_ID"
    ),
    (
        F.col("first_name").isNull()
        | (F.trim(F.col("first_name")) == ""),
        "MISSING_FIRST_NAME"
    ),
    (
        F.col("last_name").isNull()
        | (F.trim(F.col("last_name")) == ""),
        "MISSING_LAST_NAME"
    ),
    (
        F.col("date_of_birth") > F.current_date(),
        "FUTURE_DATE_OF_BIRTH"
    ),
    (
        F.col("gender").isNotNull()
        & (~F.col("gender").isin("M", "F")),
        "INVALID_GENDER"
    ),
    (
        F.col("phone").isNull()
        | (~F.col("phone").rlike(r"^[0-9]{10}$")),
        "MISSING_OR_INVALID_PHONE"
    ),
    (
        F.col("registration_date").isNull(),
        "MISSING_REGISTRATION_DATE"
    ),
    (
        F.col("registration_date") > F.current_date(),
        "FUTURE_REGISTRATION_DATE"
    ),
    (
        F.col("updated_at").isNull(),
        "MISSING_UPDATED_AT"
    )
]

rejection_reason = F.concat_ws(
    ", ",
    *[
        F.when(condition, F.lit(reason))
        for condition, reason in rejection_conditions
    ]
)

patients_df = patients_df.withColumn(
    "rejection_reason",
    rejection_reason
)

rejected_df = patients_df.filter(
    F.col("rejection_reason") != ""
)

valid_df = patients_df.filter(
    F.col("rejection_reason") == ""
).drop("rejection_reason")

print(f"Records before validation: {patients_df.count()}")
print(f"Rejected before deduplication: {rejected_df.count()}")


# ============================================================
# 8. REMOVE DUPLICATE PATIENT IDs FROM VALID RECORDS
# ============================================================

print("\n========== DEDUPLICATION ==========")

window_spec = Window.partitionBy("patient_id").orderBy(
    F.col("updated_at").desc()
)

valid_df = (
    valid_df
    .withColumn("row_number", F.row_number().over(window_spec))
)

duplicate_count = valid_df.filter(
    F.col("row_number") > 1
).count()

valid_df = (
    valid_df
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

print(f"Duplicate valid records removed: {duplicate_count}")


# ============================================================
# 9. WRITE TRANSLATE AND REJECTED PARQUET
# ============================================================

print("\n========== WRITE PARQUET ==========")

TRANSLATE_DIR.mkdir(parents=True, exist_ok=True)
REJECTED_DIR.mkdir(parents=True, exist_ok=True)

valid_count = valid_df.count()
rejected_count = rejected_df.count()

valid_df.write.mode("overwrite").parquet(str(TRANSLATE_DIR))

rejected_df.write.mode("overwrite").parquet(str(REJECTED_DIR))

print(f"TRANSLATE records: {valid_count}")
print(f"REJECTED records: {rejected_count}")

print(f"TRANSLATE path: {TRANSLATE_DIR}")
print(f"REJECTED path: {REJECTED_DIR}")


# ============================================================
# 10. FINAL SUMMARY
# ============================================================

print("\n========== FINAL SUMMARY ==========")

print(f"RAW records: {raw_count}")
print(f"TRANSLATE records after deduplication: {valid_count}")
print(f"REJECTED records: {rejected_count}")
print(f"Duplicate valid records removed: {duplicate_count}")

print("\nTRANSLATE sample:")
valid_df.show(10, truncate=False)

print("\nREJECTED sample:")
rejected_df.select(
    "patient_id",
    "first_name",
    "last_name",
    "gender",
    "phone",
    "rejection_reason"
).show(10, truncate=False)

spark.stop()

print("\nSpark session stopped.")
