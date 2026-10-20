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
    PROJECT_DIR / "data" / "raw" / "healthcare" / "payments"
)

SQL_FILE = (
    PROJECT_DIR / "sql" / "healthcare" / "payments.sql"
)

TRANSLATE_DIR = (
    PROJECT_DIR / "data" / "translate" / "healthcare" / "payments"
)

REJECTED_DIR = (
    PROJECT_DIR / "data" / "rejected" / "healthcare" / "payments"
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
    .appName("Healthcare_Payments_RAW_to_Translate")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 5. READ RAW DATA AND CAST DATATYPES
# ============================================================

print("=" * 60)
print("Payments RAW TO TRANSLATE")
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

raw_df.createOrReplaceTempView("payments_raw")

payments_sql = SQL_FILE.read_text(encoding="utf-8")
payments_df = spark.sql(payments_sql)

print("\n========== TYPE CASTED DATA STRUCTURE ==========")
payments_df.printSchema()

payments_df.show(10, truncate=False)

if payments_df.count() != raw_count:
    raise ValueError("Record count changed during datatype casting.")


# ============================================================
# 6. STANDARDIZE VALUES
# ============================================================

print("\n========== STANDARDIZATION ==========")

payments_df = (
    payments_df
    .withColumn("patient_id", F.trim(F.col("patient_id")))
    .withColumn("payment_amount", F.trim(F.col("payment_amount")))
    .withColumn("payment_method", F.trim(F.col("payment_method")))
    .withColumn("payment_status", F.trim(F.col("payment_status")))
    .withColumn("transaction_ref", F.trim(F.col("transaction_ref")))
)

# Convert blank strings to NULL in optional text columns.
for column_name in ["payment_method", "payment_status", "transaction_ref"]:
    payments_df = payments_df.withColumn(
        column_name,
        F.when(
            F.trim(F.col(column_name)) == "",
            F.lit(None).cast("string")
        ).otherwise(F.col(column_name))
    )

# Standardize status: uppercase and trim.
payments_df = payments_df.withColumn(
    "payment_status",
    F.upper(F.trim(F.col("payment_status")))
)

print("Text standardization completed.")


# ============================================================
# 7. IDENTIFY INVALID RECORDS
# ============================================================

print("\n========== DATA QUALITY VALIDATION ==========")

rejection_conditions = [
    (
        F.col("payment_id").isNull(),
        "MISSING_PAYMENT_ID"
    ),
    (
        F.col("payment_id") <= 0,
        "INVALID_PAYMENT_ID"
    ),
    (
        F.col("claim_id").isNull(),
        "MISSING_CLAIM_ID"
    ),
    (
        F.col("claim_id") <= 0,
        "INVALID_CLAIM_ID"
    ),
    (
        F.col("patient_id").isNull(),
        "MISSING_PATIENT_ID"
    ),
    (
        F.col("patient_id") <= 0,
        "INVALID_PATIENT_ID"
    ),
    (
        F.col("payment_date") > F.current_date(),
        "FUTURE_PAYMENT_DATE"
    ),
    (
        F.col("payment_amount") < 0.00,
        "INVALID_PAYMENT_AMOUNT"
    ),
    (
        F.col("payment_method").isNull()
        | (F.trim(F.col("payment_method")) == ""),
        "MISSING_PAYMENT_METHOD"
    ),
    (
        F.col("payment_status").isNotNull()
        & (~F.col("payment_status").isin("SUCCESSFUL", "PENDING", "FAILED")),
        "INVALID_PAYMENT_STATUS"
    ),
    (
        F.col("transaction_ref").isNull()
        | (F.trim(F.col("transaction_ref")) == ""),
        "MISSING_TRANSACTION_REF"
    ),
    (
        F.col("updated_at").isNull(),
        "MISSING_UPDATED_AT"
    ),
    (
        F.col("updated_at") > F.current_timestamp(),
        "FUTURE_UPDATED_AT"
    )
]

rejection_reason = F.concat_ws(
    ", ",
    *[
        F.when(condition, F.lit(reason))
        for condition, reason in rejection_conditions
    ]
)

payments_df = payments_df.withColumn(
    "rejection_reason",
    rejection_reason
)

rejected_df = payments_df.filter(
    F.col("rejection_reason") != ""
)

valid_df = payments_df.filter(
    F.col("rejection_reason") == ""
).drop("rejection_reason")

print(f"Records before validation: {payments_df.count()}")
print(f"Rejected before deduplication: {rejected_df.count()}")


# ============================================================
# 8. REMOVE DUPLICATE PAYMENT IDs FROM VALID RECORDS
# ============================================================

print("\n========== DEDUPLICATION ==========")

window_spec = Window.partitionBy("payment_id").orderBy(
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
print(f"Valid records: {valid_count}")
print(f"REJECTED records: {rejected_count}")
print(f"Duplicate records removed: {duplicate_count}")

print("\nTRANSLATE sample:")
valid_df.show(10, truncate=False)

print("\nREJECTED sample:")
rejected_df.select(
    "payment_id",
    "claim_id",
    "patient_id",
    "payment_date",
    "payment_amount",
    "payment_method",
    "payment_status",
    "transaction_ref",
    "rejection_reason"
).show(10, truncate=False)

spark.stop()

print("\nSpark session stopped.")
