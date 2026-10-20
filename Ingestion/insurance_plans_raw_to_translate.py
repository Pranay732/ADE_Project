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
    PROJECT_DIR / "data" / "raw" / "healthcare" / "insurance_plans"
)

SQL_FILE = (
    PROJECT_DIR / "sql" / "healthcare" / "insurance_plans.sql"
)

TRANSLATE_DIR = (
    PROJECT_DIR / "data" / "translate" / "healthcare" / "insurance_plans"
)

REJECTED_DIR = (
    PROJECT_DIR / "data" / "rejected" / "healthcare" / "insurance_plans"
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
    .appName("Healthcare_Insurance_Plans_RAW_to_Translate")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 5. READ RAW DATA AND CAST DATATYPES
# ============================================================

print("=" * 60)
print("Insurance_Plans RAW TO TRANSLATE")
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

raw_df.createOrReplaceTempView("insurance_plans_raw")

insurance_plans_sql = SQL_FILE.read_text(encoding="utf-8")
insurance_plans_df = spark.sql(insurance_plans_sql)

print("\n========== TYPE CASTED DATA STRUCTURE ==========")
insurance_plans_df.printSchema()

if insurance_plans_df.count() != raw_count:
    raise ValueError("Record count changed during datatype casting.")


# ============================================================
# 6. STANDARDIZE VALUES
# ============================================================

print("\n========== STANDARDIZATION ==========")

insurance_plans_df = (
    insurance_plans_df
    .withColumn("insurance_plan_id", F.trim(F.col("insurance_plan_id")))
    .withColumn("insurance_company", F.trim(F.col("insurance_company")))
    .withColumn("plan_name", F.upper(F.trim(F.col("plan_name"))))
    .withColumn("plan_type", F.trim(F.col("plan_type")))
    .withColumn("coverage_percentage", F.trim(F.col("coverage_percentage")))
    .withColumn("annual_limit", F.trim(F.col("annual_limit")))
    .withColumn("status", F.trim(F.col("status")))
)

# Convert blank strings to NULL in optional text columns.
for column_name in ["insurance_company", "plan_name", "plan_type", "status"]:
    insurance_plans_df = insurance_plans_df.withColumn(
        column_name,
        F.when(
            F.trim(F.col(column_name)) == "",
            F.lit(None).cast("string")
        ).otherwise(F.col(column_name))
    )

# Standardize status: uppercase and trim.
insurance_plans_df = insurance_plans_df.withColumn(
    "status",
    F.upper(F.trim(F.col("status")))
)

print("Text standardization completed.")


# ============================================================
# 7. IDENTIFY INVALID RECORDS
# ============================================================

print("\n========== DATA QUALITY VALIDATION ==========")

rejection_conditions = [
    (
        F.col("insurance_plan_id").isNull(),
        "MISSING_INSURANCE_PLAN_ID"
    ),
    (
            F.col("insurance_plan_id") <= 0,
            "INVALID_INSURANCE_PLAN_ID"
    ),
    (
        F.col("insurance_company").isNull()
        | (F.trim(F.col("insurance_company")) == ""),
        "MISSING_INSURANCE_COMPANY"
    ),
    (
        F.col("plan_name").isNull()
        | (F.trim(F.col("plan_name")) == ""),
        "MISSING_PLAN_NAME"
    ),
    (
        F.col("plan_type").isNull()
        | (F.trim(F.col("plan_type")) == ""),
        "MISSING_PLAN_TYPE"
    ),
    (
        (F.col("coverage_percentage") < 0.00) 
        | (F.col("coverage_percentage") > 100.00),
        "INVALID_COVERAGE_PERCENTAGE"
    ),
    (
        F.col("coverage_percentage").isNull()
        | (F.trim(F.col("coverage_percentage")) == ""),
        "MISSING_COVERAGE_PERCENTAGE"
    ),
    (
        F.col("annual_limit") < 0.00,
        "INVALID_ANNUAL_LIMIT"
    ),
    (
        F.col("status").isNotNull()
        & (~F.col("status").isin("ACTIVE", "INACTIVE")),
        "INVALID_STATUS"
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

insurance_plans_df = insurance_plans_df.withColumn(
    "rejection_reason",
    rejection_reason
)

rejected_df = insurance_plans_df.filter(
    F.col("rejection_reason") != ""
)

valid_df = insurance_plans_df.filter(
    F.col("rejection_reason") == ""
).drop("rejection_reason")

print(f"Records before validation: {insurance_plans_df.count()}")
print(f"Rejected before deduplication: {rejected_df.count()}")


# ============================================================
# 8. REMOVE DUPLICATE INSURANCE PLAN IDs FROM VALID RECORDS
# ============================================================

print("\n========== DEDUPLICATION ==========")

window_spec = Window.partitionBy("insurance_plan_id").orderBy(
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
    "insurance_plan_id",
    "insurance_company",
    "plan_name",
    "plan_type",
    "coverage_percentage",
    "annual_limit",
    "status",
    "rejection_reason"
).show(10, truncate=False)

spark.stop()

print("\nSpark session stopped.")
