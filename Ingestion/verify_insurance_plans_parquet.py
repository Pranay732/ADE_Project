import os
from pathlib import Path

from pyspark.sql import SparkSession


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
# 2. PARQUET PATHS
# ============================================================

PROJECT_DIR = Path(r"E:\ADE_Project")

TRANSLATE_DIR = (
    PROJECT_DIR / "data" / "translate" / "healthcare" / "insurance_plans"
)

REJECTED_DIR = (
    PROJECT_DIR / "data" / "rejected" / "healthcare" / "insurance_plans"
)


# ============================================================
# 3. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Insurance_Plans_Parquet_SQL_Validation")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 4. READ PARQUET OUTPUTS
# ============================================================

if not TRANSLATE_DIR.exists():
    raise FileNotFoundError(f"TRANSLATE not found: {TRANSLATE_DIR}")

if not REJECTED_DIR.exists():
    raise FileNotFoundError(f"REJECTED not found: {REJECTED_DIR}")

translate_df = spark.read.parquet(str(TRANSLATE_DIR))
rejected_df = spark.read.parquet(str(REJECTED_DIR))

translate_df.createOrReplaceTempView("insurance_plans_translate")
rejected_df.createOrReplaceTempView("insurance_plans_rejected")


# ============================================================
# 5. RECORD COUNTS
# ============================================================

print("=" * 60)
print("POST-WRITE PARQUET VALIDATION")
print("=" * 60)

spark.sql("""
    SELECT 'TRANSLATE' AS layer, COUNT(*) AS record_count
    FROM insurance_plans_translate

    UNION ALL

    SELECT 'REJECTED' AS layer, COUNT(*) AS record_count
    FROM insurance_plans_rejected
""").show(truncate=False)


# ============================================================
# 6. CHECK REQUIRED AND RANGED FIELDS IN TRANSLATE
# ============================================================

print("\n========== REQUIRED AND RANGED FIELD CHECK ==========")

spark.sql("""
    SELECT
        SUM(CASE WHEN insurance_plan_id IS NULL
                      OR insurance_plan_id <= 0
                 THEN 1 ELSE 0 END)
            AS missing_or_invalid_insurance_plan_id,

        SUM(CASE WHEN insurance_company IS NULL
                      OR TRIM(insurance_company) = ''
                 THEN 1 ELSE 0 END)
            AS missing_insurance_company,

        SUM(CASE WHEN plan_name IS NULL
                      OR TRIM(plan_name) = ''
                 THEN 1 ELSE 0 END)
            AS missing_plan_name,

        SUM(CASE WHEN plan_type IS NULL
                      OR TRIM(plan_type) = ''
                 THEN 1 ELSE 0 END)
            AS missing_plan_type,

        SUM(CASE WHEN coverage_percentage IS NULL
                      OR coverage_percentage < 0.00
                      OR coverage_percentage > 100.00
                 THEN 1 ELSE 0 END)
            AS missing_or_invalid_coverage_percentage,

        SUM(CASE WHEN annual_limit < 0.00
                 THEN 1 ELSE 0 END)
            AS invalid_annual_limit,

        SUM(CASE WHEN updated_at IS NULL
                 THEN 1 ELSE 0 END)
            AS missing_updated_at,

        SUM(CASE WHEN updated_at > CURRENT_TIMESTAMP()
                 THEN 1 ELSE 0 END)
            AS future_updated_at
    FROM insurance_plans_translate
""").show(truncate=False)


# ============================================================
# 7. CHECK STATUS VALUES
# NULL IS ALLOWED; POPULATED VALUES MUST BE ACTIVE OR INACTIVE
# ============================================================

print("\n========== STATUS CHECK ==========")

spark.sql("""
    SELECT status, COUNT(*) AS record_count
    FROM insurance_plans_translate
    GROUP BY status
    ORDER BY status
""").show(truncate=False)

spark.sql("""
    SELECT COUNT(*) AS invalid_status_records
    FROM insurance_plans_translate
    WHERE status IS NOT NULL
      AND status NOT IN ('ACTIVE', 'INACTIVE')
""").show()


# ============================================================
# 8. OPTIONAL ANNUAL LIMIT
# NULL VALUES ARE ALLOWED; NEGATIVE VALUES ARE INVALID
# ============================================================

print("\n========== OPTIONAL ANNUAL LIMIT CHECK ==========")

spark.sql("""
    SELECT
        SUM(CASE WHEN annual_limit IS NULL THEN 1 ELSE 0 END)
            AS null_annual_limit,

        SUM(CASE WHEN annual_limit < 0.00 THEN 1 ELSE 0 END)
            AS invalid_annual_limit
    FROM insurance_plans_translate
""").show(truncate=False)


# ============================================================
# 9. REJECTION REASON CHECK
# ============================================================

print("\n========== REJECTION REASON CHECK ==========")

spark.sql("""
    SELECT COUNT(*) AS missing_rejection_reason
    FROM insurance_plans_rejected
    WHERE rejection_reason IS NULL
       OR TRIM(rejection_reason) = ''
""").show()

spark.sql("""
    SELECT rejection_reason, COUNT(*) AS record_count
    FROM insurance_plans_rejected
    GROUP BY rejection_reason
    ORDER BY record_count DESC
""").show(truncate=False)


# ============================================================
# 10. DUPLICATE INSURANCE PLAN IDs IN TRANSLATE
# ============================================================

print("\n========== DUPLICATE CHECK ==========")

spark.sql("""
    SELECT insurance_plan_id, COUNT(*) AS record_count
    FROM insurance_plans_translate
    GROUP BY insurance_plan_id
    HAVING COUNT(*) > 1
""").show(truncate=False)


# ============================================================
# 11. STOP SPARK
# ============================================================

spark.stop()

print("\nPost-write validation completed.")
