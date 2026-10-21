import os
from pathlib import Path

from pyspark.sql import SparkSession


# ============================================================
# 1. HADOOP CONFIGURATION
# ============================================================

os.environ["HADOOP_HOME"] = r"C:\hadoop"

hadoop_bin = r"C:\hadoop\bin"

if hadoop_bin not in os.environ.get("PATH", ""):
    os.environ["PATH"] = (
        hadoop_bin + os.pathsep + os.environ.get("PATH", "")
    )


# ============================================================
# 2. PARQUET PATHS
# ============================================================

PROJECT_DIR = Path(r"E:\ADE_Project")

TRANSLATE_DIR = (
    PROJECT_DIR / "data" / "translate" / "healthcare" / "patients"
)

REJECTED_DIR = (
    PROJECT_DIR / "data" / "rejected" / "healthcare" / "patients"
)


# ============================================================
# 3. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Patients_Parquet_SQL_Validation")
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

translate_df.createOrReplaceTempView("patients_translate")
rejected_df.createOrReplaceTempView("patients_rejected")


# ============================================================
# 5. RECORD COUNTS
# ============================================================

print("=" * 60)
print("POST-WRITE PARQUET VALIDATION")
print("=" * 60)

spark.sql("""
    SELECT 'TRANSLATE' AS layer, COUNT(*) AS record_count
    FROM patients_translate

    UNION ALL

    SELECT 'REJECTED' AS layer, COUNT(*) AS record_count
    FROM patients_rejected
""").show(truncate=False)


# ============================================================
# 6. CHECK MANDATORY FIELDS IN TRANSLATE
# ============================================================

print("\n========== MANDATORY FIELD CHECK ==========")

spark.sql("""
    SELECT
        SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END)
            AS missing_patient_id,

        SUM(CASE WHEN first_name IS NULL
                      OR TRIM(first_name) = ''
                 THEN 1 ELSE 0 END)
            AS missing_first_name,

        SUM(CASE WHEN last_name IS NULL
                      OR TRIM(last_name) = ''
                 THEN 1 ELSE 0 END)
            AS missing_last_name,

        SUM(CASE WHEN phone IS NULL
                      OR NOT phone RLIKE '^[0-9]{10}$'
                 THEN 1 ELSE 0 END)
            AS missing_or_invalid_phone,

        SUM(CASE WHEN registration_date IS NULL
                 THEN 1 ELSE 0 END)
            AS missing_registration_date,

        SUM(CASE WHEN updated_at IS NULL
                 THEN 1 ELSE 0 END)
            AS missing_updated_at,

        SUM(CASE WHEN registration_date > CURRENT_DATE()
                 THEN 1 ELSE 0 END)
            AS future_registration_date,

        SUM(CASE WHEN date_of_birth > CURRENT_DATE()
                 THEN 1 ELSE 0 END)
            AS future_date_of_birth
    FROM patients_translate
""").show(truncate=False)


# ============================================================
# 7. CHECK GENDER VALUES
# NULL IS ALLOWED; ONLY M AND F ARE ACCEPTED WHEN POPULATED
# ============================================================

print("\n========== GENDER CHECK ==========")

spark.sql("""
    SELECT gender, COUNT(*) AS record_count
    FROM patients_translate
    GROUP BY gender
    ORDER BY gender
""").show(truncate=False)

spark.sql("""
    SELECT COUNT(*) AS invalid_gender_records
    FROM patients_translate
    WHERE gender IS NOT NULL
      AND gender NOT IN ('M', 'F')
""").show()


# ============================================================
# 8. CHECK EMAIL FORMAT
# INVALID OR MISSING EMAILS SHOULD BE NULL
# ============================================================

print("\n========== EMAIL CHECK ==========")

spark.sql("""
    SELECT
        SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END)
            AS null_emails,

        SUM(CASE
                WHEN email IS NOT NULL
                 AND NOT email RLIKE '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$'
                THEN 1 ELSE 0
            END)
            AS invalid_non_null_emails
    FROM patients_translate
""").show(truncate=False)


# ============================================================
# 9. OPTIONAL ADDRESS FIELDS
# NULL VALUES ARE ALLOWED
# ============================================================

print("\n========== OPTIONAL FIELD COUNTS ==========")

spark.sql("""
    SELECT
        SUM(CASE WHEN address IS NULL THEN 1 ELSE 0 END)
            AS null_address,

        SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END)
            AS null_city,

        SUM(CASE WHEN state IS NULL THEN 1 ELSE 0 END)
            AS null_state
    FROM patients_translate
""").show(truncate=False)


# ============================================================
# 10. REJECTION REASON CHECK
# ============================================================

print("\n========== REJECTION REASON CHECK ==========")

spark.sql("""
    SELECT COUNT(*) AS missing_rejection_reason
    FROM patients_rejected
    WHERE rejection_reason IS NULL
       OR TRIM(rejection_reason) = ''
""").show()

spark.sql("""
    SELECT rejection_reason, COUNT(*) AS record_count
    FROM patients_rejected
    GROUP BY rejection_reason
    ORDER BY record_count DESC
""").show(truncate=False)


# ============================================================
# 11. DUPLICATE PATIENT IDs IN TRANSLATE
# ============================================================

print("\n========== DUPLICATE CHECK ==========")

spark.sql("""
    SELECT patient_id, COUNT(*) AS record_count
    FROM patients_translate
    GROUP BY patient_id
    HAVING COUNT(*) > 1
""").show(truncate=False)


# ============================================================
# 12. STOP SPARK
# ============================================================

spark.stop()

print("\nPost-write validation completed.")