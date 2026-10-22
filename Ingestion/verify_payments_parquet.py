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
    PROJECT_DIR / "data" / "translate" / "healthcare" / "payments"
)

REJECTED_DIR = (
    PROJECT_DIR / "data" / "rejected" / "healthcare" / "payments"
)


# ============================================================
# 3. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("Payments_Parquet_SQL_Validation")
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

translate_df.createOrReplaceTempView("payments_translate")
rejected_df.createOrReplaceTempView("payments_rejected")


# ============================================================
# 5. RECORD COUNTS
# ============================================================

print("=" * 60)
print("POST-WRITE PARQUET VALIDATION")
print("=" * 60)

spark.sql("""
    SELECT 'TRANSLATE' AS layer, COUNT(*) AS record_count
    FROM payments_translate

    UNION ALL

    SELECT 'REJECTED' AS layer, COUNT(*) AS record_count
    FROM payments_rejected
""").show(truncate=False)


# ============================================================
# 6. CHECK MANDATORY FIELDS IN TRANSLATE
# ============================================================

print("\n========== MANDATORY FIELD CHECK ==========")

spark.sql("""SELECT
    SUM(CASE WHEN payment_id IS NULL
             THEN 1 ELSE 0 END) AS missing_payment_id,

    SUM(CASE WHEN claim_id IS NULL
             THEN 1 ELSE 0 END) AS missing_claim_id,

    SUM(CASE WHEN patient_id IS NULL
             THEN 1 ELSE 0 END) AS missing_patient_id,

    SUM(CASE WHEN payment_amount <= 0.00
             THEN 1 ELSE 0 END) AS invalid_payment_amount,

    SUM(CASE WHEN payment_method IS NULL
                  OR LTRIM(RTRIM(payment_method)) = ''
             THEN 1 ELSE 0 END) AS missing_payment_method,

    SUM(CASE WHEN payment_status IS NULL
                  OR payment_status NOT IN ('SUCCESSFUL', 'PENDING', 'FAILED')
             THEN 1 ELSE 0 END) AS invalid_payment_status,

    SUM(CASE WHEN transaction_ref IS NULL
                  OR LTRIM(RTRIM(transaction_ref)) = ''
             THEN 1 ELSE 0 END) AS missing_transaction_ref,

    SUM(CASE WHEN updated_at IS NULL
             THEN 1 ELSE 0 END) AS missing_updated_at
    FROM payments_translate
""").show(truncate=False)


# ============================================================
# 7. ALL PAYMENT METHODS
# NULL IS ALLOWED; ONLY Bank Transfer, Card, Cash, Insurance ARE ACCEPTED WHEN POPULATED
# ============================================================

print("\n========== PAYMENT METHOD CHECK ==========")

spark.sql("""
    SELECT payment_method, COUNT(*) AS record_count
    FROM payments_translate
    GROUP BY payment_method
    ORDER BY payment_method
""").show(truncate=False)

spark.sql("""
    SELECT COUNT(*) AS invalid_payment_method_records
    FROM payments_translate
    WHERE payment_method IS NOT NULL
      AND payment_method NOT IN ('Bank Transfer', 'Card', 'Cash', 'Insurance')
""").show()


# ============================================================
# 8. CHECK PAYMENT STATUS FORMAT
# INVALID OR MISSING PAYMENT STATUS SHOULD BE NULL
# ============================================================

print("\n========== PAYMENT STATUS CHECK ==========")

spark.sql("""
    SELECT
        SUM(CASE WHEN payment_status IS NULL
                  OR payment_status NOT IN ('SUCCESSFUL', 'PENDING', 'FAILED')
             THEN 1 ELSE 0 END) AS invalid_payment_status
    FROM payments_translate
""").show(truncate=False)


# ============================================================
# 9. OPTIONAL ADDRESS FIELDS
# NULL VALUES ARE ALLOWED
# ============================================================

print("\n========== OPTIONAL FIELD COUNTS ==========")

spark.sql("""
    SELECT
        SUM(CASE WHEN transaction_ref IS NULL
                  OR LTRIM(RTRIM(transaction_ref)) = ''
             THEN 1 ELSE 0 END) AS missing_transaction_ref
    FROM payments_translate
""").show(truncate=False)


# ============================================================
# 10. REJECTION REASON CHECK
# ============================================================

print("\n========== REJECTION REASON CHECK ==========")

spark.sql("""
    SELECT COUNT(*) AS missing_rejection_reason
        FROM payments_rejected
        WHERE rejection_reason IS NULL
           OR TRIM(rejection_reason) = ''
""").show()

spark.sql("""
    SELECT rejection_reason, COUNT(*) AS record_count
    FROM payments_rejected
    GROUP BY rejection_reason
    ORDER BY record_count DESC
""").show(truncate=False)


# ============================================================
# 11. DUPLICATE PAYMENTS IDs IN TRANSLATE
# ============================================================

print("\n========== DUPLICATE CHECK ==========")

spark.sql("""
    SELECT payment_id, COUNT(*) AS record_count
    FROM payments_translate
    GROUP BY payment_id
    HAVING COUNT(*) > 1
""").show(truncate=False)


# ============================================================
# 12. STOP SPARK
# ============================================================

spark.stop()

print("\nPost-write validation completed.")