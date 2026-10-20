"""
Healthcare RAW CSV -> typed/cleaned Parquet pipeline.
Pattern follows the starter snippet:
1. Read a raw CSV.
2. Show the raw data/schema.
3. Create a temporary view.
4. Read that table's Spark SQL file.
5. Execute SQL casts and conservative text cleanup.
6. Save a cleaned Parquet copy and a simple quality report.

Expected folder structure:
TestProject/
  data/raw/healthcare/patients/patients_20261008_172622.csv
  data/raw/healthcare/appointments/appointments_<timestamp>.csv
  ...one folder per table...
  sql/patients.sql
  sql/appointments.sql
  ...one SQL file per table...
  run_healthcare_cleaning.py

This script never writes to SQL Server. It reads the exported RAW CSV files.
"""

from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

# ==========================================================
# CONFIGURATION: change this to your project folder
# ==========================================================
project_path = Path(r"E:\ADE_Project")

raw_root = project_path / "data" / "raw" / "healthcare"
sql_root = project_path / "sql"
output_root = project_path / "data" / "cleaned" / "healthcare"
report_root = project_path / "reports"

TABLES = [
    "appointments",
    "claim_lines",
    "claims",
    "diagnoses",
    "encounters",
    "etl_run_log",
    "etl_table_config",
    "etl_watermark",
    "hospitals",
    "insurance_plans",
    "patient_insurance",
    "patients",
    "payments",
    "procedures",
    "providers",
]

PRIMARY_KEYS = {
    "appointments": "appointment_id",
    "claim_lines": "claim_line_id",
    "claims": "claim_id",
    "diagnoses": "diagnosis_id",
    "encounters": "encounter_id",
    "etl_run_log": "prcs_run_id",
    "etl_table_config": "prcs_id",
    "etl_watermark": "watermark_id",
    "hospitals": "hospital_id",
    "insurance_plans": "insurance_plan_id",
    "patient_insurance": "patient_insurance_id",
    "patients": "patient_id",
    "payments": "payment_id",
    "procedures": "procedure_id",
    "providers": "provider_id",
}

# ==========================================================
# CREATE SPARK SESSION
# ==========================================================
spark = (
    SparkSession.builder
    .appName("Healthcare_RAW_to_Cleaned")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
output_root.mkdir(parents=True, exist_ok=True)
report_root.mkdir(parents=True, exist_ok=True)

quality_records = []
summary_records = []

def find_latest_csv(table_name):
    """Find the most recently modified CSV for a table's RAW folder."""
    table_folder = raw_root / table_name
    if not table_folder.exists():
        raise FileNotFoundError(
            f"RAW folder not found for {table_name}: {table_folder}"
        )

    # Supports names such as patients_20261008_172622.csv.
    matches = list(table_folder.glob(f"{table_name}_*.csv"))
    if not matches:
        # Also support a simple filename such as patients.csv.
        simple_file = table_folder / f"{table_name}.csv"
        if simple_file.exists():
            matches = [simple_file]

    if not matches:
        raise FileNotFoundError(
            f"No CSV found for {table_name} in {table_folder}. "
            f"Expected {table_name}_<timestamp>.csv or {table_name}.csv"
        )

    return max(matches, key=lambda file_path: file_path.stat().st_mtime)


def read_sql_file(sql_path):
    """Read the Spark SQL transformation from a .sql file."""
    with open(sql_path, "r", encoding="utf-8") as file:
        return file.read()


def count_nulls(dataframe):
    """Return null count for each column in a DataFrame."""
    expressions = [
        F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(name)
        for name in dataframe.columns
    ]
    result = dataframe.select(expressions).first()
    return {name: int(result[name] or 0) for name in dataframe.columns}


try:
    for table_name in TABLES:
        print("\n" + "=" * 75)
        print(f"TABLE: {table_name}")
        print("=" * 75)

        raw_path = find_latest_csv(table_name)
        sql_path = sql_root / "healthcare" / f"{table_name}.sql"

        if not sql_path.is_file():
            raise FileNotFoundError(f"SQL transformation file not found: {sql_path}")

        print(f"RAW CSV: {raw_path}")

        # READ RAW DATA: all columns initially arrive as strings.
        raw_df = (
            spark.read
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("inferSchema", "false")
            .csv(str(raw_path))
        )

        print("\n========== RAW DATA SAMPLE ==========")
        raw_df.show(5, truncate=False)

        print("\n========== RAW DATA STRUCTURE ==========")
        raw_df.printSchema()

        # Create a view that the table-specific SQL file will query.
        raw_df.createOrReplaceTempView(f"{table_name}_raw")
        print(f"Temporary view created: {table_name}_raw")

        # Run the table-specific Spark SQL transformation.
        sql_text = read_sql_file(sql_path)
        cleaned_df = spark.sql(sql_text)

        print("\n========== CLEANED DATA SAMPLE ==========")
        cleaned_df.show(5, truncate=False)

        print("\n========== CLEANED DATA STRUCTURE ==========")
        cleaned_df.printSchema()

        raw_count = raw_df.count()
        cleaned_count = cleaned_df.count()

        # Profile missing values after type casting and standardization.
        for column_name, null_count in count_nulls(cleaned_df).items():
            quality_records.append(
                (table_name, "null_count_after_cleaning",
                 column_name, int(null_count))
            )

        # Primary-key checks: report only; do not delete duplicate rows.
        primary_key = PRIMARY_KEYS[table_name]
        if primary_key in cleaned_df.columns:
            duplicate_groups = (
                cleaned_df.groupBy(primary_key)
                .count()
                .filter(F.col("count") > 1)
                .count()
            )
            null_key_rows = cleaned_df.filter(
                F.col(primary_key).isNull()
            ).count()

            quality_records.append(
                (table_name, "duplicate_primary_key_groups",
                 primary_key, int(duplicate_groups))
            )
            quality_records.append(
                (table_name, "null_primary_key_rows",
                 primary_key, int(null_key_rows))
            )
        else:
            duplicate_groups = -1
            null_key_rows = -1
            quality_records.append(
                (table_name, "missing_primary_key_column",
                 primary_key, 1)
            )

        # Save a separate cleaned Parquet dataset for this table.
        table_output = output_root / table_name
        (
            cleaned_df.write
            .mode("overwrite")
            .parquet(str(table_output))
        )

        summary_records.append((
            table_name,
            str(raw_path),
            int(raw_count),
            int(cleaned_count),
            int(duplicate_groups),
            int(null_key_rows),
            str(table_output),
        ))

        print(f"RAW rows: {raw_count}")
        print(f"CLEANED rows: {cleaned_count}")
        print(f"Output: {table_output}")

    # Write reports. Spark creates a folder with part-*.csv files.
    quality_schema = [
        "table_name", "check_type", "column_or_rule", "problem_count"
    ]
    quality_df = spark.createDataFrame(quality_records, quality_schema)
    (
        quality_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(str(report_root / "quality_report"))
    )

    summary_schema = [
        "table_name", "raw_file", "raw_rows", "cleaned_rows",
        "duplicate_primary_key_groups", "null_primary_key_rows",
        "cleaned_output_path",
    ]
    summary_df = spark.createDataFrame(summary_records, summary_schema)
    (
        summary_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(str(report_root / "table_summary"))
    )

    print("\nALL TABLES COMPLETED.")
    print(f"Cleaned data folder: {output_root}")
    print(f"Quality report folder: {report_root / 'quality_report'}")
    print(f"Summary report folder: {report_root / 'table_summary'}")

finally:
    spark.stop()