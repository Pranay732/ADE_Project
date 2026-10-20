import pyodbc
import csv
import os
from datetime import datetime


# ============================================================
# 1. DATABASE CONFIGURATION
# ============================================================

SERVER = "localhost"
DATABASE = "Healthcare_Source"

RAW_FOLDER = r"E:\ADE_Project\data\raw\healthcare"


# ============================================================
# 2. RUNTIME PARAMETER
# ============================================================

LOAD_TYPE = "FULL"


# ============================================================
# 3. VALIDATE RUNTIME PARAMETER
# ============================================================

if LOAD_TYPE not in ["FULL", "INCREMENTAL"]:
    raise ValueError(
        "LOAD_TYPE must be either FULL or INCREMENTAL"
    )

print("========================================")
print("Healthcare Data Ingestion")
print("========================================")
print(f"Load type selected: {LOAD_TYPE}")
print()


# ============================================================
# 4. DATABASE CONNECTION
# ============================================================

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

conn = pyodbc.connect(connection_string)

print("Database connection successful.")
print()


# ============================================================
# 5. READ TABLE METADATA
# ============================================================

cursor = conn.cursor()

metadata_query = """
SELECT
    prcs_id,
    source_table_name,
    primary_key_column,
    watermark_column,
    raw_folder
FROM etl_table_config
WHERE is_active = 1
ORDER BY prcs_id
"""

cursor.execute(metadata_query)

table_configs = cursor.fetchall()

print(f"Active tables found: {len(table_configs)}")
print()


# ============================================================
# 6. PROCESS EACH TABLE
# ============================================================

for config in table_configs:

    prcs_id = config.prcs_id
    table_name = config.source_table_name
    primary_key_column = config.primary_key_column
    watermark_column = config.watermark_column
    metadata_raw_folder = config.raw_folder

    print("========================================")
    print(f"Config ID       : {prcs_id}")
    print(f"Table           : {table_name}")
    print(f"Primary Key     : {primary_key_column}")
    print(f"Watermark       : {watermark_column}")
    print(f"Load Type       : {LOAD_TYPE}")
    print("========================================")

    # --------------------------------------------------------
    # FULL LOAD
    # --------------------------------------------------------

    if LOAD_TYPE == "FULL":

        query = f"""
        SELECT *
        FROM {table_name}
        """

        print("Executing FULL LOAD...")
        print(f"Query: {query.strip()}")

        cursor.execute(query)

        rows = cursor.fetchall()

        print(f"Records extracted: {len(rows)}")

        # ----------------------------------------------------
        # Get column names
        # ----------------------------------------------------

        columns = [
            column[0]
            for column in cursor.description
        ]

        # ----------------------------------------------------
        # Create RAW table folder
        # ----------------------------------------------------

        table_folder = os.path.join(
            RAW_FOLDER,
            table_name
        )

        os.makedirs(
            table_folder,
            exist_ok=True
        )

        # ----------------------------------------------------
        # Create timestamp
        # ----------------------------------------------------

        timestamp = datetime.now().strftime(
            "%Y%m%d_%H%M%S"
        )

        output_file = os.path.join(
            table_folder,
            f"{table_name}_{timestamp}.csv"
        )

        # ----------------------------------------------------
        # Write RAW CSV
        # ----------------------------------------------------

        with open(
            output_file,
            mode="w",
            newline="",
            encoding="utf-8"
        ) as file:

            writer = csv.writer(file)

            # Header
            writer.writerow(columns)

            # Data
            writer.writerows(rows)

        print(f"RAW file created successfully:")
        print(output_file)
        print()


# ============================================================
# 7. CLOSE CONNECTION
# ============================================================

conn.close()

print("========================================")
print("All table ingestion completed.")
print("========================================")