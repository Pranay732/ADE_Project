import pyodbc
import csv
import os
from datetime import datetime

SERVER = "localhost"
DATABASE = "Healthcare_Source"
RAW_FOLDER = r"E:\ADE_Project\data\raw"

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

conn = pyodbc.connect(connection_string)

print("Database connection successful.")

cursor = conn.cursor()

table_name = input("Enter table name: ")

query = f"""
SELECT *
FROM {table_name}
"""

cursor.execute(query)

rows = cursor.fetchall()

print("Records extracted:", len(rows))

columns = [column[0] for column in cursor.description]

table_folder = os.path.join(
    RAW_FOLDER,
    table_name
)

os.makedirs(
    table_folder,
    exist_ok=True
)

timestamp = datetime.now().strftime(
    "%Y%m%d_%H%M%S"
)

output_file = os.path.join(
    table_folder,
    f"{table_name}_{timestamp}.csv"
)

with open(
    output_file,
    mode="w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.writer(file)

    writer.writerow(columns)
    writer.writerows(rows)

print("RAW file created:", output_file)

conn.close()