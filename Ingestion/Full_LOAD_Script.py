import pyodbc
import csv
import os


# -----------------------------
# SQL Server connection
# -----------------------------

connection = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=SourceDB;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

cursor = connection.cursor()


# -----------------------------
# Raw folder
# -----------------------------

raw_path = r"E:\ADE_Project\data\raw"

os.makedirs(raw_path, exist_ok=True)


# -----------------------------
# Test extraction
# -----------------------------

query = """
SELECT *
FROM dbo.Customers
"""

cursor.execute(query)

columns = [column[0] for column in cursor.description]

rows = cursor.fetchall()


# -----------------------------
# Write CSV
# -----------------------------

file_path = os.path.join(
    raw_path,
    "customers.csv"
)

with open(
    file_path,
    mode="w",
    newline="",
    encoding="utf-8"
) as file:

    writer = csv.writer(file)

    writer.writerow(columns)

    writer.writerows(rows)


# -----------------------------
# Close connection
# -----------------------------

cursor.close()
connection.close()


print("Customers extracted successfully.")
print(f"Records extracted: {len(rows)}")
print(f"File created: {file_path}")
