import sqlite3
import pandas as pd
import os
import argparse


# Set up argument parsing
parser = argparse.ArgumentParser(description="Convert SQLite database tables to CSV files.")
parser.add_argument("db_path", help="Path to the SQLite database file")
parser.add_argument("output_folder", help="Folder where CSV files will be saved")


# Parse arguments
args = parser.parse_args()
db_path = args.db_path
output_folder = args.output_folder

# Ensure output directory exists
os.makedirs(output_folder, exist_ok=True)

# Connect to SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get table names, excluding "_attributes"
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '%_attributes';")
tables = cursor.fetchall()

# Convert each table to a CSV file
for table in tables:
   table_name = table[0]
   df = pd.read_sql_query(f"SELECT * FROM {table_name};", conn)
   csv_path = os.path.join(output_folder, f"{table_name}.csv")

   # Overwrite file if it exists
   df.to_csv(csv_path, index=False, mode='w', header = True, sep=";") # 'w' mode ensures overwriting
   print(f"Exported (Overwritten): {csv_path}")


# Close the database connection
conn.close()
print("All tables exported successfully (excluding _attributes).")


