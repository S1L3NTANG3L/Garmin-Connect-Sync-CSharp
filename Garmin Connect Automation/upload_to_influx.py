import argparse
import pandas as pd
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Argument parser for command-line input
parser = argparse.ArgumentParser(description="Upload CSV data to InfluxDB with upsert behavior")

parser.add_argument("--url", required=True, help="InfluxDB URL (default: http://localhost:8086)")
parser.add_argument("--token", required=True, help="InfluxDB authentication token (default: your-influxdb-token)")
parser.add_argument("--org", required=True, help="InfluxDB organization name (default: default-org)")
parser.add_argument("--bucket", required=True, help="InfluxDB bucket name (default: default-bucket)")
parser.add_argument("--file", required=True, help="Path to the CSV file (default: data.csv)")
parser.add_argument("--measurement", required=True, help="InfluxDB measurement name (default: default_measurement)")

args = parser.parse_args()

# Read the CSV file
try:
    df = pd.read_csv(args.file, delimiter=";", dtype=str)  # Read everything as string first
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit(1)

# Ensure the file is not empty
if df.shape[1] == 0:
    print("Error: CSV file is empty or incorrectly formatted.")
    exit(1)

# Identify the timestamp column (supports "day" and "timestamp")
timestamp_col = None
for col in df.columns:
    if "day" in col.lower():
        timestamp_col = col
        break
    elif "timestamp" in col.lower():
        timestamp_col = col
        break

if timestamp_col is None:
    print("Error: Could not find a suitable timestamp column (expected 'day' or 'timestamp').")
    exit(1)

# Attempt to parse the timestamp based on the detected column
try:
    if "day" in timestamp_col.lower():
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], format="%Y-%m-%d", errors="coerce")
    else:  # Assumes "timestamp" column
        df[timestamp_col] = pd.to_datetime(df[timestamp_col], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce")
except Exception as e:
    print(f"Error parsing timestamp column '{timestamp_col}': {e}")
    exit(1)

# Drop rows with invalid timestamps
df = df.dropna(subset=[timestamp_col])

# Ensure there are valid rows left
if df.empty:
    print("Error: No valid timestamp data found.")
    exit(1)

print("try connection")
# Initialize InfluxDB client
client = InfluxDBClient(url=args.url, token=args.token, org=args.org)
write_api = client.write_api(write_options=SYNCHRONOUS)
print("connection up")
# Process and send data to InfluxDB
for _, row in df.iterrows():
    point = Point(args.measurement).time(row[timestamp_col])  # Use detected timestamp column

    # Use timestamp as a unique tag to allow overwriting
    point.tag("date", row[timestamp_col].strftime("%Y-%m-%d"))

    # Add all numeric columns as fields, skipping NaN values
    for col in df.columns:
        if col != timestamp_col:  # Skip timestamp column
            value = row[col]
            if pd.notna(value) and value != "":  # Ensure value is not empty
                try:
                    if ":" in value:  # If it's a time duration, keep as string
                        point.field(col, str(value))
                    elif col == "activity_type":
                        point.field(col, str(value))
                    else:
                        point.field(col, float(value))  # Convert to float
                except ValueError:
                    print(f"Warning: Could not convert {col} value '{value}' to float. Skipping.")
    print(point)
    write_api.write(bucket=args.bucket, org=args.org, record=point)

print(f"Data from {args.file} successfully written to InfluxDB under measurement {args.measurement} with upsert-like behavior.")

# Close client
client.close()
