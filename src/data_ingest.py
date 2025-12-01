"""
Simple CSV ingester to load traffic CSV into the traffic_records table.

- Expects CSV with columns: timestamp, road_segment, avg_speed, travel_time_min, vehicle_count
- If your CSV has different headers, adjust the mapping below.
"""

import argparse
import pandas as pd
import sqlite3
from pathlib import Path
from dateutil import parser as dateparser

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "nj_turnpike.db"

def ingest_csv(csv_path):
    # read CSV using pandas (robust to many formats)
    df = pd.read_csv(csv_path)
    # normalize column names (lowercase)
    df.columns = [c.strip().lower() for c in df.columns]

    # try to map commonly-named columns to our schema
    # adjust this mapping if your CSV differs
    col_map = {}
    if 'timestamp' in df.columns:
        col_map['timestamp'] = 'timestamp'
    elif 'date_time' in df.columns:
        col_map['timestamp'] = 'date_time'
    else:
        raise ValueError("CSV must have a timestamp column named 'timestamp' or 'date_time'")

    col_map['road_segment'] = next((c for c in ['road_segment','segment','location'] if c in df.columns), None)
    col_map['avg_speed'] = next((c for c in ['avg_speed','speed','mean_speed'] if c in df.columns), None)
    col_map['travel_time_min'] = next((c for c in ['travel_time_min','travel_time','duration_min'] if c in df.columns), None)
    col_map['vehicle_count'] = next((c for c in ['vehicle_count','count','volume'] if c in df.columns), None)

    # minimal validation
    if not col_map['road_segment']:
        raise ValueError("Could not find a road segment column in the CSV")
    # keep only the columns we need (missing columns will be filled with NaN)
    df2 = pd.DataFrame({
        'timestamp': pd.to_datetime(df[col_map['timestamp']]),
        'road_segment': df[col_map['road_segment']],
        'avg_speed': df[col_map['avg_speed']] if col_map['avg_speed'] else None,
        'travel_time_min': df[col_map['travel_time_min']] if col_map['travel_time_min'] else None,
        'vehicle_count': df[col_map['vehicle_count']] if col_map['vehicle_count'] else None
    })

    # save to sqlite
    conn = sqlite3.connect(DB_PATH)
    df2.to_sql('traffic_records', conn, if_exists='append', index=False)
    conn.close()
    print(f"Ingested {len(df2)} rows from {csv_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', required=True, help='Path to traffic CSV file')
    args = parser.parse_args()
    ingest_csv(args.csv)
