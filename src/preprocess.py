"""
Preprocess & join traffic and weather data into a clean table for modeling.

- Reads from SQLite traffic_records and weather tables
- Performs time alignment (round timestamps to the nearest hour)
- Fills small gaps and drops rows with no target (travel_time_min)
- Saves a cleaned CSV to data/cleaned_features.csv (and optionally back to DB)
"""

import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "nj_turnpike.db"
OUT_CSV = Path(__file__).resolve().parents[1] / "data" / "cleaned_features.csv"

def load_tables():
    conn = sqlite3.connect(DB_PATH)
    traffic = pd.read_sql_query("SELECT * FROM traffic_records", conn, parse_dates=['timestamp'])
    weather = pd.read_sql_query("SELECT * FROM weather", conn, parse_dates=['timestamp'])
    conn.close()
    return traffic, weather

def preprocess():
    traffic, weather = load_tables()
    # round timestamps to hour (makes joining simpler)
    traffic['ts_hour'] = pd.to_datetime(traffic['timestamp']).dt.round('H')
    weather['ts_hour'] = pd.to_datetime(weather['timestamp']).dt.round('H')

    # group traffic per segment-hour: average speed, mean travel_time, sum vehicle_count
    grouped = traffic.groupby(['road_segment','ts_hour']).agg({
        'avg_speed':'mean',
        'travel_time_min':'mean',
        'vehicle_count':'sum'
    }).reset_index()

    # merge with weather (left join so we keep traffic records)
    merged = pd.merge(grouped, weather[['ts_hour','temp_c','precipitation_mm','visibility_m','weather_main']],
                      how='left', on='ts_hour')

    # simple cleaning: drop rows without travel_time (target)
    merged = merged.dropna(subset=['travel_time_min'])

    # save cleaned
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_CSV, index=False)
    print(f"Saved cleaned features to {OUT_CSV} ({len(merged)} rows)")

if __name__ == "__main__":
    preprocess()
