"""
Fetch weather observations from OpenWeatherMap (example) and insert into weather table.

NOTE: Historical weather APIs often require a paid key for full history. This script demonstrates
how to call the API for a timestamp range and persist the hourly data to the DB.

Replace `api_key` with your OpenWeatherMap key or adapt to NOAA/other APIs.
"""

import argparse
import sqlite3
import time
from pathlib import Path
import requests
from dateutil import parser
import pandas as pd

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "nj_turnpike.db"

def fetch_hourly_openweather(lat, lon, timestamp_unix, api_key):
    # Example endpoint: OpenWeatherMap history by timestamp (this is conceptual - check API docs)
    url = f"https://api.openweathermap.org/data/2.5/onecall/timemachine"
    params = {
        "lat": lat,
        "lon": lon,
        "dt": timestamp_unix,
        "appid": api_key,
        "units": "metric"
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

def ingest_range(lat, lon, start_date, end_date, api_key):
    # iterate over days, fetch hourly data per day (API-dependent)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    current = pd.to_datetime(start_date).normalize()
    end = pd.to_datetime(end_date).normalize()
    while current <= end:
        ts = int(current.timestamp()) + 12*3600  # midday timestamp for that day
        try:
            data = fetch_hourly_openweather(lat, lon, ts, api_key)
            # parse hourly array
            for h in data.get('hourly', []):
                timestamp = pd.to_datetime(h['dt'], unit='s')
                temp_c = h.get('temp', None)
                precipitation = 0.0
                # precipitation may have 'rain' or 'snow' keys
                if 'rain' in h:
                    precipitation += h['rain'].get('1h', 0.0)
                if 'snow' in h:
                    precipitation += h['snow'].get('1h', 0.0)
                visibility = h.get('visibility', None)
                weather_main = h.get('weather', [{}])[0].get('main', None)

                cur.execute("""
                INSERT INTO weather (timestamp, temp_c, precipitation_mm, visibility_m, weather_main)
                VALUES (?, ?, ?, ?, ?)
                """, (timestamp.isoformat(), temp_c, precipitation, visibility, weather_main))
            conn.commit()
            print(f"Inserted weather for {current.date()}")
        except Exception as e:
            print("Warning: failed to fetch", current.date(), e)
        current += pd.Timedelta(days=1)
        time.sleep(1)  # be polite with API rate limits
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--lat', required=True, type=float)
    parser.add_argument('--lon', required=True, type=float)
    parser.add_argument('--start', required=True, help='YYYY-MM-DD')
    parser.add_argument('--end', required=True, help='YYYY-MM-DD')
    parser.add_argument('--api_key', required=True)
    args = parser.parse_args()
    ingest_range(args.lat, args.lon, args.start, args.end, args.api_key)
