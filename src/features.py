"""
Feature engineering:

- adds time features (hour of day, day of week)
- computes lag features (previous hour avg_speed) using groupby/shift
- encodes weather severity into a simple index
- outputs features.csv used for model training
"""

import pandas as pd
from pathlib import Path

CLEANED_CSV = Path(__file__).resolve().parents[1] / "data" / "cleaned_features.csv"
FEAT_CSV = Path(__file__).resolve().parents[1] / "data" / "features.csv"

def weather_severity(row):
    # simple heuristic combining precipitation and visibility
    severity = 0
    if pd.notna(row['precipitation_mm']) and row['precipitation_mm'] > 0.5:
        severity += 1
    if pd.notna(row['visibility_m']) and row['visibility_m'] < 5000:
        severity += 1
    if pd.notna(row['weather_main']) and row['weather_main'] in ['Thunderstorm','Snow']:
        severity += 2
    return severity

def make_features():
    df = pd.read_csv(CLEANED_CSV, parse_dates=['ts_hour'])
    df['hour'] = df['ts_hour'].dt.hour
    df['dayofweek'] = df['ts_hour'].dt.dayofweek
    # lag features per road_segment
    df = df.sort_values(['road_segment','ts_hour'])
    df['prev_avg_speed'] = df.groupby('road_segment')['avg_speed'].shift(1)
    # fill naive missing lags with rolling mean or current value
    df['prev_avg_speed'] = df['prev_avg_speed'].fillna(df['avg_speed'].rolling(3,min_periods=1).mean())

    # weather severity
    df['weather_severity'] = df.apply(weather_severity, axis=1)

    # target is travel_time_min
    df = df.dropna(subset=['travel_time_min'])  # ensure target exists
    df.to_csv(FEAT_CSV, index=False)
    print(f"Saved engineered features to {FEAT_CSV} ({len(df)} rows)")

if __name__ == "__main__":
    make_features()
