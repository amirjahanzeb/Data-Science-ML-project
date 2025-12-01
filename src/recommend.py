"""
Simple command-line recommender:

Given a road_segment, a date, and a departure window (start_time .. end_time),
this script simulates candidate departure times (every `window_minutes`) and
returns the time with the lowest predicted travel_time_min using the trained model.

It uses the latest available weather & current features from features.csv. In a production system,
you'd query the DB for the exact route & time and real-time weather forecast.
"""

import argparse
import pandas as pd
from pathlib import Path
import joblib
from datetime import datetime, timedelta

FEAT_CSV = Path(__file__).resolve().parents[1] / "data" / "features.csv"
MODEL_PATH = Path(__file__).resolve().parents[1] / "models" / "trained_model.joblib"

def load_model():
    obj = joblib.load(MODEL_PATH)
    return obj['model'], obj['feature_cols']

def build_candidate_rows(base_df, segment, dt):
    # base_df is features.csv; we'll find the most recent row for that segment & hour and update hour/dayofweek
    # Simpler approach: take the median feature vector for the segment/hour-of-day if available
    hour = dt.hour
    seg = base_df[base_df['road_segment']==segment]
    subset = seg[seg['hour']==hour]
    if len(subset)==0:
        subset = seg
    if len(subset)==0:
        # fallback to global median
        return base_df.median(numeric_only=True).to_frame().T

    # return median row numeric features as dataframe
    return subset.median(numeric_only=True).to_frame().T

def recommend(segment, date_str, start_time_str, end_time_str, window_minutes=15):
    model, feature_cols = load_model()
    base_df = pd.read_csv(FEAT_CSV, parse_dates=['ts_hour'])

    date = pd.to_datetime(date_str).date()
    start_dt = datetime.combine(date, datetime.strptime(start_time_str, "%H:%M").time())
    end_dt = datetime.combine(date, datetime.strptime(end_time_str, "%H:%M").time())

    candidates = []
    t = start_dt
    while t <= end_dt:
        # assemble a feature vector for this candidate time (use median historical for that segment/hour)
        row = build_candidate_rows(base_df, segment, t)
        # set dynamic fields
        row = row.copy()
        # Ensure expected features exist in the row; if not, fill with 0
        for c in feature_cols:
            if c not in row.columns:
                row[c] = 0.0
        # overwrite hour/dayofweek
        row['hour'] = t.hour
        row['dayofweek'] = t.weekday()
        X = row[feature_cols].fillna(0.0).values.reshape(1, -1)
        pred = model.predict(X)[0]
        candidates.append((t.strftime("%H:%M"), pred))
        t += timedelta(minutes=window_minutes)

    # pick the candidate with min predicted time
    best_time, best_pred = min(candidates, key=lambda x: x[1])
    print(f"Recommended departure: {best_time} (predicted travel time {best_pred:.2f} minutes)")
    print("\nAll candidates (time -> predicted minutes):")
    for t, p in candidates:
        print(f"{t} -> {p:.2f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--segment', required=True, help='road_segment identifier (match traffic data)')
    parser.add_argument('--date', required=True, help='YYYY-MM-DD')
    parser.add_argument('--start_time', required=True, help='HH:MM')
    parser.add_argument('--end_time', required=True, help='HH:MM')
    parser.add_argument('--window_minutes', type=int, default=15)
    args = parser.parse_args()
    recommend(args.segment, args.date, args.start_time, args.end_time, args.window_minutes)
