"""
Train a regression model (RandomForest) to predict travel_time_min.
- Loads features.csv
- Splits into train/test
- Trains model and serializes to models/trained_model.joblib
- Prints MAE/RMSE on holdout set
"""

import pandas as pd
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib
import numpy as np

FEAT_CSV = Path(__file__).resolve().parents[1] / "data" / "features.csv"
MODEL_PATH = Path(__file__).resolve().parents[1] / "models" / "trained_model.joblib"

def train():
    df = pd.read_csv(FEAT_CSV, parse_dates=['ts_hour'])
    # select features (adjust as needed)
    feature_cols = ['avg_speed','prev_avg_speed','vehicle_count','hour','dayofweek','weather_severity','temp_c','precipitation_mm','visibility_m']
    # ensure columns exist (some may be missing, fill with zeros)
    for c in feature_cols:
        if c not in df.columns:
            df[c] = 0.0

    X = df[feature_cols].fillna(0.0)
    y = df['travel_time_min']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    print(f"MAE: {mae:.3f} minutes  RMSE: {rmse:.3f} minutes")

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({'model': model, 'feature_cols': feature_cols}, MODEL_PATH)
    print(f"Saved trained model to {MODEL_PATH}")

if __name__ == "__main__":
    train()
