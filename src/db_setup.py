"""
Creates an SQLite database and the required tables.

- traffic_records: main traffic observations with timestamp, segment id, avg_speed, travel_time_minutes, vehicle_count
- weather: weather observations joined by timestamp
- events: construction/incidents
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "db" / "nj_turnpike.db"

def create_tables(conn):
    c = conn.cursor()
    # traffic_records table
    c.execute("""
    CREATE TABLE IF NOT EXISTS traffic_records (
        id INTEGER PRIMARY KEY,
        timestamp TEXT NOT NULL,
        road_segment TEXT NOT NULL,
        avg_speed REAL,
        travel_time_min REAL,
        vehicle_count INTEGER
    );
    """)
    # weather table
    c.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY,
        timestamp TEXT NOT NULL,
        temp_c REAL,
        precipitation_mm REAL,
        visibility_m REAL,
        weather_main TEXT
    );
    """)
    # events table
    c.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY,
        timestamp TEXT NOT NULL,
        road_segment TEXT,
        event_type TEXT,
        severity INTEGER,
        description TEXT
    );
    """)
    conn.commit()

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)
    conn.close()
    print(f"Database and tables created at {DB_PATH}")

if __name__ == "__main__":
    main()
