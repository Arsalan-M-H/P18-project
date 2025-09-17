
# Path to BoM CDO CSV
csv_file = "C://Users//alwaz//Downloads//IDCJAC0009_086077_1800//IDCJAC0009_086077_1800_Data.csv"

import psycopg2
import pandas as pd
from datetime import datetime, timezone, timedelta

# Database connection
conn = psycopg2.connect(
    dbname="flood_project",
    user="postgres",
    password="admin123",
    host="localhost",
    port="5432"
)
cur = conn.cursor()


# Load CSV
df = pd.read_csv(csv_file, comment="#")

# Rename columns for easier access
df.columns = [c.strip() for c in df.columns]

station_id = str(df["Bureau of Meteorology station number"].iloc[0])
station_name = "Frankston"   # you can extract from header if needed

print(f"Loading data for {station_name} (ID {station_id})")

for _, row in df.iterrows():
    try:
        y, m, d = int(row["Year"]), int(row["Month"]), int(row["Day"])
        obs_time = datetime(y, m, d, 9, tzinfo=timezone(timedelta(hours=10)))

        rain_str = row["Rainfall amount (millimetres)"]

        # Handle missing/trace
        if pd.isna(rain_str):
            continue
        if str(rain_str).strip().lower() == "trace":
            rain_val = 0.1
        else:
            rain_val = float(rain_str)

        cur.execute("""
            INSERT INTO rainfall_obs
            (station_id, station_name, obs_time, rainfall_since_9am_mm, data_source)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (station_id, obs_time) DO NOTHING;
        """, (station_id, station_name, obs_time, rain_val, "BoM CDO (daily)"))

    except Exception as e:
        print("⚠️ Skipping row:", row, "Error:", e)

conn.commit()
print(f"✅ Historical data loaded for {station_name} ({station_id})")

cur.close()
conn.close()
