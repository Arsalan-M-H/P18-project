import requests
import psycopg2
import datetime
from datetime import timezone, timedelta

# Database connection
conn = psycopg2.connect(
    dbname="flood_project",
    user="postgres",
    password="admin123",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

url = "http://www.bom.gov.au/fwo/IDV60701/IDV60701.94870.json"
headers = {"User-Agent": "Mozilla/5.0"}

try:
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    data = response.json()
except Exception as e:
    print("Error fetching data:", e)
    exit()

# Take the latest observation
obs = data["observations"]["data"][0]

station_id = str(obs.get("wmo"))
station_name = obs.get("name")

# Convert UTC string → datetime → AEST
utc_time = datetime.datetime.strptime(obs["aifstime_utc"], "%Y%m%d%H%M%S")
utc_time = utc_time.replace(tzinfo=timezone.utc)
obs_time = utc_time.astimezone(timezone(timedelta(hours=10)))  # AEST

temperature = obs.get("air_temp")
pressure = obs.get("press")
rain_trace = float(obs.get("rain_trace", 0.0))
wind_dir = obs.get("wind_dir")
wind_speed = obs.get("wind_spd_kt")
wind_gust = obs.get("gust_kt")
lat = obs.get("lat")
lon = obs.get("lon")

# 🔹 Compute rain_increment (difference vs last record)
cur.execute("""
    SELECT rainfall_since_9am 
    FROM rainfall_live 
    WHERE station_id = %s 
    ORDER BY obs_time DESC LIMIT 1
""", (station_id,))
last_row = cur.fetchone()

rain_increment = None
if last_row and last_row[0] is not None:
    prev_val = float(last_row[0])
    rain_increment = rain_trace - prev_val
    if rain_increment < 0:
        # Reset at 9am → take current value
        rain_increment = rain_trace

print(f"{station_name} at {obs_time} AEST → {rain_trace} mm since 9am (+{rain_increment} mm)")

# UPSERT → insert new record, skip if duplicate (station_id + obs_time already exists)
cur.execute("""
    INSERT INTO rainfall_live 
    (station_id, station_name, obs_time, temperature, pressure, rainfall_since_9am, rain_increment, wind_dir, wind_speed, wind_gust, lat, lon)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (station_id, obs_time) DO NOTHING;
""", (station_id, station_name, obs_time, temperature, pressure, rain_trace, rain_increment, wind_dir, wind_speed, wind_gust, lat, lon))

conn.commit()
print("✅ Data inserted successfully or skipped (duplicate).")

cur.close()
conn.close()
