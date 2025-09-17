import psycopg2
import math

# AEP → ARI conversion
def aep_to_ari(aep_percent: float) -> float:
    aep = aep_percent / 100.0
    return 1.0 / (-math.log(1.0 - aep))

# Example subset of your pasted data (add more rows as needed)
ifd_data = [
    # duration_min, {aep%: depth_mm}
    (30, {63.2: 12.2, 50: 13.6, 20: 18.2, 10: 21.7, 5: 25.3, 2: 30.5, 1: 34.8}),
    (60, {63.2: 15.7, 50: 17.4, 20: 23.1, 10: 27.2, 5: 31.5, 2: 37.4, 1: 42.3}),
    (120, {63.2: 19.9, 50: 21.9, 20: 28.6, 10: 33.5, 5: 38.4, 2: 45.1, 1: 50.4}),
    # add the rest...
]

conn = psycopg2.connect(
    dbname="flood_project",
    user="postgres",
    password="admin123",
    host="localhost",
    port=5432
)
cur = conn.cursor()

for dur, aep_dict in ifd_data:
    for aep, depth in aep_dict.items():
        ari = round(aep_to_ari(aep), 2)
        cur.execute("""
            INSERT INTO ifd_lookup (lat, lon, duration_minutes, ari_years, depth_mm, aep_percent, source)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (lat, lon, duration_minutes, ari_years)
            DO UPDATE SET depth_mm = EXCLUDED.depth_mm, aep_percent = EXCLUDED.aep_percent;
        """, (-38.15, 145.12, dur, ari, depth, aep, "BoM Table (manual)"))

conn.commit()
cur.close()
conn.close()
print("✅ IFD table populated from manual AEP data")
