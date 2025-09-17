import psycopg2
import pandas as pd

# --- Database connection ---
conn = psycopg2.connect(
    dbname="flood_project",
    user="postgres",
    password="admin123",
    host="localhost",
    port="5432"
)

def classify_event(conn, station_id: str, date_str: str, duration_min: int):
    """
    Generalized classifier:
    - Pull rainfall for station/date (rainfall_since_9am_mm).
    - Compare vs ifd_lookup for given duration.
    - If duration not available, warn.
    """
    # 1. Get rainfall
    sql_rain = """
        SELECT obs_time, rainfall_since_9am_mm
        FROM rainfall_obs
        WHERE station_id = %s
          AND obs_time::date = %s::date
        LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql_rain, (station_id, date_str))
        row = cur.fetchone()
    if not row or row[1] is None:
        return f"No rainfall data for {date_str}"

    obs_time, rain_mm = row
    print(f"🌧 {station_id} on {obs_time.date()} → {rain_mm} mm")

    # 2. Load IFD for given duration
    df_ifd = pd.read_sql(
        "SELECT ari_years, depth_mm::float AS depth_mm "
        "FROM ifd_lookup WHERE duration_minutes = %s ORDER BY ari_years",
        conn,
        params=[duration_min]
    )
    if df_ifd.empty:
        return f"No IFD curve for {duration_min} min in database."

    # 3. Bracket rainfall against curve
    lower = df_ifd[df_ifd["depth_mm"] <= rain_mm].tail(1)
    higher = df_ifd[df_ifd["depth_mm"] >= rain_mm].head(1)

    if not lower.empty and not higher.empty and int(lower.iloc[0]["ari_years"]) != int(higher.iloc[0]["ari_years"]):
        lo = int(lower.iloc[0]["ari_years"])
        hi = int(higher.iloc[0]["ari_years"])
        return f"{rain_mm} mm = between 1-in-{lo}yr and 1-in-{hi}yr ARI ({duration_min}min)"
    elif not lower.empty and higher.empty:
        lo = int(lower.iloc[0]["ari_years"])
        return f"{rain_mm} mm = exceeds 1-in-{lo}yr ARI ({duration_min}min)"
    elif lower.empty and not higher.empty:
        hi = int(higher.iloc[0]["ari_years"])
        return f"{rain_mm} mm = below 1-in-{hi}yr ARI ({duration_min}min)"
    else:
        return "Classification indeterminate (check IFD table)."


# --- Demo ---
print(classify_event(conn, "86077", "2025-09-17", duration_min=30))   # Sub-daily (30min)
print(classify_event(conn, "86077", "2025-09-17", duration_min=60))   # 1 hour
print(classify_event(conn, "86077", "2025-09-17", duration_min=1440)) # Daily

conn.close()
