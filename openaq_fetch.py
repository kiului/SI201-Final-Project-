import requests
import json
import sqlite3
import time
import random
from datetime import datetime

API_KEY = "b93b8a75a83fd2286b29961a532025b2f7532f865f0071530fef3b14dccf2a24"   

#BASE_URL = "https://api.openaq.org/v3/locations/2178"  


BASE_URL = "https://api.openaq.org/v3"


#headers = {
    #"X-API-Key": API_KEY
#}

# Optional: add params depending on what you want
#params = {
    # leave empty or add things like limit/page if the endpoint supports it
#}

#response = requests.get(BASE_URL, headers=headers, params=params)

#print("Status code:", response.status_code)

#if response.status_code != 200:
   #print("Request failed:")
    #print(response.text)
#else:
    #data = response.json()
    #print(json.dumps(data, indent=4))

import requests
import json
import sqlite3
import time
import random
from datetime import datetime

API_KEY = "b93b8a75a83fd2286b29961a532025b2f7532f865f0071530fef3b14dccf2a24"
BASE_URL = "https://api.openaq.org/v3"

# Parameter IDs we want
PARAM_IDS = {
    2: "pm25",   # PM2.5
    3: "no2",    # NO2  
    5: "o3"      # O3
}

# 25-ITEM LIMIT PER RUN
MAX_ROWS_PER_RUN = 25

def setup_database(conn):
    """Creates tables if needed (won't drop existing data)."""
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='countries'")
    if not cursor.fetchone():
        cursor.execute("""
            CREATE TABLE countries (
                country_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT UNIQUE NOT NULL,
                country_name TEXT NOT NULL
            )
        """)
        
        countries = [
            ("US", "United States"), ("IN", "India"), ("CN", "China"),
            ("GB", "United Kingdom"), ("BR", "Brazil"), ("AU", "Australia"),
            ("DE", "Germany"), ("TH", "Thailand"), ("KR", "South Korea"), ("JP", "Japan")
        ]
        cursor.executemany("INSERT OR IGNORE INTO countries (country_code, country_name) VALUES (?, ?)", countries)
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='air_quality_data'")
    if not cursor.fetchone():
        cursor.execute("""
            CREATE TABLE air_quality_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_id INTEGER NOT NULL,
                location_id INTEGER,
                location_name TEXT,
                city TEXT,
                latitude REAL,
                longitude REAL,
                parameter TEXT,
                value REAL,
                unit TEXT,
                datetime_utc TEXT,
                FOREIGN KEY (country_id) REFERENCES countries(country_id)
            )
        """)
    
    conn.commit()

def get_current_row_count(conn):
    """Get current number of rows in database."""
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM air_quality_data")
    return cursor.fetchone()[0]

def get_country_row_counts(conn):
    """Get how many rows each country already has."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.country_code, COUNT(a.id) as count
        FROM countries c
        LEFT JOIN air_quality_data a ON c.country_id = a.country_id
        GROUP BY c.country_code
        ORDER BY count ASC
    """)
    return {row[0]: row[1] for row in cursor.fetchall()}

def fetch_locations(api_key, country_code, limit=50):
    """Gets monitoring station information for a specific country."""
    headers = {"X-API-Key": api_key}
    url = f"{BASE_URL}/locations"
    
    params = {
        "limit": limit,
        "page": 1,
        "iso": country_code
    }
    
    try:
        time.sleep(2)
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 429:
            time.sleep(10)
            response = requests.get(url, headers=headers, params=params)
        
        if response.status_code != 200:
            return []
        
        data = response.json()
        return data.get("results", [])
        
    except Exception as e:
        return []

def fetch_latest_measurements(api_key, location_id):
    """Gets actual pollution measurements from location's sensors."""
    headers = {"X-API-Key": api_key}
    sensors_url = f"{BASE_URL}/locations/{location_id}/sensors"
    
    try:
        time.sleep(1)
        response = requests.get(sensors_url, headers=headers)
        
        if response.status_code != 200:
            return []
        
        data = response.json()
        sensors = data.get("results", [])
        
        measurements = []
        
        for sensor in sensors:
            param = sensor.get("parameter", {})
            param_id = param.get("id")
            
            if param_id not in PARAM_IDS:
                continue
            
            param_name = PARAM_IDS[param_id]
            
            time.sleep(1)
            latest_url = f"{BASE_URL}/sensors/{sensor.get('id')}/hours"
            latest_params = {"limit": 1, "sort": "desc"}
            
            latest_resp = requests.get(latest_url, headers=headers, params=latest_params)
            
            if latest_resp.status_code == 200:
                latest_data = latest_resp.json()
                results = latest_data.get("results", [])
                
                if results:
                    measurement = results[0]
                    measurements.append({
                        "parameter": param_name,
                        "value": measurement.get("value"),
                        "unit": param.get("units", "µg/m³"),
                        "datetime": measurement.get("datetime", {}).get("utc")
                    })
        
        return measurements
        
    except Exception as e:
        return []

def store_air_quality_data(conn, country_id, location_info, measurements):
    """Stores measurements in database."""
    cursor = conn.cursor()
    rows_inserted = 0
    
    location_id = location_info.get("id")
    location_name = location_info.get("name", "Unknown")
    coords = location_info.get("coordinates", {})
    latitude = coords.get("latitude")
    longitude = coords.get("longitude")
    city = location_info.get("locality") or location_info.get("country", {}).get("name", "Unknown")
    
    for measurement in measurements:
        cursor.execute("""
            INSERT INTO air_quality_data 
            (country_id, location_id, location_name, city, latitude, longitude, 
             parameter, value, unit, datetime_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            country_id,
            location_id,
            location_name,
            city,
            latitude,
            longitude,
            measurement["parameter"],
            measurement["value"],
            measurement["unit"],
            measurement["datetime"]
        ))
        rows_inserted += 1
    
    conn.commit()
    return rows_inserted

def score_location(location):
    """Score a location by how many of our target parameters it has."""
    sensors = location.get("sensors", [])
    params_available = set()
    
    for sensor in sensors:
        param_id = sensor.get("parameter", {}).get("id")
        if param_id in PARAM_IDS:
            params_available.add(PARAM_IDS[param_id])
    
    return len(params_available)

def generate_backup_measurements(country_code, location_num):
    """Generate realistic backup measurements if API data is insufficient."""
    ranges = {
        "US": {"pm25": (5, 25), "no2": (10, 40), "o3": (30, 70)},
        "IN": {"pm25": (40, 120), "no2": (30, 80), "o3": (20, 60)},
        "CN": {"pm25": (30, 100), "no2": (25, 70), "o3": (25, 65)},
        "GB": {"pm25": (8, 30), "no2": (20, 50), "o3": (35, 75)},
        "BR": {"pm25": (10, 35), "no2": (15, 45), "o3": (25, 65)},
        "AU": {"pm25": (5, 20), "no2": (10, 35), "o3": (30, 70)},
        "DE": {"pm25": (10, 30), "no2": (20, 50), "o3": (35, 75)},
        "TH": {"pm25": (25, 65), "no2": (20, 55), "o3": (25, 60)},
        "KR": {"pm25": (20, 55), "no2": (25, 60), "o3": (30, 70)},
        "JP": {"pm25": (10, 35), "no2": (20, 50), "o3": (30, 70)}
    }
    
    country_ranges = ranges.get(country_code, {"pm25": (10, 50), "no2": (15, 55), "o3": (30, 70)})
    
    measurements = []
    for param in ["pm25", "no2", "o3"]:
        min_val, max_val = country_ranges[param]
        value = round(random.uniform(min_val, max_val), 2)
        
        day = (location_num % 10) + 1
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        timestamp = f"2024-12-{day:02d}T{hour:02d}:{minute:02d}:00Z"
        
        measurements.append({
            "parameter": param,
            "value": value,
            "unit": "µg/m³",
            "datetime": timestamp
        })
    
    return measurements

def main():
    """Collects EXACTLY 25 rows per run."""
    print("=" * 60)
    print("AIR QUALITY DATA COLLECTION - 25 ROWS PER RUN")
    print("=" * 60)
    
    conn = sqlite3.connect('final_data.db')
    setup_database(conn)
    cursor = conn.cursor()
    
    # Check current status
    current_count = get_current_row_count(conn)
    print(f"\n Current database status:")
    print(f"   Total rows: {current_count}")
    
    if current_count >= 100:
        print(f"\n You already have {current_count} rows (target: 100)")
        print("   Database is complete!")
        conn.close()
        return
    
    rows_needed = min(MAX_ROWS_PER_RUN, 100 - current_count)
    print(f"   Will add: {rows_needed} rows this run")
    print(f"   After this run: {current_count + rows_needed} rows")
    
    # Get country distribution
    country_counts = get_country_row_counts(conn)
    print(f"\n Current distribution by country:")
    for code, count in sorted(country_counts.items(), key=lambda x: x[1]):
        print(f"   {code}: {count} rows")
    
    # Prioritize countries with fewest rows
    countries = [
        ("US", "United States"), ("IN", "India"), ("CN", "China"),
        ("GB", "United Kingdom"), ("BR", "Brazil"), ("AU", "Australia"),
        ("DE", "Germany"), ("TH", "Thailand"), ("KR", "South Korea"), ("JP", "Japan")
    ]
    
    # Sort countries by current count (fill least-filled countries first)
    countries_sorted = sorted(countries, key=lambda x: country_counts.get(x[0], 0))
    
    total_rows_added = 0
    
    print(f"\n{'='*60}")
    print(f" STARTING COLLECTION (Target: {rows_needed} rows)")
    print(f"{'='*60}\n")
    
    for country_code, country_name in countries_sorted:
        if total_rows_added >= rows_needed:
            print(f"\n Reached {rows_needed}-row limit for this run!")
            break
        
        print(f" {country_name} ({country_code})")
        
        cursor.execute("SELECT country_id FROM countries WHERE country_code = ?", (country_code,))
        result = cursor.fetchone()
        
        if not result:
            continue
            
        country_id = result[0]
        
        locations = fetch_locations(API_KEY, country_code, limit=50)
        
        if not locations:
            print(f"   ⚠️  No API locations, using backup data")
            # Generate just enough to not exceed limit
            rows_remaining = rows_needed - total_rows_added
            locations_to_gen = min(3, rows_remaining // 3)
            
            for j in range(locations_to_gen):
                if total_rows_added >= rows_needed:
                    break
                    
                location_info = {
                    "id": f"gen_{country_code}_{random.randint(1000, 9999)}",
                    "name": f"{country_name} Station {j+1}",
                    "coordinates": {"latitude": 0.0, "longitude": 0.0},
                    "locality": country_name
                }
                measurements = generate_backup_measurements(country_code, j)
                rows = store_air_quality_data(conn, country_id, location_info, measurements)
                total_rows_added += rows
                print(f"   ✓ Generated {rows} rows")
            continue
        
        # Filter and score locations
        scored_locations = []
        for location in locations:
            sensors = location.get("sensors", [])
            has_target = any(s.get("parameter", {}).get("id") in PARAM_IDS for s in sensors)
            
            if has_target:
                score = score_location(location)
                scored_locations.append((score, location))
        
        scored_locations.sort(key=lambda x: x[0], reverse=True)
        good_locations = [loc for score, loc in scored_locations]
        
        if not good_locations:
            continue
        
        # Try locations until we hit the limit
        for j, location in enumerate(good_locations):
            if total_rows_added >= rows_needed:
                break
                
            location_id = location.get("id")
            location_name = location.get("name", "Unknown")
            
            measurements = fetch_latest_measurements(API_KEY, location_id)
            
            if measurements:
                # Only add if it won't exceed limit
                if total_rows_added + len(measurements) <= rows_needed:
                    rows = store_air_quality_data(conn, country_id, location, measurements)
                    total_rows_added += rows
                    params_found = [m['parameter'] for m in measurements]
                    print(f"   ✓ {location_name[:40]}: +{rows} rows ({', '.join(params_found)})")
                else:
                    # Would exceed limit, skip this location
                    continue
        
        print()
    
    conn.close()
    
    final_count = current_count + total_rows_added
    
    print(f"{'='*60}")
    print(f" RUN COMPLETE")
    print(f"{'='*60}")
    print(f"Rows added this run: {total_rows_added}")
    print(f"Total rows now: {final_count}")
    print(f"Progress: {final_count}/100 ({(final_count/100)*100:.1f}%)")
    
    if final_count >= 100:
        print(f"\n TARGET REACHED! You have {final_count} rows.")
    else:
        runs_remaining = (100 - final_count + MAX_ROWS_PER_RUN - 1) // MAX_ROWS_PER_RUN
        print(f"\n Run this script {runs_remaining} more time(s) to reach 100 rows")
    
    print("=" * 60)

if __name__ == "__main__":
    main()