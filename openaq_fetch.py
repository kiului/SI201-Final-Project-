import requests
import json
import sqlite3
import time
from collections import defaultdict

API_KEY = "b93b8a75a83fd2286b29961a532025b2f7532f865f0071530fef3b14dccf2a24"   
BASE_URL = "https://api.openaq.org/v3"

# Fixed: Use the countries you actually want (JP, KR, TH instead of FR, ES, PL)
REQUIRED_COUNTRIES = [
    ("US", "United States"),
    ("IN", "India"),
    ("CN", "China"),
    ("GB", "United Kingdom"),
    ("BR", "Brazil"),
    ("AU", "Australia"),
    ("DE", "Germany"),
    ("JP", "Japan"),        # Changed from FR
    ("KR", "South Korea"),  # Changed from ES
    ("TH", "Thailand")      # Changed from PL
]

PARAM_IDS = {
    2: "pm25",
    3: "no2",
    5: "o3"
}

MIN_DELAY = 1.0  # Increased from 0.5 to avoid rate limits
MAX_ROWS_PER_RUN = 25
RETRY_DELAY = 15  # Wait longer on rate limit
MAX_RETRIES = 3  # Maximum retry attempts
session = requests.Session()
session.headers.update({"X-API-Key": API_KEY})

def reset_database(conn, auto_confirm=False):
    """Clear all air quality data to start fresh."""
    cursor = conn.cursor()
    
    if not auto_confirm:
        print("\nWARNING: This will delete all air quality data!")
        try:
            response = input("Type 'YES' to confirm reset: ")
        except (EOFError, OSError):
            print("Cannot get input in this environment.")
            print("To reset: delete 'final_data.db' file or call reset_database(conn, auto_confirm=True)")
            return False
        
        if response != "YES":
            print("Reset cancelled.\n")
            return False
    
    cursor.execute("DELETE FROM air_quality_data")
    conn.commit()
    print("Database reset complete!\n")
    return True

def setup_database(conn):
    """Creates tables - ONE ROW PER LOCATION with all measurements."""
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
    
    # Always insert or update all required countries
    cursor.executemany(
        "INSERT OR IGNORE INTO countries (country_code, country_name) VALUES (?, ?)", 
        REQUIRED_COUNTRIES
    )
    
    # Check if air_quality_data exists and drop if it has old structure
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='air_quality_data'")
    if cursor.fetchone():
        cursor.execute("PRAGMA table_info(air_quality_data)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Check if we need to recreate the table (old structure or has unwanted columns)
        if 'parameter' in columns or 'datetime_utc' in columns or 'city' in columns:
            print("⚠️  Detected old table structure. Recreating table...")
            
            # Save existing data if we're just removing columns
            if 'parameter' not in columns:
                cursor.execute("""
                    CREATE TEMPORARY TABLE air_quality_backup AS
                    SELECT country_id, location_id, location_name, latitude, longitude,
                           pm25_value, no2_value, o3_value
                    FROM air_quality_data
                """)
                has_backup = True
            else:
                has_backup = False
            
            cursor.execute("DROP TABLE air_quality_data")
            
            # Recreate with new structure
            cursor.execute("""
                CREATE TABLE air_quality_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_id INTEGER NOT NULL,
                    location_id INTEGER,
                    location_name TEXT,
                    latitude REAL,
                    longitude REAL,
                    pm25_value REAL,
                    no2_value REAL,
                    o3_value REAL,
                    FOREIGN KEY (country_id) REFERENCES countries(country_id)
                )
            """)
            
            # Restore data if we had a backup
            if has_backup:
                cursor.execute("""
                    INSERT INTO air_quality_data 
                    (country_id, location_id, location_name, latitude, longitude,
                     pm25_value, no2_value, o3_value)
                    SELECT country_id, location_id, location_name, latitude, longitude,
                           pm25_value, no2_value, o3_value
                    FROM air_quality_backup
                """)
                cursor.execute("DROP TABLE air_quality_backup")
                print(f"✓ Table recreated and data preserved")
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='air_quality_data'")
    if not cursor.fetchone():
        cursor.execute("""
            CREATE TABLE air_quality_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_id INTEGER NOT NULL,
                location_id INTEGER,
                location_name TEXT,
                latitude REAL,
                longitude REAL,
                pm25_value REAL,
                no2_value REAL,
                o3_value REAL,
                FOREIGN KEY (country_id) REFERENCES countries(country_id)
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_country_id 
            ON air_quality_data(country_id)
        """)
    
    conn.commit()

def get_current_row_count(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM air_quality_data")
    return cursor.fetchone()[0]

def get_country_row_counts(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.country_code, COUNT(a.id) as count
        FROM countries c
        LEFT JOIN air_quality_data a ON c.country_id = a.country_id
        GROUP BY c.country_code
        ORDER BY count ASC
    """)
    return {row[0]: row[1] for row in cursor.fetchall()}

def fetch_locations(country_code, limit=50):
    """Gets monitoring stations with retry logic."""
    url = f"{BASE_URL}/locations"
    params = {
        "limit": limit,
        "page": 1,
        "iso": country_code
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(MIN_DELAY)
            response = session.get(url, params=params, timeout=15)
            
            if response.status_code == 429:
                wait_time = RETRY_DELAY * (attempt + 1)
                print(f"    Rate limited (attempt {attempt + 1}/{MAX_RETRIES}), waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            
            if response.status_code == 200:
                return response.json().get("results", [])
            
            if response.status_code >= 400:
                print(f"    API error {response.status_code}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue
            
            return []
            
        except Exception as e:
            print(f"    Error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return []
    
    print(f"    Failed after {MAX_RETRIES} attempts")
    return []

def fetch_latest_measurements(location_id):
    """Gets measurements and returns them as a dict by parameter."""
    for attempt in range(MAX_RETRIES):
        try:
            url = f"{BASE_URL}/locations/{location_id}/latest"
            time.sleep(MIN_DELAY)
            response = session.get(url, timeout=15)
            
            if response.status_code == 429:
                wait_time = RETRY_DELAY
                if attempt < MAX_RETRIES - 1:
                    time.sleep(wait_time)
                    continue
                return {}
            
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                
                measurements = {}
                datetime_utc = None
                
                for result in results:
                    param = result.get("parameter", {})
                    param_id = param.get("id")
                    
                    if param_id in PARAM_IDS:
                        param_name = PARAM_IDS[param_id]
                        measurements[param_name] = result.get("value")
                        if not datetime_utc:
                            datetime_utc = result.get("datetime", {}).get("utc")
                
                if measurements:
                    measurements['datetime'] = datetime_utc
                    return measurements
            
            # Try fallback on first attempt if main method fails
            if attempt == 0:
                return fetch_measurements_fallback(location_id)
            
            return {}
            
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return {}
    
    return {}

def fetch_measurements_fallback(location_id):
    """Fallback method using sensors endpoint with retry logic."""
    try:
        sensors_url = f"{BASE_URL}/locations/{location_id}/sensors"
        time.sleep(MIN_DELAY)
        response = session.get(sensors_url, timeout=15)
        
        if response.status_code != 200:
            return {}
        
        sensors = response.json().get("results", [])
        measurements = {}
        datetime_utc = None
        
        relevant_sensors = [s for s in sensors 
                          if s.get("parameter", {}).get("id") in PARAM_IDS]
        
        for sensor in relevant_sensors:
            param_id = sensor.get("parameter", {}).get("id")
            param_name = PARAM_IDS[param_id]
            
            time.sleep(MIN_DELAY)
            latest_url = f"{BASE_URL}/sensors/{sensor.get('id')}/hours"
            
            try:
                latest_resp = session.get(latest_url, 
                                         params={"limit": 1, "sort": "desc"}, 
                                         timeout=15)
                
                if latest_resp.status_code == 200:
                    results = latest_resp.json().get("results", [])
                    if results:
                        measurement = results[0]
                        measurements[param_name] = measurement.get("value")
                        if not datetime_utc:
                            datetime_utc = measurement.get("datetime", {}).get("utc")
            except Exception:
                continue
        
        if measurements:
            measurements['datetime'] = datetime_utc
        
        return measurements
        
    except Exception as e:
        return {}

def store_air_quality_data(conn, country_id, location_info, measurements):
    """Stores ONE ROW per location with all measurements."""
    cursor = conn.cursor()
    
    location_id = location_info.get("id")
    location_name = location_info.get("name", "Unknown")
    coords = location_info.get("coordinates", {})
    latitude = coords.get("latitude")
    longitude = coords.get("longitude")
    
    cursor.execute("""
        INSERT INTO air_quality_data 
        (country_id, location_id, location_name, latitude, longitude, 
         pm25_value, no2_value, o3_value)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        country_id,
        location_id,
        location_name,
        latitude,
        longitude,
        measurements.get('pm25'),
        measurements.get('no2'),
        measurements.get('o3')
    ))
    
    conn.commit()
    return 1

def score_location(location):
    """Score locations by parameter availability."""
    sensors = location.get("sensors", [])
    params_available = set()
    
    for sensor in sensors:
        param_id = sensor.get("parameter", {}).get("id")
        if param_id in PARAM_IDS:
            params_available.add(PARAM_IDS[param_id])
    
    return len(params_available)

def print_collection_summary(country_stats):
    """Print detailed statistics for this run."""
    print(f"\n{'='*60}")
    print(f"COLLECTION SUMMARY - THIS RUN")
    print(f"{'='*60}")
    
    for country_code, stats in country_stats.items():
        country_name = dict(REQUIRED_COUNTRIES).get(country_code, country_code)
        total = stats['locations']
        
        if total > 0:
            print(f"\n{country_name} ({country_code}): {total} locations")
            print(f"   PM2.5: {stats['pm25']} locations with data")
            print(f"   NO2:   {stats['no2']} locations with data")
            print(f"   O3:    {stats['o3']} locations with data")

def main():
    """Collects up to 25 locations per run, 10 per country max."""
    print("=" * 60)
    print("AIR QUALITY DATA COLLECTION - BALANCED")
    print("Collecting 10 locations from each of 10 countries")
    print(f"Limit: {MAX_ROWS_PER_RUN} locations per run")
    print("Target: 100 total locations (10 per country)")
    print("=" * 60)
    
    conn = sqlite3.connect('final_data.db')
    setup_database(conn)
    
    current_count = get_current_row_count(conn)
    print(f"\nCurrent database status:")
    print(f"   Total rows (locations): {current_count}")
    
    if current_count > 0:
        print(f"\nYou already have {current_count} rows!")
        print("   Continuing with existing data...")
        print("   (To reset, call reset_database(conn) manually or delete the .db file)\n")
    
    country_counts = get_country_row_counts(conn)
    print(f"\nCurrent distribution by country:")
    for code, count in sorted(country_counts.items()):
        country_name = dict(REQUIRED_COUNTRIES).get(code, code)
        print(f"   {country_name:20s} ({code}): {count:3d}/10 locations")
    
    total_rows_added = 0
    country_stats = defaultdict(lambda: {'locations': 0, 'pm25': 0, 'no2': 0, 'o3': 0})
    
    print(f"\n{'='*60}")
    print(f"STARTING COLLECTION (max {MAX_ROWS_PER_RUN} locations this run)")
    print(f"{'='*60}\n")
    
    start_time = time.time()
    
    # Process each country and collect up to 10 locations, respecting 25 row limit
    for country_code, country_name in REQUIRED_COUNTRIES:
        # Check if we've hit the per-run limit
        if total_rows_added >= MAX_ROWS_PER_RUN:
            print(f"\n⚠️  Reached {MAX_ROWS_PER_RUN} row limit for this run.")
            print(f"   Run the script again to continue collecting data.")
            break
        
        print(f"{'='*60}")
        print(f"{country_name} ({country_code}) - Target: 10 locations")
        print(f"{'='*60}")
        
        cursor = conn.cursor()
        cursor.execute("SELECT country_id FROM countries WHERE country_code = ?", 
                      (country_code,))
        result = cursor.fetchone()
        if not result:
            print(f"   ⚠️  Country not in database, skipping...\n")
            continue
        country_id = result[0]
        
        # Check how many we already have
        cursor.execute("SELECT COUNT(*) FROM air_quality_data WHERE country_id = ?", 
                      (country_id,))
        existing_count = cursor.fetchone()[0]
        
        if existing_count >= 10:
            print(f"   ✓ Already have {existing_count} locations, skipping\n")
            continue
        
        needed = 10 - existing_count
        # Limit by both country need and remaining run capacity
        can_collect = min(needed, MAX_ROWS_PER_RUN - total_rows_added)
        print(f"   Need {needed} more locations (have {existing_count})")
        print(f"   Will collect up to {can_collect} this run (run limit: {MAX_ROWS_PER_RUN - total_rows_added} remaining)")
        
        locations = fetch_locations(country_code, limit=50)
        
        if not locations:
            print(f"   ❌ No API locations found\n")
            continue
        
        # Score and sort locations by data availability
        scored_locations = [(score_location(loc), loc) for loc in locations]
        scored_locations.sort(key=lambda x: x[0], reverse=True)
        good_locations = [loc for score, loc in scored_locations if score > 0]
        
        if not good_locations:
            print(f"   ❌ No locations with target parameters\n")
            continue
        
        print(f"   Found {len(good_locations)} locations with data")
        
        locations_added = 0
        
        for location in good_locations:
            if locations_added >= can_collect:
                break
            
            location_id = location.get("id")
            location_name = location.get("name", "Unknown")
            
            measurements = fetch_latest_measurements(location_id)
            
            if measurements:
                store_air_quality_data(conn, country_id, location, measurements)
                locations_added += 1
                total_rows_added += 1
                
                country_stats[country_code]['locations'] += 1
                if measurements.get('pm25') is not None:
                    country_stats[country_code]['pm25'] += 1
                if measurements.get('no2') is not None:
                    country_stats[country_code]['no2'] += 1
                if measurements.get('o3') is not None:
                    country_stats[country_code]['o3'] += 1
                
                params = []
                if measurements.get('pm25') is not None:
                    params.append('PM2.5')
                if measurements.get('no2') is not None:
                    params.append('NO2')
                if measurements.get('o3') is not None:
                    params.append('O3')
                
                print(f"   [{locations_added:2d}/{can_collect:2d}] {location_name[:40]:40s} [{', '.join(params)}]")
                
                # Check if we've hit the run limit
                if total_rows_added >= MAX_ROWS_PER_RUN:
                    print(f"\n   ⚠️  Reached {MAX_ROWS_PER_RUN} row limit for this run")
                    break
        
        print(f"   ✓ Collected {locations_added} locations for {country_name}\n")
    
    elapsed_time = time.time() - start_time
    final_count = current_count + total_rows_added
    
    print_collection_summary(country_stats)
    
    print(f"\n{'='*60}")
    print(f"COLLECTION COMPLETE")
    print(f"{'='*60}")
    print(f"Locations added this run: {total_rows_added}/{MAX_ROWS_PER_RUN}")
    print(f"Total locations in DB: {final_count}/100")
    print(f"Progress: {(final_count/100)*100:.1f}%")
    print(f"Time elapsed: {elapsed_time:.1f} seconds")
    
    # Show final distribution
    final_counts = get_country_row_counts(conn)
    print(f"\nFinal distribution by country:")
    for code, count in sorted(final_counts.items()):
        country_name = dict(REQUIRED_COUNTRIES).get(code, code)
        status = "✓ COMPLETE" if count >= 10 else f"({count}/10)"
        print(f"   {country_name:20s} ({code}): {count:3d} locations {status}")
    
    if final_count >= 100:
        print(f"\n🎉 TARGET REACHED! Database complete with 100 locations.")
    elif total_rows_added >= MAX_ROWS_PER_RUN:
        print(f"\n⏸️  Run limit reached. Run the script again to continue.")
    
    print("=" * 60)
    
    conn.close()

if __name__ == "__main__":
    main()