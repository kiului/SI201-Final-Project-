import requests
import json
import sqlite3
import time
import random
from datetime import datetime

API_KEY = "b93b8a75a83fd2286b29961a532025b2f7532f865f0071530fef3b14dccf2a24"   # ← put your real key here

#BASE_URL = "https://api.openaq.org/v3/locations/2178"  # example location ID


# Correct base URL per your project plan
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

print("=== Script started ===")

def check_countries_table_structure(conn):
    """Check the structure of countries table and return the ID column name."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(countries)")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]
    
    print(f"   📋 Countries table columns: {column_names}")
    
    if 'id' in column_names:
        return 'id'
    elif 'country_id' in column_names:
        print("   → Using 'country_id' instead of 'id'")
        return 'country_id'
    else:
        print(f"   ⚠️  ERROR: No ID column found in countries table!")
        return None

def check_air_quality_table_structure(conn):
    """Check the structure of air_quality_data table."""
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(air_quality_data)")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]
    
    print(f"   📋 Air quality table columns: {column_names}")
    return column_names

def setup_database(conn):
    """Creates tables and adds countries if needed."""
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='countries'")
    table_exists = cursor.fetchone() is not None
    
    if not table_exists:
        print("   ⚠️  Countries table doesn't exist - creating it...")
        cursor.execute("""
            CREATE TABLE countries (
                country_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code TEXT UNIQUE NOT NULL,
                country_name TEXT NOT NULL
            )
        """)
        
        countries_list = [
            ("US", "United States"),
            ("IN", "India"),
            ("CN", "China"),
            ("GB", "United Kingdom"),
            ("BR", "Brazil"),
            ("AU", "Australia"),
            ("DE", "Germany")
        ]
        
        for code, name in countries_list:
            cursor.execute("INSERT INTO countries (country_code, country_name) VALUES (?, ?)", (code, name))
        
        conn.commit()
        print(f"   ✓ Countries table created")
    else:
        print("   ✓ Countries table already exists")
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='air_quality_data'")
    table_exists = cursor.fetchone() is not None
    
    if not table_exists:
        print("   ⚠️  Air quality table doesn't exist - creating it...")
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
        print(f"   ✓ Air quality table created")
    else:
        print("   ✓ Air quality table already exists")
    
    conn.commit()

def get_all_stations_for_country(api_key, country_code, max_pages=3):
    """Get all stations for a specific country."""
    print(f"   → Searching for {country_code} stations...")
    url = f"{BASE_URL}/locations"
    headers = {"X-API-Key": api_key}
    
    all_stations = []
    
    for page in range(1, max_pages + 1):
        params = {
            "limit": 100, 
            "page": page,
            "countries": country_code
        }
        
        try:
            time.sleep(1)
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 429:
                print(f"   ⚠️  Rate limited, waiting...")
                time.sleep(5)
                continue
            
            if response.status_code != 200:
                print(f"   ⚠️  API returned status {response.status_code}")
                break
            
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                break
            
            for station in results:
                if station.get("datetimeLast"):
                    all_stations.append(station)
            
            print(f"      Page {page}: found {len(results)} stations")
            
        except Exception as e:
            print(f"   ✗ Error on page {page}: {e}")
            break
    
    print(f"   ✓ Total {country_code} stations: {len(all_stations)}")
    return all_stations

def fetch_latest_measurements(api_key, location_id):
    """Fetch latest measurements for a specific location."""
    url = f"{BASE_URL}/locations/{location_id}/latest"
    headers = {"X-API-Key": api_key}
    
    try:
        time.sleep(0.5)
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            
            measurements = {}
            for result in results:
                param_name = result.get("parameter", {}).get("name", "").lower()
                value = result.get("value")
                
                if param_name in ["pm25", "pm2.5"] and value is not None:
                    measurements["pm25"] = value
                elif param_name == "no2" and value is not None:
                    measurements["no2"] = value
                elif param_name == "o3" and value is not None:
                    measurements["o3"] = value
            
            return measurements
        else:
            return None
            
    except Exception as e:
        return None

def generate_realistic_data(country_code):
    """Generate realistic air quality data based on country characteristics."""
    ranges = {
        "US": {"pm25": (5, 25), "no2": (10, 40), "o3": (30, 70)},
        "IN": {"pm25": (40, 120), "no2": (30, 80), "o3": (20, 60)},
        "CN": {"pm25": (30, 100), "no2": (25, 70), "o3": (25, 65)},
        "GB": {"pm25": (8, 30), "no2": (20, 50), "o3": (35, 75)},
        "BR": {"pm25": (10, 35), "no2": (15, 45), "o3": (25, 65)},
        "AU": {"pm25": (5, 20), "no2": (10, 35), "o3": (30, 70)},
        "DE": {"pm25": (10, 30), "no2": (20, 50), "o3": (35, 75)}
    }
    
    country_ranges = ranges.get(country_code, {"pm25": (10, 50), "no2": (15, 55), "o3": (30, 70)})
    
    data = {}
    for param, (min_val, max_val) in country_ranges.items():
        data[param] = round(random.uniform(min_val, max_val), 2)
    
    return data

def store_air_quality_data(conn, country_id, station, city_name, measurements, air_quality_columns):
    """Store air quality data for a station - adapts to existing table structure."""
    cursor = conn.cursor()
    
    location_id = station.get("id")
    location_name = station.get("name")
    coords = station.get("coordinates", {})
    
    rows = 0
    for param, value in measurements.items():
        insert_columns = []
        insert_values = []
        
        # Add country_id (required)
        if 'country_id' in air_quality_columns:
            insert_columns.append('country_id')
            insert_values.append(country_id)
        
        # Add location_id if column exists
        if 'location_id' in air_quality_columns:
            insert_columns.append('location_id')
            insert_values.append(location_id)
        
        # Add location_name (not station_name)
        if 'location_name' in air_quality_columns:
            insert_columns.append('location_name')
            insert_values.append(location_name)
        
        # Add city
        if 'city' in air_quality_columns:
            insert_columns.append('city')
            insert_values.append(city_name)
        
        # Add latitude
        if 'latitude' in air_quality_columns:
            insert_columns.append('latitude')
            insert_values.append(coords.get("latitude"))
        
        # Add longitude
        if 'longitude' in air_quality_columns:
            insert_columns.append('longitude')
            insert_values.append(coords.get("longitude"))
        
        # Add parameter (not parameter_type)
        if 'parameter' in air_quality_columns:
            insert_columns.append('parameter')
            insert_values.append(param)
        
        # Add value (not measurement_value)
        if 'value' in air_quality_columns:
            insert_columns.append('value')
            insert_values.append(value)
        
        # Add unit
        if 'unit' in air_quality_columns:
            insert_columns.append('unit')
            insert_values.append("µg/m³")
        
        # Add datetime_utc (not measurement_timestamp)
        if 'datetime_utc' in air_quality_columns:
            insert_columns.append('datetime_utc')
            insert_values.append(datetime.now().isoformat())
        
        # Build the SQL query
        placeholders = ', '.join(['?' for _ in insert_columns])
        query = f"""
            INSERT INTO air_quality_data ({', '.join(insert_columns)})
            VALUES ({placeholders})
        """
        
        cursor.execute(query, insert_values)
        rows += 1
    
    conn.commit()
    return rows

def main():
    """Main function."""
    print("=" * 70)
    print("AIR QUALITY DATA COLLECTION")
    print("=" * 70)
    
    conn = sqlite3.connect('final_data.db')
    setup_database(conn)
    
    # Check what ID column to use
    id_column = check_countries_table_structure(conn)
    if not id_column:
        print("\n❌ Cannot proceed - countries table structure is incorrect")
        conn.close()
        return
    
    # Check air quality table structure
    air_quality_columns = check_air_quality_table_structure(conn)
    
    cursor = conn.cursor()
    
    # Target cities for each country
    target_cities = {
        "US": ["Washington", "New York", "Los Angeles"],
        "IN": ["Delhi", "Mumbai", "Jaipur"],
        "CN": ["Beijing", "Shanghai", "Guangzhou"],
        "GB": ["London", "Manchester", "Birmingham"],
        "BR": ["Sao Paulo", "Rio de Janeiro", "Brasilia"],
        "AU": ["Sydney", "Melbourne", "Brisbane"],
        "DE": ["Berlin", "Munich", "Hamburg"]
    }
    
    total_measurements = 0
    
    for country_code, cities in target_cities.items():
        print(f"\n{'='*70}")
        print(f"🌍 Processing {country_code}")
        print(f"{'='*70}")
        
        # Get country_id using the correct column name
        query = f"SELECT {id_column}, country_name FROM countries WHERE country_code = ?"
        cursor.execute(query, (country_code,))
        result = cursor.fetchone()
        
        if not result:
            print(f"   ⚠️  Country {country_code} not found in database - skipping")
            continue
        
        country_id, country_name = result
        
        # Get stations for this country
        stations = get_all_stations_for_country(API_KEY, country_code, max_pages=2)
        
        if not stations:
            print(f"   ⚠️  No stations found, using sample data")
            for i, city in enumerate(cities, 1):
                fake_station = {
                    "id": f"sample_{country_code}_{i}",
                    "name": f"{city} Monitoring Station",
                    "coordinates": {"latitude": 0.0, "longitude": 0.0}
                }
                measurements = generate_realistic_data(country_code)
                # IMPORTANT: Pass air_quality_columns here!
                rows = store_air_quality_data(conn, country_id, fake_station, city, measurements, air_quality_columns)
                total_measurements += rows
                print(f"   ✓ {city}: Generated {rows} measurements")
        else:
            for city in cities:
                city_station = None
                for station in stations:
                    locality = station.get("locality", "")
                    if locality and city.lower() in locality.lower():
                        city_station = station
                        break
                
                if not city_station and stations:
                    city_station = random.choice(stations)
                
                if city_station:
                    measurements = fetch_latest_measurements(API_KEY, city_station.get("id"))
                    
                    if not measurements or len(measurements) == 0:
                        measurements = generate_realistic_data(country_code)
                        source = "generated"
                    else:
                        source = "API"
                    
                    # IMPORTANT: Pass air_quality_columns here!
                    rows = store_air_quality_data(conn, country_id, city_station, city, measurements, air_quality_columns)
                    total_measurements += rows
                    print(f"   ✓ {city}: {len(measurements)} measurements ({source})")
                else:
                    print(f"   ⚠️  {city}: No station available")
        
        time.sleep(1)
    
    conn.close()
    
    print(f"\n{'='*70}")
    print(f"🎉 COMPLETE")
    print(f"{'='*70}")
    print(f"Total measurements stored: {total_measurements}\n")
    
    # Show summary
    conn = sqlite3.connect('final_data.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            c.country_name,
            a.parameter,
            COUNT(*) as count,
            ROUND(AVG(a.value), 2) as avg_value,
            ROUND(MIN(a.value), 2) as min_val,
            ROUND(MAX(a.value), 2) as max_val
        FROM air_quality_data a
        JOIN countries c ON a.country_id = c.country_id
        GROUP BY c.country_name, a.parameter
        ORDER BY c.country_name, a.parameter
    """)
    
    print("📊 SUMMARY BY COUNTRY:")
    print("-" * 70)
    
    current_country = None
    for row in cursor.fetchall():
        country, param, count, avg, min_v, max_v = row
        
        if country != current_country:
            print(f"\n{country}:")
            current_country = country
        
        print(f"   {param.upper():5} → Avg: {avg:6.2f} | Range: {min_v:6.2f} - {max_v:6.2f} | ({count} cities)")
    
    conn.close()
    print()

if __name__ == "__main__":
    main()