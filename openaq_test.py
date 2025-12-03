import requests
import json
import sqlite3

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

# TEST 2: Test API connection directly
print("\n=== Testing API connection ===")
test_url = f"{BASE_URL}/locations"
headers = {"X-API-Key": API_KEY}
params = {"countries": "US", "limit": 1}

try:
    response = requests.get(test_url, headers=headers, params=params)
    print(f"Status code: {response.status_code}")
    print(f"Response preview: {response.text[:200]}")
except Exception as e:
    print(f"Error: {e}")

# TEST 3: Check database
print("\n=== Checking database ===")
try:
    conn = sqlite3.connect('final_data.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"Tables in database: {tables}")
    
    cursor.execute("SELECT * FROM countries LIMIT 5")
    countries = cursor.fetchall()
    print(f"Sample countries: {countries}")
    conn.close()
except Exception as e:
    print(f"Database error: {e}")

print("\n=== Starting main() ===\n")


def setup_database(conn):
    """
    Creates tables and adds countries if they don't exist.
    Safe to run multiple times.
    """
    cursor = conn.cursor()
    
    # Create countries table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS countries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT UNIQUE NOT NULL,
            country_name TEXT NOT NULL
        )
    """)
    
    # Create air_quality_data table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS air_quality_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL,
            location_id INTEGER,
            station_name TEXT,
            city TEXT,
            latitude REAL,
            longitude REAL,
            parameter_type TEXT,
            measurement_value REAL,
            unit TEXT,
            measurement_timestamp TEXT,
            FOREIGN KEY (country_id) REFERENCES countries(id)
        )
    """)
    
    # Add your specific countries
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
        cursor.execute("""
            INSERT OR IGNORE INTO countries (country_code, country_name)
            VALUES (?, ?)
        """, (code, name))
    
    conn.commit()
    print(f"   ✓ Countries table ready with {len(countries_list)} countries")

def fetch_locations_by_city(api_key, city_name, country_code, limit=5):
    """
    Fetches monitoring stations for a specific city.
    
    Inputs:
        api_key (str)
        city_name (str) - e.g., "New York"
        country_code (str) - e.g., "US"
        limit (int)
    
    Output:
        list of location dicts
    """
    print(f"      → Searching for stations in {city_name}...")
    url = f"{BASE_URL}/locations"
    headers = {"X-API-Key": api_key}
    params = {
        "countries": country_code,
        "limit": limit
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        results = data.get("results", [])
        
        # Filter for locations in the specific city
        # OpenAQ uses "locality" field for city name
        city_locations = []
        for loc in results:
            locality = loc.get("locality", "").lower()
            city_lower = city_name.lower()
            
            # Match city name (handle variations like "New York" vs "New York City")
            if city_lower in locality or locality in city_lower:
                city_locations.append(loc)
        
        if city_locations:
            print(f"      ✓ Found {len(city_locations)} stations in {city_name}")
        else:
            print(f"      ⚠️  No stations found specifically for {city_name}, using first available")
            # If no city match, use first result from country
            city_locations = results[:1] if results else []
        
        return city_locations

    except requests.exceptions.RequestException as e:
        print(f"      ✗ Error fetching locations: {e}")
        return []

def fetch_latest_measurements(api_key, location_id):
    """
    Fetches pm25, no2, and o3 measurements for a given location.
    
    Inputs:
        api_key (str)
        location_id (int)
    
    Output:
        list of measurement dicts
    """
    url = f"{BASE_URL}/locations/{location_id}/latest"
    headers = {"X-API-Key": api_key}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if not results:
            return []

        measurements = results[0].get("measurements", [])

        # Filter for required pollutants
        wanted = {"pm25", "no2", "o3"}
        filtered = []
        for m in measurements:
            param_name = m.get("parameter", {}).get("name", "").lower()
            if param_name in wanted:
                filtered.append(m)
        
        return filtered

    except requests.exceptions.RequestException as e:
        print(f"        ✗ Error fetching measurements: {e}")
        return []

def store_air_quality_data(conn, country_id, location_info, measurements):
    """
    Inserts location + measurement data into the database.
    
    Inputs:
        conn (sqlite3.Connection)
        country_id (int)
        location_info (dict)
        measurements (list of dicts)
    
    Output:
        int = number of rows inserted
    """
    cursor = conn.cursor()
    rows_inserted = 0

    location_id = location_info.get("id")
    location_name = location_info.get("name")
    city = location_info.get("locality")
    coords = location_info.get("coordinates", {})
    latitude = coords.get("latitude")
    longitude = coords.get("longitude")

    for m in measurements:
        parameter_info = m.get("parameter", {})
        parameter_name = parameter_info.get("name", "").lower()
        
        value = m.get("value")
        unit = m.get("unit")
        timestamp = m.get("datetime", {}).get("utc")

        try:
            cursor.execute("""
                INSERT INTO air_quality_data (
                    country_id,
                    location_id,
                    station_name,
                    city,
                    latitude,
                    longitude,
                    parameter_type,
                    measurement_value,
                    unit,
                    measurement_timestamp
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                country_id,
                location_id,
                location_name,
                city,
                latitude,
                longitude,
                parameter_name,
                value,
                unit,
                timestamp
            ))
            rows_inserted += 1
        except sqlite3.Error as e:
            print(f"        ✗ Database error: {e}")

    conn.commit()
    return rows_inserted

def main():
    """
    Main function to collect air quality data from specific cities.
    Collects data from multiple cities per country.
    """
    print("=" * 70)
    print("AIR QUALITY DATA COLLECTION - MULTI-CITY")
    print("=" * 70)
    
    # Connect to database
    conn = sqlite3.connect('final_data.db')
    
    # Setup database
    print("\n1. Setting up database...")
    setup_database(conn)
    
    # Define countries and their cities
    countries_cities = {
        "US": {
            "name": "United States",
            "cities": ["Washington", "New York", "Los Angeles"]
        },
        "IN": {
            "name": "India",
            "cities": ["New Delhi", "Mumbai", "Jaipur"]
        },
        "CN": {
            "name": "China",
            "cities": ["Beijing", "Shanghai", "Shenzhen"]
        },
        "GB": {
            "name": "United Kingdom",
            "cities": ["London", "Manchester", "Bristol"]
        },
        "BR": {
            "name": "Brazil",
            "cities": ["Sao Paulo", "Rio de Janeiro", "Brasilia"]
        },
        "AU": {
            "name": "Australia",
            "cities": ["Canberra", "Melbourne", "Sydney"]  # Changed "City" to "Sydney"
        },
        "DE": {
            "name": "Germany",
            "cities": ["Berlin", "Munich", "Hamburg"]
        }
    }
    
    cursor = conn.cursor()
    total_measurements = 0
    
    print(f"\n2. Collecting air quality data from {len(countries_cities)} countries...")
    
    for country_code, info in countries_cities.items():
        country_name = info["name"]
        cities = info["cities"]
        
        print(f"\n{'='*70}")
        print(f"🌍 PROCESSING: {country_name} ({country_code})")
        print(f"   Cities: {', '.join(cities)}")
        print(f"{'='*70}")
        
        # Get country_id
        cursor.execute("SELECT id FROM countries WHERE country_code = ?", (country_code,))
        result = cursor.fetchone()
        
        if not result:
            print(f"   ✗ Country not found in database!")
            continue
        
        country_id = result[0]
        country_measurement_count = 0
        
        # Process each city
        for city_name in cities:
            print(f"\n   📍 {city_name}:")
            
            # Get locations for this city
            locations = fetch_locations_by_city(API_KEY, city_name, country_code, limit=10)
            
            if not locations:
                print(f"      ⚠️  No monitoring stations found")
                continue
            
            # Use the first location from this city
            location = locations[0]
            location_id = location.get("id")
            station_name = location.get("name")
            
            print(f"      → Station: {station_name} (ID: {location_id})")
            
            # Fetch measurements
            measurements = fetch_latest_measurements(API_KEY, location_id)
            
            if measurements:
                print(f"      📊 Measurements:")
                for m in measurements:
                    param = m.get('parameter', {}).get('name', 'unknown').upper()
                    value = m.get('value')
                    unit = m.get('unit')
                    print(f"         • {param}: {value} {unit}")
                
                # Store in database
                rows = store_air_quality_data(conn, country_id, location, measurements)
                country_measurement_count += rows
                total_measurements += rows
                print(f"      ✓ Stored {rows} measurements")
            else:
                print(f"      ⚠️  No measurements available")
        
        print(f"\n   ✅ {country_name} complete: {country_measurement_count} total measurements")
    
    conn.close()
    
    print(f"\n{'='*70}")
    print(f"🎉 COLLECTION COMPLETE")
    print(f"{'='*70}")
    print(f"Total measurements stored: {total_measurements}")
    
    # Show summary by country
    print("\n📊 SUMMARY BY COUNTRY:")
    print("-" * 70)
    
    conn = sqlite3.connect('final_data.db')
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            c.country_name,
            a.parameter_type,
            COUNT(*) as measurement_count,
            AVG(a.measurement_value) as avg_value,
            MIN(a.measurement_value) as min_value,
            MAX(a.measurement_value) as max_value
        FROM air_quality_data a
        JOIN countries c ON a.country_id = c.id
        GROUP BY c.country_name, a.parameter_type
        ORDER BY c.country_name, a.parameter_type
    """)
    
    current_country = None
    for row in cursor.fetchall():
        country, param, count, avg, min_val, max_val = row
        
        if country != current_country:
            print(f"\n{country}:")
            current_country = country
        
        print(f"   {param.upper():5} → Avg: {avg:6.2f} (min: {min_val:6.2f}, max: {max_val:6.2f}) from {count} cities")
    
    conn.close()

if __name__ == "__main__":
    main()