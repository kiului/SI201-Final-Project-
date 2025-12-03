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


def fetch_locations(api_key, country_code, limit=50):
    """
    Fetches monitoring station locations for a given country.

    Inputs:
        api_key (str)
        country_code (str)
        limit (int)

    Output:
        list of location dicts
    """
    url = f"{BASE_URL}/locations"
    headers = {"X-API-Key": api_key}
    params = {
        "countries": country_code,  # Note: OpenAQ v3 uses "countries" (plural)
        "limit": limit
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        return data.get("results", [])

    except requests.exceptions.RequestException as e:
        print(f"Error fetching locations for {country_code}: {e}")
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
    # Correct endpoint for latest measurements
    url = f"{BASE_URL}/locations/{location_id}/latest"
    headers = {"X-API-Key": api_key}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        # Extract measurements from the response
        results = data.get("results", [])
        if not results:
            return []

        measurements = results[0].get("measurements", [])

        # Filter for required pollutants (check parameter.name per API v3 structure)
        wanted = {"pm25", "no2", "o3"}
        filtered = []
        for m in measurements:
            param_name = m.get("parameter", {}).get("name", "").lower()
            if param_name in wanted:
                filtered.append(m)
        
        return filtered

    except requests.exceptions.RequestException as e:
        print(f"Error fetching measurements for location {location_id}: {e}")
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

    # Extract location details
    location_id = location_info.get("id")
    location_name = location_info.get("name")
    city = location_info.get("locality")
    coords = location_info.get("coordinates", {})
    latitude = coords.get("latitude")
    longitude = coords.get("longitude")

    for m in measurements:
        # Extract parameter info (OpenAQ v3 structure)
        parameter_info = m.get("parameter", {})
        parameter_name = parameter_info.get("name", "").lower()
        
        # Get measurement value and timestamp
        value = m.get("value")
        unit = m.get("unit")
        timestamp = m.get("datetime", {}).get("utc")

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

    conn.commit()
    return rows_inserted

def main():
    """
    Main function to run air quality data collection.
    Respects 25-item limit per project requirements.
    """
    # Connect to database
    conn = sqlite3.connect('final_data.db')
    
    # List of countries to collect data from (limit to ~25 total items)
    # You'll need to coordinate with your team on which countries to use
    countries = ["US", "GB", "IN", "JP", "BR"]  # Example - expand as needed
    
    total_items = 0
    max_items = 25
    
    for country_code in countries:
        if total_items >= max_items:
            print(f"Reached 25-item limit. Stopping.")
            break
            
        print(f"\nFetching air quality data for {country_code}...")
        
        # Get locations for this country
        locations = fetch_locations(API_KEY, country_code, limit=10)
        
        if not locations:
            print(f"No locations found for {country_code}")
            continue
        
        # Process first location for this country (to respect item limit)
        location = locations[0]
        location_id = location.get("id")
        
        # Get country_id from database (assuming it exists from openweather_fetch.py)
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM countries WHERE country_code = ?", (country_code,))
        result = cursor.fetchone()
        
        if not result:
            print(f"Country {country_code} not found in database. Skipping.")
            continue
            
        country_id = result[0]
        
        # Fetch measurements
        measurements = fetch_latest_measurements(API_KEY, location_id)
        
        if measurements:
            rows = store_air_quality_data(conn, country_id, location, measurements)
            print(f"Stored {rows} measurements for {country_code}")
            total_items += 1
        else:
            print(f"No measurements found for location {location_id}")
    
    conn.close()
    print(f"\nTotal countries processed: {total_items}")

if __name__ == "__main__":
    main()