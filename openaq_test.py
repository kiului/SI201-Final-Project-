import requests
import json

API_KEY = "b93b8a75a83fd2286b29961a532025b2f7532f865f0071530fef3b14dccf2a24"   # ← put your real key here

BASE_URL = "https://api.openaq.org/v3/locations/2178"  # example location ID

headers = {
    "X-API-Key": API_KEY
}

# Optional: add params depending on what you want
params = {
    # leave empty or add things like limit/page if the endpoint supports it
}

response = requests.get(BASE_URL, headers=headers, params=params)

print("Status code:", response.status_code)

if response.status_code != 200:
    print("Request failed:")
    print(response.text)
else:
    data = response.json()
    print(json.dumps(data, indent=4))



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
        "country": country_code,
        "limit": limit
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        return data.get("results", [])

    except requests.exceptions.RequestException as e:
        print(f"Error fetching locations: {e}")
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
    url = f"{BASE_URL}/latest"
    headers = {"X-API-Key": api_key}
    params = {"location_id": location_id}

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if not results:
            return []

        measurements = results[0].get("measurements", [])

        # Filter for required pollutants
        wanted = {"pm25", "no2", "o3"}
        return [m for m in measurements if m.get("parameter") in wanted]

    except requests.exceptions.RequestException as e:
        print(f"Error fetching measurements: {e}")
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

    for m in measurements:
        cursor.execute("""
            INSERT INTO air_quality (
                country_id,
                location_id,
                location_name,
                parameter,
                value,
                unit,
                last_updated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            country_id,
            location_info.get("id"),
            location_info.get("name"),
            m.get("parameter"),
            m.get("value"),
            m.get("unit"),
            m.get("lastUpdated")
        ))

        rows_inserted += 1

    conn.commit()
    return rows_inserted
