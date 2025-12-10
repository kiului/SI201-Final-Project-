"""
SI 201 Final Project - OpenWeather Data Collection
Member 1's File: openweather_fetch.py

This script fetches weather data from OpenWeather API and stores it in SQLite database.
It creates all database tables and manages the countries lookup table.

Run this script 4 times to collect 100+ weather data points (25 cities per run).
"""

import requests
import sqlite3
import json
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

API_KEY = "e92c3942dfe584525e4535af0db5bd23"
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
DATABASE_PATH = "final_data.db"

# List of cities to collect data from
# Format: (city_name, country_code)
# 96 cities across 32 countries for comprehensive spatial coverage
CITIES_TO_COLLECT = [
    # USA (3 cities)
    ("Washington", "US"),  # Washington DC
    ("New York", "US"),
    ("Los Angeles", "US"),
    
    # India (3 cities)
    ("New Delhi", "IN"),
    ("Mumbai", "IN"),
    ("Jaipur", "IN"),
    
    # China (3 cities)
    ("Beijing", "CN"),
    ("Shanghai", "CN"),
    ("Shenzhen", "CN"),
    
    # United Kingdom (3 cities)
    ("London", "GB"),
    ("Manchester", "GB"),
    ("Bristol", "GB"),
    
    # Brazil (3 cities)
    ("São Paulo", "BR"),
    ("Rio de Janeiro", "BR"),
    ("Brasília", "BR"),
    
    # Australia (3 cities)
    ("Canberra", "AU"),
    ("Melbourne", "AU"),
    ("Sydney", "AU"),  # "City" in your list likely means Sydney
    
    # Germany (3 cities)
    ("Berlin", "DE"),
    ("Munich", "DE"),
    ("Hamburg", "DE"),
    
    # South Africa (3 cities)
    ("Johannesburg", "ZA"),
    ("Cape Town", "ZA"),
    ("Pretoria", "ZA"),
    
    # Sweden (3 cities)
    ("Stockholm", "SE"),
    ("Gothenburg", "SE"),
    ("Uppsala", "SE"),
    
    # Russia (3 cities)
    ("Moscow", "RU"),
    ("Saint Petersburg", "RU"),
    ("Samara", "RU"),
    
    # Pakistan (3 cities)
    ("Karachi", "PK"),
    ("Lahore", "PK"),
    ("Multan", "PK"),
    
    # Spain (3 cities)
    ("Barcelona", "ES"),
    ("Madrid", "ES"),
    ("Seville", "ES"),
    
    # Thailand (3 cities)
    ("Bangkok", "TH"),
    ("Chiang Mai", "TH"),
    ("Pattaya", "TH"),
    
    # Japan (3 cities)
    ("Tokyo", "JP"),
    ("Sapporo", "JP"),
    ("Osaka", "JP"),
    
    # France (3 cities)
    ("Paris", "FR"),
    ("Lyon", "FR"),
    ("Marseille", "FR"),
    
    # Italy (3 cities)
    ("Rome", "IT"),
    ("Milan", "IT"),
    ("Florence", "IT"),
    
    # Canada (3 cities)
    ("Toronto", "CA"),
    ("Vancouver", "CA"),
    ("Montreal", "CA"),
    
    # Mexico (3 cities)
    ("Mexico City", "MX"),
    ("Guadalajara", "MX"),
    ("Monterrey", "MX"),
    
    # Argentina (3 cities)
    ("Buenos Aires", "AR"),
    ("Córdoba", "AR"),
    ("Rosario", "AR"),
    
    # South Korea (3 cities)
    ("Seoul", "KR"),
    ("Busan", "KR"),
    ("Incheon", "KR"),
    
    # Turkey (3 cities)
    ("Istanbul", "TR"),
    ("Ankara", "TR"),
    ("Izmir", "TR"),
    
    # Egypt (3 cities)
    ("Cairo", "EG"),
    ("Alexandria", "EG"),
    ("Giza", "EG"),
    
    # Indonesia (3 cities)
    ("Jakarta", "ID"),
    ("Surabaya", "ID"),
    ("Bandung", "ID"),
    
    # Saudi Arabia (3 cities)
    ("Riyadh", "SA"),
    ("Jeddah", "SA"),
    ("Dammam", "SA"),
    
    # Qatar (3 cities)
    ("Doha", "QA"),
    ("Al Rayyan", "QA"),
    ("Al Wakrah", "QA"),
    
    # Netherlands (3 cities)
    ("Amsterdam", "NL"),
    ("Rotterdam", "NL"),
    ("The Hague", "NL"),
    
    # Belgium (3 cities)
    ("Brussels", "BE"),
    ("Antwerp", "BE"),
    ("Ghent", "BE"),
    
    # Norway (3 cities)
    ("Oslo", "NO"),
    ("Bergen", "NO"),
    ("Stavanger", "NO"),
    
    # Switzerland (3 cities)
    ("Zurich", "CH"),
    ("Geneva", "CH"),
    ("Basel", "CH"),
    
    # Poland (3 cities)
    ("Warsaw", "PL"),
    ("Kraków", "PL"),
    ("Gdańsk", "PL"),
    
    # Greece (3 cities)
    ("Athens", "GR"),
    ("Thessaloniki", "GR"),
    ("Patras", "GR"),
    
    # Portugal (3 cities)
    ("Lisbon", "PT"),
    ("Porto", "PT"),
    ("Braga", "PT"),
    
    # Vietnam (3 cities)
    ("Hanoi", "VN"),
    ("Ho Chi Minh City", "VN"),
    ("Da Nang", "VN"),

    # Kenya (3 cities)
    ("Nairobi", "KE"),
    ("Mombasa", "KE"),
    ("Kisumu", "KE"),
]

# Country information with both 2-letter and 3-letter codes
# 2-letter codes: Used by OpenWeather and OpenAQ APIs
# 3-letter codes: Used by World Bank API
COUNTRY_INFO = {
    "US": {"name": "United States", "iso3": "USA"},
    "IN": {"name": "India", "iso3": "IND"},
    "CN": {"name": "China", "iso3": "CHN"},
    "GB": {"name": "United Kingdom", "iso3": "GBR"},
    "BR": {"name": "Brazil", "iso3": "BRA"},
    "AU": {"name": "Australia", "iso3": "AUS"},
    "DE": {"name": "Germany", "iso3": "DEU"},
    "ZA": {"name": "South Africa", "iso3": "ZAF"},
    "SE": {"name": "Sweden", "iso3": "SWE"},
    "RU": {"name": "Russia", "iso3": "RUS"},
    "PK": {"name": "Pakistan", "iso3": "PAK"},
    "ES": {"name": "Spain", "iso3": "ESP"},
    "TH": {"name": "Thailand", "iso3": "THA"},
    "JP": {"name": "Japan", "iso3": "JPN"},
    "FR": {"name": "France", "iso3": "FRA"},
    "IT": {"name": "Italy", "iso3": "ITA"},
    "CA": {"name": "Canada", "iso3": "CAN"},
    "MX": {"name": "Mexico", "iso3": "MEX"},
    "AR": {"name": "Argentina", "iso3": "ARG"},
    "KR": {"name": "South Korea", "iso3": "KOR"},
    "TR": {"name": "Turkey", "iso3": "TUR"},
    "EG": {"name": "Egypt", "iso3": "EGY"},
    "ID": {"name": "Indonesia", "iso3": "IDN"},
    "SA": {"name": "Saudi Arabia", "iso3": "SAU"},
    "QA": {"name": "Qatar", "iso3": "QAT"},
    "NL": {"name": "Netherlands", "iso3": "NLD"},
    "BE": {"name": "Belgium", "iso3": "BEL"},
    "NO": {"name": "Norway", "iso3": "NOR"},
    "CH": {"name": "Switzerland", "iso3": "CHE"},
    "PL": {"name": "Poland", "iso3": "POL"},
    "GR": {"name": "Greece", "iso3": "GRC"},
    "PT": {"name": "Portugal", "iso3": "PRT"},
    "VN": {"name": "Vietnam", "iso3": "VNM"},
    "KE": {"name": "Kenya", "iso3": "KEN"}
}


# ============================================================================
# FUNCTION 1: Create Database Tables
# ============================================================================

def create_database_tables(conn):
    """
    Creates all 4 database tables if they don't exist.
    
    Input: SQLite connection object
    Output: None (creates tables in database)
    Purpose: Sets up the complete database schema
    """
    cursor = conn.cursor()
    
    # Table 1: countries (lookup table with integer primary key)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS countries (
            country_id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT UNIQUE NOT NULL,
            country_code_3 TEXT UNIQUE NOT NULL,
            country_name TEXT NOT NULL
        )
    ''')
    
    # Table 2: weather_data (stores OpenWeather API data)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL,
            city_name TEXT NOT NULL,
            latitude REAL,
            longitude REAL,
            temperature REAL,
            humidity INTEGER,
            pressure INTEGER,
            timestamp INTEGER,
            FOREIGN KEY (country_id) REFERENCES countries(country_id)
        )
    ''')
    
    # Table 3: air_quality_data (for OpenAQ data - created by Member 2)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS air_quality_data (
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
    ''')
    
    # Table 4: economic_data (for World Bank data - created by Member 3)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS economic_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL,
            indicator_id TEXT,
            indicator_name TEXT,
            year INTEGER,
            value REAL,
            FOREIGN KEY (country_id) REFERENCES countries(country_id)
        )
    ''')
    
    conn.commit()
    print("✓ All database tables created successfully")


# ============================================================================
# FUNCTION 2: Get or Create Country ID
# ============================================================================

def get_or_create_country_id(conn, country_code, country_name, country_code_3=None):
    """
    Looks up country in countries table; if not found, creates it.
    
    Input:
        - conn: SQLite connection
        - country_code: str (2-letter code, e.g., "US")
        - country_name: str (e.g., "United States")
        - country_code_3: str (3-letter code, e.g., "USA") - optional
    Output: int (country_id)
    Purpose: Ensures no duplicate countries and returns the country_id
    """
    cursor = conn.cursor()
    
    # Try to find existing country by 2-letter code
    cursor.execute(
        'SELECT country_id FROM countries WHERE country_code = ?',
        (country_code,)
    )
    result = cursor.fetchone()
    
    if result:
        # Country exists, return its ID
        return result[0]
    else:
        # Country doesn't exist, create it
        # If 3-letter code not provided, use 2-letter code as fallback
        if country_code_3 is None:
            country_code_3 = country_code
            
        cursor.execute(
            'INSERT INTO countries (country_code, country_code_3, country_name) VALUES (?, ?, ?)',
            (country_code, country_code_3, country_name)
        )
        conn.commit()
        return cursor.lastrowid


# ============================================================================
# FUNCTION 3: Fetch Weather Data from API
# ============================================================================

def fetch_weather_data(api_key, city, country_code):
    """
    Makes API call to OpenWeather and returns JSON response.
    
    Input:
        - api_key: str
        - city: str (city name)
        - country_code: str
    Output: dict (weather data from API) or None if request fails
    Purpose: Retrieves current weather data for a specific city
    """
    params = {
        "q": f"{city},{country_code}",
        "appid": api_key,
        "units": "metric"  # Get temperature in Celsius
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Extract relevant fields
            weather_data = {
                'city': data['name'],
                'country': data['sys']['country'],
                'latitude': data['coord']['lat'],
                'longitude': data['coord']['lon'],
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'timestamp': data['dt']
            }
            
            return weather_data
        else:
            print(f"✗ Error fetching data for {city}, {country_code}: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"✗ Exception fetching data for {city}, {country_code}: {e}")
        return None


# ============================================================================
# FUNCTION 4: Store Weather Data in Database
# ============================================================================

def store_weather_data(conn, country_id, weather_dict):
    """
    Inserts weather data into database.
    
    Input:
        - conn: SQLite connection
        - country_id: int
        - weather_dict: dict (from fetch_weather_data)
    Output: bool (True if inserted, False if duplicate)
    Purpose: Stores weather data, avoiding duplicates
    """
    cursor = conn.cursor()
    
    # Check if this exact data point already exists
    # (same city and timestamp = duplicate)
    cursor.execute(
        '''SELECT COUNT(*) FROM weather_data 
           WHERE city_name = ? AND timestamp = ?''',
        (weather_dict['city'], weather_dict['timestamp'])
    )
    
    if cursor.fetchone()[0] > 0:
        # Duplicate found
        return False
    
    # Insert new weather data
    cursor.execute(
        '''INSERT INTO weather_data 
           (country_id, city_name, latitude, longitude, 
            temperature, humidity, pressure, timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
        (country_id,
         weather_dict['city'],
         weather_dict['latitude'],
         weather_dict['longitude'],
         weather_dict['temperature'],
         weather_dict['humidity'],
         weather_dict['pressure'],
         weather_dict['timestamp'])
    )
    
    conn.commit()
    return True


# ============================================================================
# FUNCTION 5: Main Execution Function
# ============================================================================

def main():
    """
    Orchestrates the entire weather data collection process.
    Respects 25-item limit per run.
    
    Purpose: 
        - Connects to database
        - Creates tables if needed
        - Fetches weather data for cities
        - Stores data respecting 25-item limit
        - Tracks progress
    """
    print("=" * 60)
    print("SI 201 Final Project - OpenWeather Data Collection")
    print("=" * 60)
    print()
    
    # Connect to database
    conn = sqlite3.connect(DATABASE_PATH)
    print(f"✓ Connected to database: {DATABASE_PATH}")
    
    # Create all tables
    create_database_tables(conn)
    print()
    
    # Check how many weather records already exist
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM weather_data')
    existing_count = cursor.fetchone()[0]
    print(f"Current weather data records in database: {existing_count}")
    
    # How many items to collect this run (fixed cap, no total target)
    items_to_collect_this_run = 25
    print(f"Will collect up to {items_to_collect_this_run} new records this run (if available).")
    print()
    print("-" * 60)
    print()
    
    # Track which cities have already been collected
    cursor.execute('SELECT DISTINCT city_name FROM weather_data')
    collected_cities = set(row[0] for row in cursor.fetchall())
    
    # Collect new weather data
    items_collected = 0
    
    for city, country_code in CITIES_TO_COLLECT:
        # Stop if we've collected enough items this run
        if items_collected >= items_to_collect_this_run:
            break
        
        # Skip if we already have this city
        if city in collected_cities:
            continue
        
        print(f"Fetching: {city}, {country_code}...", end=" ")
        
        # Fetch weather data from API
        weather_data = fetch_weather_data(API_KEY, city, country_code)
        
        if weather_data is None:
            continue
        
        # Get or create country in database
        country_info = COUNTRY_INFO.get(country_code, {})
        country_name = country_info.get("name", country_code)
        country_code_3 = country_info.get("iso3", country_code)
        country_id = get_or_create_country_id(conn, country_code, country_name, country_code_3)
        
        # Store weather data
        inserted = store_weather_data(conn, country_id, weather_data)
        
        if inserted:
            items_collected += 1
            print(f"✓ Stored ({items_collected}/{items_to_collect_this_run})")
            print(f"   Temperature: {weather_data['temperature']}°C, "
                  f"Humidity: {weather_data['humidity']}%")
        else:
            print("✗ Duplicate (skipped)")
    
    # Final summary
     # Final summary
    print()
    print("-" * 60)
    print()
    cursor.execute('SELECT COUNT(*) FROM weather_data')
    final_count = cursor.fetchone()[0]
    print("✓ Data collection complete!")
    print(f"  - New records added this run: {items_collected}")
    print(f"  - Total records in database: {final_count}")
    
    # Close database connection
    conn.close()
    print()
    print("=" * 60)


if __name__ == "__main__":
    main()