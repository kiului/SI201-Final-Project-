
import requests
import json
import requests
import sqlite3
import json
from datetime import datetime


# Fetch one World Bank indicator value for a specific country and year
def fetch_indicator(indicator_id, country_code, year):
    # Construct the base API URL using country + indicator ID
    BASE_URL = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_id}"

    params = {
        "format": "json",
        "per_page": 50,   # how many results per page (max 50)
        "page": 1
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        print("Request failed for {country_code}")
        return []


    data = response.json()
    
    # World Bank API has data[0] = metadata, data[1] = actual results
    # data[1] contains the actual indicator entries
    if not data or len(data) < 2:
        return []
    
    # Extract only the data we use 
    entries = data[1]

    results = []

    # Loop through every entry returned by the API
    for item in entries:
        # Only keep the entry where the year matches what the user wants
        if item["date"] == str(year):
            # Create a clean dictionary with only the useful fields
            cleaned = {
                "country_name": item["country"]["value"],
                "country_code": item["countryiso3code"],
                "indicator_id": item["indicator"]["id"],
                "indicator_name": item["indicator"]["value"],
                "year": int(item["date"]),
                "value": item["value"]
            }

            results.append(cleaned)
            break # Stop early because we found the correct year
    return results

print(fetch_indicator("NY.GDP.PCAP.CD", "USA", 2024))
print(fetch_indicator("NY.GDP.PCAP.CD", "IND", 2024))
print(fetch_indicator("NY.GDP.PCAP.CD", "CHN", 2024))
print(fetch_indicator("NY.GDP.PCAP.CD", "GBR", 2024))
print(fetch_indicator("NY.GDP.PCAP.CD", "BRA", 2024))
print(fetch_indicator("NY.GDP.PCAP.CD", "AUS", 2024))
print(fetch_indicator("NY.GDP.PCAP.CD", "DEU", 2024))


# create economic data table
DB_NAME = "final_data.db"
def create_economic_table():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS economic_data") # Delete old table so we can recreate it with UNIQUE constraint

    # Create table with UNIQUE constraint
    cur.execute("""
        CREATE TABLE IF NOT EXISTS economic_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_id INTEGER NOT NULL,
            indicator_id TEXT,
            indicator_name TEXT,
            year INTEGER,
            value REAL,
            FOREIGN KEY (country_id) REFERENCES countries(country_id),
            UNIQUE (country_id, indicator_id, year)
        )
    """)

    conn.commit()
    conn.close()
    print("New economic_data table created with UNIQUE constraint.")

# Select country_id for the matching country_code
def get_country_id(country_code):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT country_id FROM countries WHERE country_code_3 = ?", (country_code,))

    row = cur.fetchone()

    conn.close()

    if row is None:
        raise ValueError(f"Country code '{country_code}' not found in countries table.")
    return row[0]

# INSERT new data row into economic_data table
def store_economic_data(entry):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    country_id = get_country_id(entry["country_code"])

    cur.execute("""
        INSERT INTO economic_data (country_id, indicator_id, indicator_name, year, value)
        VALUES (?, ?, ?, ?, ?)
    """, (
        country_id,
        entry["indicator_id"],
        entry["indicator_name"],
        entry["year"],
        entry["value"]
    ))

    conn.commit()
    conn.close()
    print(f"INSERTED → {entry['country_code']} {entry['year']}")


#main
def save_indicator_to_db(indicator_id, country_code, year):
    result = fetch_indicator(indicator_id, country_code, year)  # Fetch indicator data from the API

    if not result:
        print(f"No data returned for {country_code} {year}") 
        return

    entry = result[0] # The API returns a list; take the first (only) item
    store_economic_data(entry) # Insert it into the database



# print economic data
def print_economic_data():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT * FROM economic_data") 
    rows = cur.fetchall()

    print("\n----- ECONOMIC DATA TABLE -----")
    for row in rows:
        print(row)

    conn.close()



create_economic_table()

save_indicator_to_db("NY.GDP.PCAP.CD", "USA", 2024)   # United States
save_indicator_to_db("NY.GDP.PCAP.CD", "IND", 2024)   # India
save_indicator_to_db("NY.GDP.PCAP.CD", "CHN", 2024)   # China
save_indicator_to_db("NY.GDP.PCAP.CD", "GBR", 2024)   # United Kingdom
save_indicator_to_db("NY.GDP.PCAP.CD", "BRA", 2024)   # Brazil
save_indicator_to_db("NY.GDP.PCAP.CD", "AUS", 2024)   # Australia
save_indicator_to_db("NY.GDP.PCAP.CD", "DEU", 2024)   # Germany
save_indicator_to_db("NY.GDP.PCAP.CD", "ZAF", 2024)   # South Africa
save_indicator_to_db("NY.GDP.PCAP.CD", "SWE", 2024)   # Sweden
save_indicator_to_db("NY.GDP.PCAP.CD", "RUS", 2024)   # Russia
save_indicator_to_db("NY.GDP.PCAP.CD", "PAK", 2024)   # Pakistan
save_indicator_to_db("NY.GDP.PCAP.CD", "ESP", 2024)   # Spain
save_indicator_to_db("NY.GDP.PCAP.CD", "THA", 2024)   # Thailand
save_indicator_to_db("NY.GDP.PCAP.CD", "JPN", 2024)   # Japan
save_indicator_to_db("NY.GDP.PCAP.CD", "FRA", 2024)   # France
save_indicator_to_db("NY.GDP.PCAP.CD", "ITA", 2024)   # Italy
save_indicator_to_db("NY.GDP.PCAP.CD", "CAN", 2024)   # Canada
save_indicator_to_db("NY.GDP.PCAP.CD", "MEX", 2024)   # Mexico
save_indicator_to_db("NY.GDP.PCAP.CD", "ARG", 2024)   # Argentina
save_indicator_to_db("NY.GDP.PCAP.CD", "KOR", 2024)   # South Korea
save_indicator_to_db("NY.GDP.PCAP.CD", "TUR", 2024)   # Turkey
save_indicator_to_db("NY.GDP.PCAP.CD", "EGY", 2024)   # Egypt
save_indicator_to_db("NY.GDP.PCAP.CD", "IDN", 2024)   # Indonesia
save_indicator_to_db("NY.GDP.PCAP.CD", "SAU", 2024)   # Saudi Arabia
save_indicator_to_db("NY.GDP.PCAP.CD", "QAT", 2024)   # Qatar
save_indicator_to_db("NY.GDP.PCAP.CD", "NLD", 2024)   # Netherlands
save_indicator_to_db("NY.GDP.PCAP.CD", "BEL", 2024)   # Belgium
save_indicator_to_db("NY.GDP.PCAP.CD", "NOR", 2024)   # Norway
save_indicator_to_db("NY.GDP.PCAP.CD", "CHE", 2024)   # Switzerland
save_indicator_to_db("NY.GDP.PCAP.CD", "POL", 2024)   # Poland
save_indicator_to_db("NY.GDP.PCAP.CD", "GRC", 2024)   # Greece
save_indicator_to_db("NY.GDP.PCAP.CD", "PRT", 2024)   # Portugal
save_indicator_to_db("NY.GDP.PCAP.CD", "VNM", 2024)   # Vietnam