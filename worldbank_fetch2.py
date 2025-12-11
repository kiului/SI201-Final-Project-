import requests
import sqlite3
from datetime import datetime

DB_NAME = "final_data.db"

# Fetch one World Bank indicator value for a specific country and year
def fetch_indicator(indicator_id, country_code, year):
    """
    Fetch a single indicator value from World Bank API for a specific country and year
    """
    BASE_URL = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_id}"

    params = {
        "format": "json",
        "per_page": 25,
        "page": 1
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code != 200:
        print(f"Request failed for {country_code}")
        return []

    data = response.json()
    
    # World Bank API returns data[0] = metadata, data[1] = actual results
    if not data or len(data) < 2:
        return []
    
    entries = data[1]
    results = []

    # Loop through entries and find matching year
    for item in entries:
        if item["date"] == str(year):
            cleaned = {
                "country_name": item["country"]["value"],
                "country_code": item["countryiso3code"],
                "indicator_id": item["indicator"]["id"],
                "indicator_name": item["indicator"]["value"],
                "year": int(item["date"]),
                "value": item["value"]
            }
            results.append(cleaned)
            break
    return results


def create_economic_table():
    """
    Create economic_data table if it doesn't exist
    Uses UNIQUE constraint to prevent duplicate entries
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Create table with UNIQUE constraint (prevents duplicates)
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
    print("Economic_data table ready.")


def get_country_id(country_code):
    """
    Get country_id from countries table using country_code_3
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT country_id FROM countries WHERE country_code_3 = ?", (country_code,))
    row = cur.fetchone()
    conn.close()

    if row is None:
        raise ValueError(f"Country code '{country_code}' not found in countries table.")
    return row[0]


def count_economic_data():
    """
    Count total rows in economic_data table
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM economic_data")
    count = cur.fetchone()[0]
    conn.close()
    return count


def store_economic_data(entry):
    """
    Insert new data into economic_data table
    Returns True if inserted, False if duplicate
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    country_id = get_country_id(entry["country_code"])

    try:
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
        print(f"INSERTED → {entry['country_code']} {entry['year']}")
        return True
    except sqlite3.IntegrityError:
        # Duplicate entry - skip it
        print(f"SKIPPED (duplicate) → {entry['country_code']} {entry['year']}")
        return False
    finally:
        conn.close()


def save_indicator_to_db(indicator_id, country_code, year):
    """
    Fetch indicator from API and save to database
    Returns True if successfully saved, False otherwise
    """
    result = fetch_indicator(indicator_id, country_code, year)

    if not result:
        print(f"No data returned for {country_code} {year}")
        return False

    entry = result[0]
    return store_economic_data(entry)


def print_economic_data():
    """
    Print all rows from economic_data table
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("SELECT * FROM economic_data")
    rows = cur.fetchall()

    print("\n----- ECONOMIC DATA TABLE -----")
    for row in rows:
        print(row)

    conn.close()


# Main execution
if __name__ == "__main__":
    # Create table
    create_economic_table()

    # Check current count
    current_count = count_economic_data()
    print(f"\nCurrent rows in database: {current_count}")

    # Calculate how many more items to add (max 25 per run)
    items_to_add = min(25, 100 - current_count)
    
    if items_to_add <= 0:
        print("Already have 100+ items. No more data to add.")
        print_economic_data()
        exit()

    print(f"Will attempt to add up to {items_to_add} items this run.\n")

    # List of all data to collect (100+ total items)
    # 25 countries × 4 years = 100 items
    data_to_collect = [
        ("USA", 2023), ("USA", 2022), ("USA", 2021), ("USA", 2020),
        ("IND", 2023), ("IND", 2022), ("IND", 2021), ("IND", 2020),
        ("CHN", 2023), ("CHN", 2022), ("CHN", 2021), ("CHN", 2020),
        ("GBR", 2023), ("GBR", 2022), ("GBR", 2021), ("GBR", 2020),
        ("BRA", 2023), ("BRA", 2022), ("BRA", 2021), ("BRA", 2020),
        ("AUS", 2023), ("AUS", 2022), ("AUS", 2021), ("AUS", 2020),
        ("DEU", 2023), ("DEU", 2022), ("DEU", 2021), ("DEU", 2020),
        ("THA", 2023), ("THA", 2022), ("THA", 2021), ("THA", 2020),
        ("KOR", 2023), ("KOR", 2022), ("KOR", 2021), ("KOR", 2020),
        ("JPN", 2023), ("JPN", 2022), ("JPN", 2021), ("JPN", 2020),
        ("FRA", 2023), ("FRA", 2022), ("FRA", 2021), ("FRA", 2020),
        ("CAN", 2023), ("CAN", 2022), ("CAN", 2021), ("CAN", 2020),
        ("ITA", 2023), ("ITA", 2022), ("ITA", 2021), ("ITA", 2020),
        ("MEX", 2023), ("MEX", 2022), ("MEX", 2021), ("MEX", 2020),
        ("ESP", 2023), ("ESP", 2022), ("ESP", 2021), ("ESP", 2020),
        ("NLD", 2023), ("NLD", 2022), ("NLD", 2021), ("NLD", 2020),
        ("SAU", 2023), ("SAU", 2022), ("SAU", 2021), ("SAU", 2020),
        ("TUR", 2023), ("TUR", 2022), ("TUR", 2021), ("TUR", 2020),
        ("CHE", 2023), ("CHE", 2022), ("CHE", 2021), ("CHE", 2020),
        ("POL", 2023), ("POL", 2022), ("POL", 2021), ("POL", 2020),
        ("BEL", 2023), ("BEL", 2022), ("BEL", 2021), ("BEL", 2020),
        ("SWE", 2023), ("SWE", 2022), ("SWE", 2021), ("SWE", 2020),
        ("ARG", 2023), ("ARG", 2022), ("ARG", 2021), ("ARG", 2020),
        ("NOR", 2023), ("NOR", 2022), ("NOR", 2021), ("NOR", 2020),
        ("AUT", 2023), ("AUT", 2022), ("AUT", 2021), ("AUT", 2020),
    ]

    # Track how many items we've successfully added this run
    items_added = 0
    
    for country_code, year in data_to_collect:
        # Stop when we reach the 25-item limit
        if items_added >= items_to_add:
            print(f"\nReached limit of {items_to_add} items for this run.")
            break
        
        # Try to save the indicator
        success = save_indicator_to_db("NY.GDP.PCAP.CD", country_code, year)
        
        # Only count successful insertions (not duplicates)
        if success:
            items_added += 1

    print(f"\n=== Added {items_added} new items this run ===")
    print(f"Total items in database: {count_economic_data()}")
    
    if count_economic_data() >= 100:
        print("\n✓ Successfully collected 100+ rows!")
    else:
        remaining = 100 - count_economic_data()
        runs_needed = (remaining + 24) // 25  # Round up
        print(f"\nNeed to run {runs_needed} more time(s) to reach 100 rows.")
    
    print_economic_data()