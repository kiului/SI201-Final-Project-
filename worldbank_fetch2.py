"""
SI 201 Final Project - World Bank Data Collection
Member's File: worldbank_fetch.py

This script fetches economic data from World Bank API and stores it in SQLite database.
It manages the economic_data table and uses the shared countries table.

Run this script 4 times to collect 100+ economic data points (25 items per run).
"""

import requests
import sqlite3
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_URL = "https://api.worldbank.org/v2/country"
DATABASE_PATH = "final_data.db"

# Economic indicator to collect
INDICATOR_ID = "NY.GDP.PCAP.CD"  # GDP per capita (current US$)

# List of countries and years to collect data from
# Format: (country_code_3, year)
# 25 countries × 4 years = 100 data points
DATA_TO_COLLECT = [
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


# ============================================================================
# FUNCTION 1: Create Economic Data Table
# ============================================================================

def create_economic_table(conn):
    """
    Creates economic_data table if it doesn't exist.
    Uses UNIQUE constraint to prevent duplicate entries.
    
    Input: SQLite connection object
    Output: None (creates table in database)
    Purpose: Sets up the economic_data table schema
    """
    cursor = conn.cursor()
    
    # Check if table exists and has UNIQUE constraint
    cursor.execute("""
        SELECT sql FROM sqlite_master 
        WHERE type='table' AND name='economic_data'
    """)
    result = cursor.fetchone()
    
    # If table doesn't exist OR doesn't have UNIQUE constraint, recreate it
    if result is None or 'UNIQUE' not in result[0]:
        cursor.execute("DROP TABLE IF EXISTS economic_data")
        print("✓ Recreating economic_data table with UNIQUE constraint")
        
        cursor.execute("""
            CREATE TABLE economic_data (
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
    else:
        print("✓ Economic_data table already exists with UNIQUE constraint")
    
    conn.commit()


# ============================================================================
# FUNCTION 2: Get Country ID from Database
# ============================================================================

def get_country_id(conn, country_code_3):
    """
    Looks up country_id from countries table using 3-letter country code.
    
    Input:
        - conn: SQLite connection
        - country_code_3: str (3-letter code, e.g., "USA")
    Output: int (country_id)
    Purpose: Gets the country_id for foreign key reference
    """
    cursor = conn.cursor()
    
    cursor.execute(
        'SELECT country_id FROM countries WHERE country_code_3 = ?',
        (country_code_3,)
    )
    result = cursor.fetchone()
    
    if result is None:
        raise ValueError(f"Country code '{country_code_3}' not found in countries table")
    
    return result[0]


# ============================================================================
# FUNCTION 3: Fetch Economic Indicator from World Bank API
# ============================================================================

def fetch_indicator(indicator_id, country_code, year):
    """
    Makes API call to World Bank and returns indicator data for specific year.
    
    Input:
        - indicator_id: str (e.g., "NY.GDP.PCAP.CD")
        - country_code: str (3-letter code)
        - year: int
    Output: dict (economic data) or None if request fails
    Purpose: Retrieves economic indicator data for a specific country and year
    """
    url = f"{BASE_URL}/{country_code}/indicator/{indicator_id}"
    
    params = {
        "format": "json",
        "per_page": 25,
        "page": 1
    }
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            print(f"✗ Request failed for {country_code}: Status {response.status_code}")
            return None
        
        data = response.json()
        
        # World Bank API returns data[0] = metadata, data[1] = actual results
        if not data or len(data) < 2:
            return None
        
        entries = data[1]
        
        # Find the entry matching the requested year
        for item in entries:
            if item["date"] == str(year):
                return {
                    "country_name": item["country"]["value"],
                    "country_code": item["countryiso3code"],
                    "indicator_id": item["indicator"]["id"],
                    "indicator_name": item["indicator"]["value"],
                    "year": int(item["date"]),
                    "value": item["value"]
                }
        
        return None
        
    except Exception as e:
        print(f"✗ Exception fetching data for {country_code} {year}: {e}")
        return None


# ============================================================================
# FUNCTION 4: Store Economic Data in Database
# ============================================================================

def store_economic_data(conn, country_id, economic_dict):
    """
    Inserts economic data into database.
    
    Input:
        - conn: SQLite connection
        - country_id: int
        - economic_dict: dict (from fetch_indicator)
    Output: bool (True if inserted, False if duplicate)
    Purpose: Stores economic data, avoiding duplicates
    """
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT INTO economic_data 
            (country_id, indicator_id, indicator_name, year, value)
            VALUES (?, ?, ?, ?, ?)
        """, (
            country_id,
            economic_dict["indicator_id"],
            economic_dict["indicator_name"],
            economic_dict["year"],
            economic_dict["value"]
        ))
        
        conn.commit()
        return True
        
    except sqlite3.IntegrityError:
        # Duplicate entry - UNIQUE constraint violation
        return False


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """
    Orchestrates the entire economic data collection process.
    Respects 25-item limit per run.
    
    Purpose: 
        - Connects to database
        - Creates table if needed
        - Fetches economic data from World Bank API
        - Stores data respecting 25-item limit
        - Tracks progress
    """
    print("=" * 60)
    print("SI 201 Final Project - World Bank Data Collection")
    print("=" * 60)
    print()
    
    # Connect to database
    conn = sqlite3.connect(DATABASE_PATH)
    print(f"✓ Connected to database: {DATABASE_PATH}")
    
    # Create economic_data table
    create_economic_table(conn)
    print()
    
    # Check how many economic records already exist
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM economic_data')
    existing_count = cursor.fetchone()[0]
    print(f"Current economic data records in database: {existing_count}")
    
    # How many items to collect this run (fixed cap)
    items_to_collect_this_run = 25
    print(f"Will collect up to {items_to_collect_this_run} new records this run (if available).")
    print()
    print("-" * 60)
    print()
    
    # Track which (country, year) combinations have already been collected
    cursor.execute('''
        SELECT c.country_code_3, e.year 
        FROM economic_data e
        JOIN countries c ON e.country_id = c.country_id
        WHERE e.indicator_id = ?
    ''', (INDICATOR_ID,))
    collected_pairs = set(cursor.fetchall())
    
    # Collect new economic data
    items_collected = 0
    
    for country_code, year in DATA_TO_COLLECT:
        # Stop if we've collected enough items this run
        if items_collected >= items_to_collect_this_run:
            break
        
        # Skip if we already have this (country, year) combination
        if (country_code, year) in collected_pairs:
            continue
        
        print(f"Fetching: {country_code} {year}...", end=" ")
        
        # Fetch economic data from API
        economic_data = fetch_indicator(INDICATOR_ID, country_code, year)
        
        if economic_data is None:
            print("✗ No data available")
            continue
        
        # Get country_id from countries table
        try:
            country_id = get_country_id(conn, country_code)
        except ValueError as e:
            print(f"✗ {e}")
            continue
        
        # Store economic data
        inserted = store_economic_data(conn, country_id, economic_data)
        
        if inserted:
            items_collected += 1
            value_str = f"${economic_data['value']:,.2f}" if economic_data['value'] else "N/A"
            print(f"✓ Stored ({items_collected}/{items_to_collect_this_run})")
            print(f"   GDP per capita: {value_str}")
        else:
            print("✗ Duplicate (skipped)")
    
    # Final summary
    print()
    print("-" * 60)
    print()
    cursor.execute('SELECT COUNT(*) FROM economic_data')
    final_count = cursor.fetchone()[0]
    print("✓ Data collection complete!")
    print(f"  - New records added this run: {items_collected}")
    print(f"  - Total records in database: {final_count}")
    
    if final_count >= 100:
        print()
        print("🎉 Successfully collected 100+ economic data points!")
    else:
        remaining = 100 - final_count
        runs_needed = (remaining + 24) // 25
        print(f"  - Need {runs_needed} more run(s) to reach 100 records")
    
    # Close database connection
    conn.close()
    print()
    print("=" * 60)


if __name__ == "__main__":
    main()