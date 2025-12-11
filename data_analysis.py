"""
SI 201 Final Project - Data Analysis
This script performs calculations on data from all tables in the database.
It uses database joins and writes calculated results to a text file.
"""

import sqlite3
import json
from datetime import datetime

DATABASE_PATH = "final_data.db"
OUTPUT_FILE = "calculated_data.txt"


def connect_database():
    """Connect to the SQLite database."""
    return sqlite3.connect(DATABASE_PATH)


def calculation_1_avg_temp_by_country(conn):
    """
    CALCULATION 1: Average temperature by country
    Uses: weather_data table + countries table (JOIN)
    """
    cursor = conn.cursor()
    
    query = """
        SELECT c.country_name, 
               ROUND(AVG(w.temperature), 2) as avg_temp,
               COUNT(w.id) as num_cities
        FROM weather_data w
        JOIN countries c ON w.country_id = c.country_id
        GROUP BY c.country_name
        ORDER BY avg_temp DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    return results


def calculation_2_avg_pollution_by_country(conn):
    """
    CALCULATION 2: Average PM2.5 levels by country
    Uses: air_quality_data table + countries table (JOIN)
    """
    cursor = conn.cursor()
    
    query = """
        SELECT c.country_name,
               ROUND(AVG(a.value), 2) as avg_pm25,
               COUNT(a.id) as num_measurements
        FROM air_quality_data a
        JOIN countries c ON a.country_id = c.country_id
        WHERE a.parameter = 'pm25'
        GROUP BY c.country_name
        ORDER BY avg_pm25 DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    return results


def calculation_3_avg_gdp_by_country(conn):
    """
    CALCULATION 3: Average GDP per capita by country (2014-2023)
    Uses: economic_data table + countries table (JOIN)
    """
    cursor = conn.cursor()
    
    query = """
        SELECT c.country_name,
               ROUND(AVG(e.value), 2) as avg_gdp,
               COUNT(e.id) as num_years
        FROM economic_data e
        JOIN countries c ON e.country_id = c.country_id
        GROUP BY c.country_name
        ORDER BY avg_gdp DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    return results


def calculation_4_combined_analysis(conn):
    """
    CALCULATION 4: Combined analysis across all tables
    Shows country with temperature, pollution, and GDP data together
    Uses: ALL tables with MULTIPLE JOINS
    """
    cursor = conn.cursor()
    
    query = """
        SELECT c.country_name,
               ROUND(AVG(w.temperature), 2) as avg_temp,
               ROUND(AVG(a.value), 2) as avg_pm25,
               ROUND(AVG(e.value), 2) as avg_gdp
        FROM countries c
        LEFT JOIN weather_data w ON c.country_id = w.country_id
        LEFT JOIN air_quality_data a ON c.country_id = a.country_id AND a.parameter = 'pm25'
        LEFT JOIN economic_data e ON c.country_id = e.country_id
        GROUP BY c.country_name
        HAVING avg_temp IS NOT NULL 
           AND avg_pm25 IS NOT NULL 
           AND avg_gdp IS NOT NULL
        ORDER BY c.country_name
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    return results


def calculation_5_pollution_parameter_counts(conn):
    """
    CALCULATION 5: Count of different pollution parameters by country
    Uses: air_quality_data table + countries table (JOIN)
    """
    cursor = conn.cursor()
    
    query = """
        SELECT c.country_name,
               a.parameter,
               COUNT(*) as measurement_count,
               ROUND(AVG(a.value), 2) as avg_value
        FROM air_quality_data a
        JOIN countries c ON a.country_id = c.country_id
        GROUP BY c.country_name, a.parameter
        ORDER BY c.country_name, a.parameter
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    return results


def write_results_to_file(calc1, calc2, calc3, calc4, calc5):
    """
    Write all calculated results to a text file.
    """
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("SI 201 FINAL PROJECT - CALCULATED DATA RESULTS\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        
        # Calculation 1
        f.write("CALCULATION 1: Average Temperature by Country\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Country':<20} {'Avg Temp (°C)':<15} {'# Cities':<10}\n")
        f.write("-" * 80 + "\n")
        for row in calc1:
            f.write(f"{row[0]:<20} {row[1]:<15} {row[2]:<10}\n")
        f.write("\n\n")
        
        # Calculation 2
        f.write("CALCULATION 2: Average PM2.5 Pollution by Country\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Country':<20} {'Avg PM2.5 (µg/m³)':<20} {'# Measurements':<15}\n")
        f.write("-" * 80 + "\n")
        for row in calc2:
            f.write(f"{row[0]:<20} {row[1]:<20} {row[2]:<15}\n")
        f.write("\n\n")
        
        # Calculation 3
        f.write("CALCULATION 3: Average GDP per Capita by Country (2014-2023)\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Country':<20} {'Avg GDP per Capita ($)':<25} {'# Years':<10}\n")
        f.write("-" * 80 + "\n")
        for row in calc3:
            f.write(f"{row[0]:<20} ${row[1]:>20,.2f} {row[2]:<10}\n")
        f.write("\n\n")
        
        # Calculation 4
        f.write("CALCULATION 4: Combined Analysis (All Tables with Multiple Joins)\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Country':<15} {'Avg Temp':<12} {'Avg PM2.5':<12} {'Avg GDP':<20}\n")
        f.write("-" * 80 + "\n")
        for row in calc4:
            f.write(f"{row[0]:<15} {row[1]:<12} {row[2]:<12} ${row[3]:>15,.2f}\n")
        f.write("\n\n")
        
        # Calculation 5
        f.write("CALCULATION 5: Pollution Parameter Distribution by Country\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Country':<20} {'Parameter':<12} {'Count':<10} {'Avg Value':<15}\n")
        f.write("-" * 80 + "\n")
        for row in calc5:
            f.write(f"{row[0]:<20} {row[1]:<12} {row[2]:<10} {row[3]:<15}\n")
        f.write("\n")
        f.write("=" * 80 + "\n")


def main():
    """
    Main function to run all calculations and write results to file.
    """
    print("=" * 80)
    print("SI 201 Final Project - Data Analysis")
    print("=" * 80)
    print()
    
    # Connect to database
    conn = connect_database()
    print(f"✓ Connected to database: {DATABASE_PATH}")
    print()
    
    # Perform calculations
    print("Performing calculations...")
    print()
    
    print("1. Calculating average temperature by country (with JOIN)...")
    calc1 = calculation_1_avg_temp_by_country(conn)
    print(f"   ✓ Found data for {len(calc1)} countries")
    
    print("2. Calculating average PM2.5 pollution by country (with JOIN)...")
    calc2 = calculation_2_avg_pollution_by_country(conn)
    print(f"   ✓ Found data for {len(calc2)} countries")
    
    print("3. Calculating average GDP per capita by country (with JOIN)...")
    calc3 = calculation_3_avg_gdp_by_country(conn)
    print(f"   ✓ Found data for {len(calc3)} countries")
    
    print("4. Performing combined analysis across all tables (MULTIPLE JOINS)...")
    calc4 = calculation_4_combined_analysis(conn)
    print(f"   ✓ Found complete data for {len(calc4)} countries")
    
    print("5. Counting pollution parameters by country (with JOIN)...")
    calc5 = calculation_5_pollution_parameter_counts(conn)
    print(f"   ✓ Found {len(calc5)} parameter measurements")
    
    print()
    print("-" * 80)
    print()
    
    # Write results to file
    print(f"Writing results to {OUTPUT_FILE}...")
    write_results_to_file(calc1, calc2, calc3, calc4, calc5)
    print(f"✓ Results written to {OUTPUT_FILE}")
    
    # Display sample results
    print()
    print("Sample Results:")
    print("-" * 80)
    print()
    print("All Countries by Average Temperature:")
    for i, row in enumerate(calc1, 1):
        print(f"   {i}. {row[0]}: {row[1]}°C ({row[2]} cities)")
    
    print()
    print("All Countries by Average PM2.5 Pollution:")
    for i, row in enumerate(calc2, 1):
        print(f"   {i}. {row[0]}: {row[1]} µg/m³ ({row[2]} measurements)")
    
    print()
    print("All Countries by Average GDP per Capita:")
    for i, row in enumerate(calc3, 1):
        print(f"   {i}. {row[0]}: ${row[1]:,.2f} ({row[2]} years)")
    
    # Close connection
    conn.close()
    
    print()
    print("=" * 80)
    print("✓ Data analysis complete!")
    print(f"✓ All results saved to: {OUTPUT_FILE}")
    print("=" * 80)


if __name__ == "__main__":
    main()