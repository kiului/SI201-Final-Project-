"""
SI 201 Final Project - Data Analysis
Team: Curious Finders

This script performs 3 calculations by joining all database tables.
Each calculation writes results to a text file.
"""

import sqlite3
import pandas as pd


DATABASE_PATH = "final_data.db"


def connect_to_database(db_path=DATABASE_PATH):
    """
    Connects to the SQLite database.
    
    Input: str (database path)
    Output: SQLite connection object
    Purpose: Establishes connection to database
    """
    conn = sqlite3.connect(db_path)
    return conn


def calculation_1_avg_temp_by_country(conn):
    """
    Calculation 1: Average Temperature by Country
    
    Joins all 4 tables and calculates average temperature for each country.
    
    Input: SQLite connection
    Output: DataFrame with country_name and avg_temperature
    Purpose: Find which countries have the warmest/coldest climates
    """
    query = """
    SELECT 
        c.country_name,
        AVG(w.temperature) as avg_temperature
    FROM countries c
    JOIN weather_data w ON c.country_id = w.country_id
    JOIN air_quality_data a ON c.country_id = a.country_id
    JOIN economic_data e ON c.country_id = e.country_id
    GROUP BY c.country_name
    ORDER BY avg_temperature DESC
    """
    
    df = pd.read_sql_query(query, conn)
    return df


def calculation_2_avg_pm25_by_country(conn):
    """
    Calculation 2: Average Air Quality (PM2.5) by Country
    
    Joins all 4 tables and calculates average PM2.5 pollution for each country.
    
    Input: SQLite connection
    Output: DataFrame with country_name and avg_pm25
    Purpose: Find which countries have the cleanest/most polluted air
    """
    query = """
    SELECT 
        c.country_name,
        AVG(a.value) as avg_pm25
    FROM countries c
    JOIN air_quality_data a ON c.country_id = a.country_id
    JOIN weather_data w ON c.country_id = w.country_id
    JOIN economic_data e ON c.country_id = e.country_id
    WHERE a.parameter = 'pm25'
    GROUP BY c.country_name
    ORDER BY avg_pm25 ASC
    """
    
    df = pd.read_sql_query(query, conn)
    return df


def calculation_3_gdp_per_country(conn):
    """
    Calculation 3: Average GDP Per Capita by Country (2013–2024)
    
    Joins all 4 tables and calculates the average GDP per capita
    across the selected years for each country.
    """
    query = """
    SELECT 
        c.country_name,
        ROUND(AVG(e.value), 2) AS avg_gdp_per_capita
    FROM countries c
    JOIN economic_data e ON c.country_id = e.country_id
    JOIN weather_data w ON c.country_id = w.country_id
    JOIN air_quality_data a ON c.country_id = a.country_id
    WHERE e.indicator_id = 'NY.GDP.PCAP.CD'
      AND e.year BETWEEN 2014 AND 2023
    GROUP BY c.country_name
    ORDER BY avg_gdp_per_capita DESC;
    """
    
    df = pd.read_sql_query(query, conn)
    return df


def write_results_to_file(df1, df2, df3, output_path='calculation_results.txt'):
    """
    Writes all calculation results to a text file.
    
    Input: 
        - df1, df2, df3: DataFrames from calculations
        - output_path: str (file path)
    Output: None (writes to file)
    Purpose: Save calculation results as required by project
    """
    with open(output_path, 'w') as f:
        f.write("=" * 70 + "\n")
        f.write("SI 201 FINAL PROJECT - CALCULATION RESULTS\n")
        f.write("Team: Curious Finders\n")
        f.write("=" * 70 + "\n\n")
        
        # Calculation 1
        f.write("CALCULATION 1: Average Temperature by Country\n")
        f.write("-" * 70 + "\n")
        f.write(df1.to_string(index=False))
        f.write("\n\n")
        
        # Calculation 2
        f.write("CALCULATION 2: Average PM2.5 Air Quality by Country\n")
        f.write("-" * 70 + "\n")
        f.write(df2.to_string(index=False))
        f.write("\n\n")
        
        # Calculation 3
        f.write("CALCULATION 3: Average GDP Per Capita by Country (2013–2024)\n")
        f.write("-" * 70 + "\n")
        f.write(df3.to_string(index=False))
        f.write("\n\n")

        f.write("End of Results\n")

    
    print(f"✓ Results written to {output_path}")


def main():
    """
    Main function that runs all calculations and exports results.
    
    Purpose:
        - Connects to database
        - Runs all 3 calculations (each joins all 4 tables)
        - Writes results to text file
        - Displays results to console
    """
    print("=" * 70)
    print("SI 201 Final Project - Data Analysis")
    print("Team: Curious Finders")
    print("=" * 70)
    print()
    
    # Connect to database
    conn = connect_to_database()
    print("✓ Connected to database")
    print()
    
    # Run Calculation 1
    print("Running Calculation 1: Average Temperature by Country...")
    df_temp = calculation_1_avg_temp_by_country(conn)
    print(df_temp)
    print()
    
    # Run Calculation 2
    print("Running Calculation 2: Average PM2.5 by Country...")
    df_pm25 = calculation_2_avg_pm25_by_country(conn)
    print(df_pm25)
    print()
    
    # Run Calculation 3
    print("Running Calculation 3: GDP Per Capita by Country...")
    df_gdp = calculation_3_gdp_per_country(conn)
    print(df_gdp)
    print()
    
    # Write results to file
    write_results_to_file(df_temp, df_pm25, df_gdp)
    
    # Close connection
    conn.close()
    print()

    print("Analysis complete!")



if __name__ == "__main__":
    main()
