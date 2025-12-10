"""
SI 201 Final Project - Data Analysis
Team: Curious Finders
Members: Hong Kiu Lui, Jessica Moon, Rachael Kim

This script performs all calculations by JOINing data from all 4 tables.
It creates 3 calculations as specified in the project proposal.
"""

import sqlite3
import pandas as pd
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

DATABASE_PATH = "final_data.db"
OUTPUT_PATH = "results.txt"


# ============================================================================
# FUNCTION 1: Connect to Database
# ============================================================================

def connect_to_database(db_path='final_data.db'):
    """
    Connects to the SQLite database.
    
    Input: str (database path)
    Output: SQLite connection object
    Purpose: Establishes connection to database
    """
    try:
        conn = sqlite3.connect(db_path)
        print(f"✓ Connected to database: {db_path}")
        return conn
    except Exception as e:
        print(f"✗ Error connecting to database: {e}")
        return None


# ============================================================================
# FUNCTION 2: Calculation 1 - Average Temperature and Air Quality by Country
# ============================================================================

def calculate_temp_and_air_quality(conn):
    """
    Performs Calculation 1: Average temperature and PM2.5 per country.
    
    Input: SQLite connection
    Output: pandas DataFrame with columns: country_name, avg_temperature, avg_pm25
    Purpose: Determines which countries have comfortable climate and clean air
    
    Tables JOINed: All 4 tables (countries, weather_data, air_quality_data, economic_data)
    """
    query = """
    SELECT 
        c.country_name,
        ROUND(AVG(w.temperature), 2) as avg_temperature,
        ROUND(AVG(CASE WHEN aq.parameter = 'pm25' THEN aq.value END), 2) as avg_pm25
    FROM countries c
    INNER JOIN weather_data w ON c.country_id = w.country_id
    INNER JOIN air_quality_data aq ON c.country_id = aq.country_id
    INNER JOIN economic_data e ON c.country_id = e.country_id
    WHERE aq.parameter = 'pm25'
    GROUP BY c.country_id, c.country_name
    HAVING avg_pm25 IS NOT NULL
    ORDER BY avg_pm25 ASC
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print(f"✓ Calculation 1 complete: {len(df)} countries analyzed")
        return df
    except Exception as e:
        print(f"✗ Error in Calculation 1: {e}")
        return None


# ============================================================================
# FUNCTION 3: Calculation 2 - GDP per Capita vs Air Quality
# ============================================================================

def calculate_gdp_vs_air_quality(conn):
    """
    Performs Calculation 2: GDP per capita and average PM2.5 per country.
    
    Input: SQLite connection
    Output: pandas DataFrame with columns: country_name, avg_pm25, gdp_per_capita
    Purpose: Analyzes whether wealthier countries have cleaner or dirtier air
    
    Tables JOINed: All 4 tables (countries, weather_data, air_quality_data, economic_data)
    """
    query = """
    SELECT 
        c.country_name,
        ROUND(AVG(CASE WHEN aq.parameter = 'pm25' THEN aq.value END), 2) as avg_pm25,
        ROUND(MAX(CASE WHEN e.indicator_id = 'NY.GDP.PCAP.CD' 
                  THEN e.value END), 2) as gdp_per_capita
    FROM countries c
    INNER JOIN weather_data w ON c.country_id = w.country_id
    INNER JOIN air_quality_data aq ON c.country_id = aq.country_id
    INNER JOIN economic_data e ON c.country_id = e.country_id
    WHERE aq.parameter = 'pm25'
      AND e.indicator_id = 'NY.GDP.PCAP.CD'
    GROUP BY c.country_id, c.country_name
    HAVING avg_pm25 IS NOT NULL AND gdp_per_capita IS NOT NULL
    ORDER BY gdp_per_capita DESC
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print(f"✓ Calculation 2 complete: {len(df)} countries analyzed")
        return df
    except Exception as e:
        print(f"✗ Error in Calculation 2: {e}")
        return None


# ============================================================================
# FUNCTION 4: Calculation 3 - Multi-Factor Country Comparison
# ============================================================================

def calculate_multi_factor(conn):
    """
    Performs Calculation 3: Temperature, PM2.5, and GDP per country.
    
    Input: SQLite connection
    Output: pandas DataFrame with columns: 
            country_name, avg_temp, avg_pm25, gdp_per_capita
    Purpose: Comprehensive view of climate, air quality, and economic data
    
    Tables JOINed: All 4 tables (countries, weather_data, air_quality_data, economic_data)
    """
    query = """
    SELECT 
        c.country_name,
        ROUND(AVG(w.temperature), 2) as avg_temp,
        ROUND(AVG(CASE WHEN aq.parameter = 'pm25' THEN aq.value END), 2) as avg_pm25,
        ROUND(MAX(CASE WHEN e.indicator_id = 'NY.GDP.PCAP.CD' 
                  THEN e.value END), 2) as gdp_per_capita
    FROM countries c
    INNER JOIN weather_data w ON c.country_id = w.country_id
    INNER JOIN air_quality_data aq ON c.country_id = aq.country_id
    INNER JOIN economic_data e ON c.country_id = e.country_id
    WHERE aq.parameter = 'pm25'
      AND e.indicator_id = 'NY.GDP.PCAP.CD'
    GROUP BY c.country_id, c.country_name
    HAVING avg_temp IS NOT NULL 
       AND avg_pm25 IS NOT NULL 
       AND gdp_per_capita IS NOT NULL
    ORDER BY gdp_per_capita DESC, avg_pm25 ASC
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print(f"✓ Calculation 3 complete: {len(df)} countries analyzed")
        return df
    except Exception as e:
        print(f"✗ Error in Calculation 3: {e}")
        return None


# ============================================================================
# FUNCTION 5: Export Results to Text File
# ============================================================================

def export_results(dataframes_dict, output_path='results.txt'):
    """
    Exports all calculation results to a text file.
    
    Input: 
        - dataframes_dict: Dictionary of DataFrames {'calc1': df1, 'calc2': df2, ...}
        - output_path: Path to output file (default: 'results.txt')
    Output: None (writes to file)
    Purpose: Saves all calculation results for the final report
    """
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            # Header
            f.write("=" * 80 + "\n")
            f.write("SI 201 FINAL PROJECT - DATA ANALYSIS RESULTS\n")
            f.write("Team: Curious Finders\n")
            f.write("Members: Hong Kiu Lui, Jessica Moon, Rachael Kim\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            # Calculation 1
            if 'calc1' in dataframes_dict and dataframes_dict['calc1'] is not None:
                f.write("\n" + "=" * 80 + "\n")
                f.write("CALCULATION 1: Average Temperature and Air Quality by Country\n")
                f.write("=" * 80 + "\n")
                f.write("Purpose: Determine which countries have comfortable climate and clean air\n")
                f.write("Sorted by: PM2.5 level (cleanest air first)\n\n")
                
                df1 = dataframes_dict['calc1']
                f.write(f"{'Country':<25} {'Avg Temp (°C)':<20} {'Avg PM2.5 (µg/m³)':<20}\n")
                f.write("-" * 80 + "\n")
                for _, row in df1.iterrows():
                    f.write(f"{row['country_name']:<25} {row['avg_temperature']:<20} {row['avg_pm25']:<20}\n")
                
                f.write("\n")
                f.write("Key Findings:\n")
                cleanest = df1.iloc[0]
                most_polluted = df1.iloc[-1]
                f.write(f"  - Cleanest air: {cleanest['country_name']} (PM2.5: {cleanest['avg_pm25']} µg/m³)\n")
                f.write(f"  - Most polluted: {most_polluted['country_name']} (PM2.5: {most_polluted['avg_pm25']} µg/m³)\n")
                f.write(f"  - WHO recommended limit: 15 µg/m³\n")
                countries_below_limit = len(df1[df1['avg_pm25'] <= 15])
                f.write(f"  - Countries meeting WHO standard: {countries_below_limit} of {len(df1)}\n")
            
            # Calculation 2
            if 'calc2' in dataframes_dict and dataframes_dict['calc2'] is not None:
                f.write("\n\n" + "=" * 80 + "\n")
                f.write("CALCULATION 2: GDP per Capita vs Air Quality Comparison\n")
                f.write("=" * 80 + "\n")
                f.write("Purpose: Analyze whether wealthier countries have cleaner or dirtier air\n")
                f.write("Sorted by: GDP per capita (wealthiest first)\n\n")
                
                df2 = dataframes_dict['calc2']
                f.write(f"{'Country':<25} {'GDP per Capita (USD)':<25} {'Avg PM2.5 (µg/m³)':<20}\n")
                f.write("-" * 80 + "\n")
                for _, row in df2.iterrows():
                    f.write(f"{row['country_name']:<25} ${row['gdp_per_capita']:<24,.2f} {row['avg_pm25']:<20}\n")
                
                f.write("\n")
                f.write("Key Findings:\n")
                richest = df2.iloc[0]
                poorest = df2.iloc[-1]
                f.write(f"  - Wealthiest: {richest['country_name']} (GDP: ${richest['gdp_per_capita']:,.2f}, PM2.5: {richest['avg_pm25']})\n")
                f.write(f"  - Poorest: {poorest['country_name']} (GDP: ${poorest['gdp_per_capita']:,.2f}, PM2.5: {poorest['avg_pm25']})\n")
                
                # Correlation analysis
                correlation = df2['gdp_per_capita'].corr(df2['avg_pm25'])
                f.write(f"  - Correlation between GDP and PM2.5: {correlation:.3f}\n")
                if correlation > 0:
                    f.write(f"    (Positive: Wealthier countries tend to have MORE pollution)\n")
                else:
                    f.write(f"    (Negative: Wealthier countries tend to have LESS pollution)\n")
            
            # Calculation 3
            if 'calc3' in dataframes_dict and dataframes_dict['calc3'] is not None:
                f.write("\n\n" + "=" * 80 + "\n")
                f.write("CALCULATION 3: Multi-Factor Country Comparison\n")
                f.write("=" * 80 + "\n")
                f.write("Purpose: Comprehensive view combining climate, air quality, and economic data\n")
                f.write("Sorted by: GDP (descending), then PM2.5 (ascending)\n\n")
                
                df3 = dataframes_dict['calc3']
                f.write(f"{'Country':<20} {'Temp (°C)':<12} {'PM2.5':<12} {'GDP per Capita (USD)':<25}\n")
                f.write("-" * 80 + "\n")
                for _, row in df3.iterrows():
                    f.write(f"{row['country_name']:<20} {row['avg_temp']:<12} {row['avg_pm25']:<12} ${row['gdp_per_capita']:<24,.2f}\n")
                
                f.write("\n")
                f.write("Key Findings:\n")
                f.write("  Countries with 'ideal' conditions (high GDP, low PM2.5, comfortable temp):\n")
                
                # Define "ideal" as: GDP > median, PM2.5 < 15, temp between 10-25°C
                median_gdp = df3['gdp_per_capita'].median()
                ideal = df3[(df3['gdp_per_capita'] > median_gdp) & 
                           (df3['avg_pm25'] < 15) & 
                           (df3['avg_temp'] >= 10) & 
                           (df3['avg_temp'] <= 25)]
                
                if len(ideal) > 0:
                    for _, row in ideal.iterrows():
                        f.write(f"    - {row['country_name']}: {row['avg_temp']}°C, PM2.5: {row['avg_pm25']}, GDP: ${row['gdp_per_capita']:,.2f}\n")
                else:
                    f.write(f"    - No countries meet all 'ideal' criteria\n")
            
            # Footer
            f.write("\n\n" + "=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")
        
        print(f"✓ Results exported to: {output_path}")
        return True
        
    except Exception as e:
        print(f"✗ Error exporting results: {e}")
        return False


# ============================================================================
# FUNCTION 6: Main Execution
# ============================================================================

def main():
    """
    Main execution function.
    Runs all 3 calculations and exports results.
    """
    print("=" * 80)
    print("SI 201 FINAL PROJECT - DATA ANALYSIS")
    print("Team: Curious Finders")
    print("=" * 80)
    print()
    
    # Connect to database
    conn = connect_to_database(DATABASE_PATH)
    if conn is None:
        print("✗ Failed to connect to database. Exiting.")
        return
    
    print()
    print("-" * 80)
    print()
    
    # Perform all calculations
    print("Running calculations...")
    print()
    
    calc1_df = calculate_temp_and_air_quality(conn)
    calc2_df = calculate_gdp_vs_air_quality(conn)
    calc3_df = calculate_multi_factor(conn)
    
    # Close database connection
    conn.close()
    print()
    print("✓ Database connection closed")
    
    # Check if all calculations succeeded
    if calc1_df is None or calc2_df is None or calc3_df is None:
        print()
        print("✗ One or more calculations failed. Cannot export results.")
        return
    
    print()
    print("-" * 80)
    print()
    
    # Display preview of results
    print("PREVIEW OF RESULTS:")
    print()
    print("Calculation 1 - Top 3 countries by air quality:")
    print(calc1_df.head(3).to_string(index=False))
    print()
    print("Calculation 2 - Top 3 countries by GDP:")
    print(calc2_df.head(3).to_string(index=False))
    print()
    print("Calculation 3 - All countries (sorted by GDP and PM2.5):")
    print(calc3_df.to_string(index=False))
    
    print()
    print("-" * 80)
    print()
    
    # Export results
    print("Exporting results to file...")
    dataframes = {
        'calc1': calc1_df,
        'calc2': calc2_df,
        'calc3': calc3_df
    }
    
    success = export_results(dataframes, OUTPUT_PATH)
    
    if success:
        print()
        print("=" * 80)
        print("✓ DATA ANALYSIS COMPLETE!")
        print(f"✓ Results saved to: {OUTPUT_PATH}")
        print("=" * 80)
        
        # Return dataframes for use by visualizations.py
        return calc1_df, calc2_df, calc3_df
    else:
        print()
        print("✗ Failed to export results")
        return None, None, None


# ============================================================================
# RUN THE SCRIPT
# ============================================================================

if __name__ == "__main__":
    main()
    