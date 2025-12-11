"""
SI 201 Final Project - Data Visualization
This script creates 3 visualizations from the calculated data.
All visualizations use matplotlib and go beyond the lecture examples.
Saves as PNG files for easy inclusion in reports.
"""

import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

DATABASE_PATH = "final_data.db"


def connect_database():
    """Connect to the SQLite database."""
    return sqlite3.connect(DATABASE_PATH)


def create_visualization_1(conn):
    """
    VISUALIZATION 1: Grouped Bar Chart - Average Temperature by Country
    Goes beyond lecture: Uses grouped bars with custom colors and styling
    """
    cursor = conn.cursor()
    
    # Get temperature data with humidity - ALL countries
    query = """
        SELECT c.country_name, 
               ROUND(AVG(w.temperature), 2) as avg_temp,
               ROUND(AVG(w.humidity), 2) as avg_humidity
        FROM weather_data w
        JOIN countries c ON w.country_id = c.country_id
        GROUP BY c.country_name
        ORDER BY avg_temp DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Create DataFrame
    df = pd.DataFrame(results, columns=['Country', 'Temperature', 'Humidity'])
    
    print(f"   Creating chart with {len(df)} countries")
    
    # Create figure and axis with larger width for all countries
    fig, ax = plt.subplots(figsize=(16, 8))
    
    # Set the width of bars and positions
    x = np.arange(len(df['Country']))
    width = 0.35
    
    # Create bars
    bars1 = ax.bar(x - width/2, df['Temperature'], width, label='Temperature (°C)',
                   color='#FF7F0E', edgecolor='black', linewidth=0.7)
    bars2 = ax.bar(x + width/2, df['Humidity'], width, label='Humidity (%)',
                   color='#1F77B4', edgecolor='black', linewidth=0.7)
    
    # Add value labels on bars
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}',
                   ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # Customize the plot
    ax.set_xlabel('Country', fontsize=14, fontweight='bold')
    ax.set_ylabel('Value', fontsize=14, fontweight='bold')
    ax.set_title('Average Temperature and Humidity by Country', 
                fontsize=16, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(df['Country'], rotation=45, ha='right')
    ax.legend(loc='upper right', fontsize=12, framealpha=0.9)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.set_facecolor('#F5F5F5')
    
    plt.tight_layout()
    plt.savefig('visualization_1_temperature_humidity.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✓ Visualization 1 saved: visualization_1_temperature_humidity.png")


def create_visualization_2(conn):
    """
    VISUALIZATION 2: Scatter Plot - GDP vs Pollution with Temperature as Size
    Goes beyond lecture: Uses bubble chart with 3 variables and custom colors
    """
    cursor = conn.cursor()
    
    # Get combined data from all tables - ALL countries
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
    
    # Create DataFrame
    df = pd.DataFrame(results, columns=['Country', 'Temperature', 'PM2.5', 'GDP'])
    
    print(f"   Creating chart with {len(df)} countries")
    
    # Create figure and axis with larger size
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Create color map
    colors = plt.cm.Set3(np.linspace(0, 1, len(df)))
    
    # Create scatter plot with bubble sizes based on temperature
    scatter = ax.scatter(df['GDP'], df['PM2.5'], 
                        s=df['Temperature']*20,  # Size based on temperature
                        c=colors, 
                        alpha=0.6, 
                        edgecolors='black', 
                        linewidth=1.5)
    
    # Add country labels
    for idx, row in df.iterrows():
        ax.annotate(row['Country'], 
                   (row['GDP'], row['PM2.5']),
                   xytext=(5, 5), 
                   textcoords='offset points',
                   fontsize=10,
                   fontweight='bold')
    
    # Customize the plot
    ax.set_xlabel('GDP per Capita (USD)', fontsize=14, fontweight='bold')
    ax.set_ylabel('PM2.5 Pollution (µg/m³)', fontsize=14, fontweight='bold')
    ax.set_title('Economic Development vs Air Pollution\n(Bubble size = Average Temperature)', 
                fontsize=16, fontweight='bold', pad=20)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_facecolor('#F8F8F8')
    
    # Format x-axis to show currency
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
    
    plt.tight_layout()
    plt.savefig('visualization_2_gdp_pollution_bubble.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✓ Visualization 2 saved: visualization_2_gdp_pollution_bubble.png")


def create_visualization_3(conn):
    """
    VISUALIZATION 3: Stacked Bar Chart - Pollution Parameters by Country
    Goes beyond lecture: Uses stacked bars with multiple parameters and custom colors
    """
    cursor = conn.cursor()
    
    # Get pollution data for all parameters - ensure we get all 10 countries
    query = """
        SELECT c.country_name,
               a.parameter,
               ROUND(AVG(a.value), 2) as avg_value
        FROM air_quality_data a
        JOIN countries c ON a.country_id = c.country_id
        GROUP BY c.country_name, a.parameter
        ORDER BY c.country_name, a.parameter
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Create DataFrame
    df = pd.DataFrame(results, columns=['Country', 'Parameter', 'Value'])
    
    # Pivot data for stacked bar chart
    df_pivot = df.pivot(index='Country', columns='Parameter', values='Value').fillna(0)
    
    # Make sure we have all 10 countries - if not all countries have data, this will still show them
    print(f"   Creating chart with {len(df_pivot)} countries")
    
    # Create figure and axis with larger width for all countries
    fig, ax = plt.subplots(figsize=(16, 8))
    
    # Define colors for each parameter
    colors = {
        'pm25': '#E41A1C',  # Red
        'no2': '#377EB8',   # Blue
        'o3': '#4DAF4A'     # Green
    }
    
    # Create stacked bar chart
    bottom = np.zeros(len(df_pivot))
    
    for param in ['pm25', 'no2', 'o3']:
        if param in df_pivot.columns:
            param_name = param.upper()
            bars = ax.bar(df_pivot.index, df_pivot[param], 
                         bottom=bottom, 
                         label=param_name,
                         color=colors[param],
                         edgecolor='black',
                         linewidth=0.7)
            
            # Add value labels inside bars
            for i, (bar, value) in enumerate(zip(bars, df_pivot[param])):
                if value > 5:  # Only show label if value is significant
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., 
                           bottom[i] + height/2,
                           f'{value:.1f}',
                           ha='center', va='center', 
                           fontsize=9, fontweight='bold',
                           color='white')
            
            bottom += df_pivot[param]
    
    # Customize the plot
    ax.set_xlabel('Country', fontsize=14, fontweight='bold')
    ax.set_ylabel('Concentration (µg/m³)', fontsize=14, fontweight='bold')
    ax.set_title('Air Quality Parameters by Country\nAverage pollution levels (µg/m³)', 
                fontsize=16, fontweight='bold', pad=20)
    ax.legend(title='Pollutant', loc='upper right', fontsize=12, framealpha=0.9)
    plt.xticks(rotation=45, ha='right')
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.set_facecolor('#F8F8F8')
    
    plt.tight_layout()
    plt.savefig('visualization_3_pollution_stacked.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("✓ Visualization 3 saved: visualization_3_pollution_stacked.png")


def main():
    """
    Main function to create all visualizations.
    """
    print("=" * 80)
    print("SI 201 Final Project - Data Visualization")
    print("=" * 80)
    print()
    
    # Connect to database
    conn = connect_database()
    print(f"✓ Connected to database: {DATABASE_PATH}")
    print()
    
    # Create visualizations
    print("Creating visualizations...")
    print()
    
    print("1. Creating grouped bar chart (Temperature & Humidity)...")
    create_visualization_1(conn)
    
    print("2. Creating bubble chart (GDP vs Pollution)...")
    create_visualization_2(conn)
    
    print("3. Creating stacked bar chart (Pollution Parameters)...")
    create_visualization_3(conn)
    
    # Close connection
    conn.close()
    
    print()
    print("=" * 80)
    print("✓ All visualizations created successfully!")
    print()
    print("PNG files created:")
    print("  - visualization_1_temperature_humidity.png")
    print("  - visualization_2_gdp_pollution_bubble.png")
    print("  - visualization_3_pollution_stacked.png")
    print()
    print("All images saved at 300 DPI for high-quality printing.")
    print("=" * 80)


if __name__ == "__main__":
    main()