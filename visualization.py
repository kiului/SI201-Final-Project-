"""
SI 201 Final Project - Data Visualizations
Team: Curious Finders

This script creates 3 visualizations using matplotlib.
Each visualization shows data for all 10 countries in the study.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from adjustText import adjust_text

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


def visualization_1_temperature_bar_chart(conn, output_path='viz1_temperature.png'):
    """
    Visualization 1: Average Temperature by Country (Bar Chart)
    
    Creates a horizontal bar chart showing average temperature for all 10 countries.
    Uses custom colors (gradient from blue=cold to red=hot).
    
    Input: SQLite connection, output file path
    Output: None (saves PNG file)
    Purpose: Compare climate across all countries
    """
    # Get data from database
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
    
    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8))

    colors = "#4A90E2"

    y_pos = np.arange(len(df))
    
    # Create color gradient from blue (cold) to red (hot)
    temps = df['avg_temperature'].values
    
    # Create horizontal bar chart
    y_pos = np.arange(len(df))
    bars = ax.barh(y_pos, df['avg_temperature'], color=colors, edgecolor='black', linewidth=0.5)
    
    # Customize chart
    ax.set_yticks(y_pos)
    ax.set_yticklabels(df['country_name'])
    ax.set_xlabel('Average Temperature (°C)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Country', fontsize=12, fontweight='bold')
    ax.set_title('Average Temperature by Country\n(All 10 Countries)', 
                 fontsize=14, fontweight='bold', pad=20)
    
    # Add value labels on bars
    for i, (bar, temp) in enumerate(zip(bars, df['avg_temperature'])):
        ax.text(temp + 0.5, i, f'{temp:.1f}°C', 
                va='center', fontsize=9, fontweight='bold')
    
    # Add grid for readability
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Visualization 1 saved to {output_path}")
    plt.close()


def visualization_2_air_quality_bar_chart(conn, output_path='viz2_air_quality.png'):
    """
    Visualization 2: Average PM2.5 Air Quality by Country (Bar Chart)
    
    Creates a horizontal bar chart showing PM2.5 levels for all 10 countries.
    Uses green (clean) to red (polluted) color scheme.
    
    Input: SQLite connection, output file path
    Output: None (saves PNG file)
    Purpose: Compare air pollution across all countries
    """
    # Get data from database
    query = """
    SELECT 
        c.country_name,
        AVG(a.pm25_value) as avg_pm25
    FROM countries c
    JOIN air_quality_data a ON c.country_id = a.country_id
    JOIN weather_data w ON c.country_id = w.country_id
    JOIN economic_data e ON c.country_id = e.country_id
    GROUP BY c.country_name
    ORDER BY avg_pm25 ASC
    """
    
    df = pd.read_sql_query(query, conn)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Create color gradient from green (clean) to red (polluted)
    pm25_values = df['avg_pm25'].values
    colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(pm25_values)))
    
    # Create horizontal bar chart
    y_pos = np.arange(len(df))
    bars = ax.barh(y_pos, df['avg_pm25'], color=colors, edgecolor='black', linewidth=0.5)
    
    # Customize chart
    ax.set_yticks(y_pos)
    ax.set_yticklabels(df['country_name'])
    ax.set_xlabel('Average PM2.5 Level (µg/m³)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Country', fontsize=12, fontweight='bold')
    ax.set_title('Average PM2.5 Air Quality by Country\n(All 10 Countries - Lower is Better)', 
                 fontsize=14, fontweight='bold', pad=20)
    
    # Add WHO guideline reference line (15 µg/m³)
    ax.axvline(x=15, color='red', linestyle='--', linewidth=2, alpha=0.7, label='WHO Guideline (15 µg/m³)')
    ax.legend(loc='lower right', fontsize=9)
    
    # Add value labels on bars
    for i, (bar, pm25) in enumerate(zip(bars, df['avg_pm25'])):
        ax.text(pm25 + 1, i, f'{pm25:.1f}', 
                va='center', fontsize=9, fontweight='bold')
    
    # Add grid for readability
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Visualization 2 saved to {output_path}")
    plt.close()

def visualization_3_gdp_scatter(conn, output_path='viz3_gdp_vs_pollution.png'):
    """
    Visualization 3: GDP vs Air Quality Scatter Plot (Cleaned)
    
    - Dot size is constant (temperature removed)
    - Labels automatically adjust to avoid overlapping using adjustText
    - Coloring still based on PM2.5 levels
    """

    query = """
    SELECT 
        c.country_name,
        AVG(a.pm25_value) as avg_pm25,
        e.value as gdp_per_capita,
        AVG(w.temperature) as avg_temperature
    FROM countries c
    JOIN air_quality_data a ON c.country_id = a.country_id
    JOIN economic_data e ON c.country_id = e.country_id
    JOIN weather_data w ON c.country_id = w.country_id
    WHERE e.indicator_id = 'NY.GDP.PCAP.CD'
    GROUP BY c.country_name
    """

    df = pd.read_sql_query(query, conn)

    fig, ax = plt.subplots(figsize=(12, 8))

    # Constant dot size (no temperature scaling)
    dot_size = 200

    # Color points using PM2.5 gradient
    colors = plt.cm.RdYlGn_r(
        (df['avg_pm25'] - df['avg_pm25'].min()) /
        (df['avg_pm25'].max() - df['avg_pm25'].min())
    )

    # Scatter plot
    ax.scatter(
        df['gdp_per_capita'], 
        df['avg_pm25'], 
        s=dot_size, 
        c=colors,
        alpha=0.7,
        edgecolors='black',
        linewidth=1.2
    )

    # Collect all label objects for adjustText
    texts = []
    for idx, row in df.iterrows():
        texts.append(
            ax.text(
                row['gdp_per_capita'], row['avg_pm25'],
                row['country_name'],
                fontsize=9, fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.3)
            )
        )

    # Automatically adjust overlapping labels
    adjust_text(
        texts,
        expand_points=(1.2, 1.4),
        arrowprops=dict(arrowstyle="-", color='gray', lw=0.6)
    )

    # Axis labels and title
    ax.set_xlabel('GDP Per Capita (USD)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Average PM2.5 Level (µg/m³)', fontsize=12, fontweight='bold')
    ax.set_title(
        'GDP Per Capita vs Air Quality (PM2.5)\nLabels Automatically Adjusted',
        fontsize=14, fontweight='bold',
        pad=20
    )

    # Reference line: WHO safe limit
    ax.axhline(
        y=15, color='red', linestyle='--', linewidth=1.3, alpha=0.6,
        label='WHO PM2.5 Guideline (15 µg/m³)'
    )
    ax.legend(loc='upper right', fontsize=10)

    # Grid for readability
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Visualization 3 saved to {output_path}")
    plt.close()

def visualization_4_gdp_trend_top_polluters(conn, output_path='viz4_gdp_trend.png'):
    """
    Visualization 4: GDP Trend Over Time for Top 3 Most Polluting Countries
    
    Countries: India, China, South Korea
    Shows how GDP per capita changes over time.
    """

    query = """
    SELECT 
        c.country_name,
        e.year,
        e.value AS gdp_per_capita
    FROM countries c
    JOIN economic_data e ON c.country_id = e.country_id
    WHERE e.indicator_id = 'NY.GDP.PCAP.CD'
      AND c.country_name IN ('India', 'China', 'South Korea')
    ORDER BY e.year
    """

    df = pd.read_sql_query(query, conn)

    fig, ax = plt.subplots(figsize=(11, 7))

    # Plot GDP trend for each country
    for country in df['country_name'].unique():
        country_df = df[df['country_name'] == country]
        ax.plot(
            country_df['year'],
            country_df['gdp_per_capita'],
            marker='o',
            linewidth=2.5,
            label=country
        )

    ax.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax.set_ylabel('GDP Per Capita (USD)', fontsize=12, fontweight='bold')
    ax.set_title(
        'GDP Per Capita Trends Over Time\nTop 3 Most Polluting Countries',
        fontsize=14,
        fontweight='bold',
        pad=20
    )

    ax.legend(title='Country')
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Visualization 4 saved to {output_path}")
    plt.close()

def visualization_5_temp_vs_pm25(conn, output_path='viz5_temp_vs_pm25.png'):
    """
    Visualization 5: Average Temperature vs PM2.5 (Scatter Plot)
    
    Explores relationship between climate and air pollution.
    """

    query = """
    SELECT 
        c.country_name,
        AVG(w.temperature) as avg_temperature,
        AVG(a.pm25_value) as avg_pm25
    FROM countries c
    JOIN weather_data w ON c.country_id = w.country_id
    JOIN air_quality_data a ON c.country_id = a.country_id
    GROUP BY c.country_name
    """

    df = pd.read_sql_query(query, conn)

    fig, ax = plt.subplots(figsize=(10, 8))

    ax.scatter(
        df['avg_temperature'],
        df['avg_pm25'],
        s=180,
        color="#00B894",
        edgecolors='black',
        linewidth=1,
        alpha=0.75
    )

    # Label each point
    for _, row in df.iterrows():
        ax.text(
            row['avg_temperature'],
            row['avg_pm25'],
            row['country_name'],
            fontsize=9,
            ha='center',
            va='bottom'
        )

    ax.set_xlabel('Average Temperature (°C)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Average PM2.5 Level (µg/m³)', fontsize=12, fontweight='bold')
    ax.set_title(
        'Average Temperature vs Air Pollution (PM2.5)\n(All 10 Countries)',
        fontsize=14, fontweight='bold',
        pad=20
    )

    ax.axhline(15, color='red', linestyle='--', alpha=0.6, label='WHO PM2.5 Guideline')
    ax.legend()

    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Visualization 5 saved to {output_path}")
    plt.close()



def main():
    """
    Main function that creates all 3 visualizations.
    
    Purpose:
        - Connects to database
        - Creates 3 different visualizations
        - Saves each as PNG file
        - All visualizations show all 10 countries
    """
    print("=" * 70)
    print("SI 201 Final Project - Data Visualizations")
    print("Team: Curious Finders")
    print("=" * 70)
    print()
    
    # Connect to database
    conn = connect_to_database()
    print("✓ Connected to database")
    print()
    
    # Create Visualization 1
    print("Creating Visualization 1: Temperature Bar Chart...")
    visualization_1_temperature_bar_chart(conn)
    print()
    
    # Create Visualization 2
    print("Creating Visualization 2: Air Quality Bar Chart...")
    visualization_2_air_quality_bar_chart(conn)
    print()
    
    # Create Visualization 3
    print("Creating Visualization 3: GDP vs Pollution Scatter Plot...")
    visualization_3_gdp_scatter(conn)
    print()

    # Create Visualization 4
    print("Creating Visualization 4: GDP Trend for Top Polluting Countries...")
    visualization_4_gdp_trend_top_polluters(conn)
    print()

    # Create Visualization 5
    print("Creating Visualization 5: Temperature vs PM2.5 Scatter...")
    visualization_5_temp_vs_pm25(conn)
    print()



    
    # Close connection
    conn.close()
    
    print("=" * 70)
    print("All visualizations created successfully!")
    print("Files created:")
    print("  - viz1_temperature.png")
    print("  - viz2_air_quality.png")
    print("  - viz3_gdp_vs_pollution.png")
    print("  - viz4_gdp_trend.png")
    print("  - viz5_temp_vs_pm25.png")
    print("=" * 70)


if __name__ == "__main__":
    main()
