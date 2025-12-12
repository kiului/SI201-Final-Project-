"""
SI 201 Final Project - Data Visualizations
Team: Curious Finders

This script creates 5 visualizations using matplotlib.
Each visualization shows data for the countries in the study.
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from adjustText import adjust_text

DATABASE_PATH = "final_data.db"


def connect_to_database(db_path=DATABASE_PATH):
    """Connects to the SQLite database."""
    conn = sqlite3.connect(db_path)
    return conn


def visualization_1_temperature_bar_chart(conn, output_path='viz1_temperature.png'):
    """
    Visualization 1: Average Temperature by Country (Bar Chart)
    """
    # Using LEFT JOIN to ensure all countries are pulled
    query = """
    SELECT 
        c.country_name,
        AVG(w.temperature) as avg_temperature
    FROM countries c
    JOIN weather_data w ON c.country_id = w.country_id
    LEFT JOIN air_quality_data a ON c.country_id = a.country_id
    LEFT JOIN economic_data e ON c.country_id = e.country_id
    GROUP BY c.country_name
    ORDER BY avg_temperature DESC
    """
    
    df = pd.read_sql_query(query, conn)
    
    fig, ax = plt.subplots(figsize=(10, 8))

    # Create color gradient from blue (cold) to red (hot)
    norm = plt.Normalize(df['avg_temperature'].min(), df['avg_temperature'].max())
    colors = plt.cm.coolwarm(norm(df['avg_temperature']))

    y_pos = np.arange(len(df))
    bars = ax.barh(y_pos, df['avg_temperature'], color=colors, edgecolor='black', linewidth=0.5)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(df['country_name'])
    ax.set_xlabel('Average Temperature (°C)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Country', fontsize=12, fontweight='bold')
    ax.set_title('Average Temperature by Country', fontsize=14, fontweight='bold', pad=20)
    
    for i, (bar, temp) in enumerate(zip(bars, df['avg_temperature'])):
        ax.text(temp + 0.2, i, f'{temp:.1f}°C', va='center', fontsize=9, fontweight='bold')
    
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Visualization 1 saved to {output_path}")
    plt.close()


def visualization_2_air_quality_bar_chart(conn, output_path='viz2_air_quality.png'):
    """
    Visualization 2: Average PM2.5 Air Quality by Country (Bar Chart)
    """
    # FIX: a.pm25_value -> a.value
    # Using LEFT JOIN to show countries even if they have no PM2.5 data
    query = """
    SELECT 
        c.country_name,
        AVG(a.value) as avg_pm25
    FROM countries c
    LEFT JOIN air_quality_data a ON c.country_id = a.country_id
    GROUP BY c.country_name
    ORDER BY avg_pm25 ASC
    """
    
    df = pd.read_sql_query(query, conn)
    # Fill missing values with 0 for the sake of the bar chart, or drop them
    df['avg_pm25'] = df['avg_pm25'].fillna(0)
    
    fig, ax = plt.subplots(figsize=(10, 8))
    
    pm25_values = df['avg_pm25'].values
    colors = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(pm25_values)))
    
    y_pos = np.arange(len(df))
    bars = ax.barh(y_pos, df['avg_pm25'], color=colors, edgecolor='black', linewidth=0.5)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(df['country_name'])
    ax.set_xlabel('Average PM2.5 Level (µg/m³)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Country', fontsize=12, fontweight='bold')
    ax.set_title('Average PM2.5 Air Quality by Country\n(Lower is Better)', fontsize=14, fontweight='bold', pad=20)
    
    ax.axvline(x=15, color='red', linestyle='--', linewidth=2, alpha=0.7, label='WHO Guideline (15 µg/m³)')
    ax.legend(loc='lower right', fontsize=9)
    
    for i, (bar, pm25) in enumerate(zip(bars, df['avg_pm25'])):
        label = f'{pm25:.1f}' if pm25 > 0 else 'No Data'
        ax.text(pm25 + 0.5, i, label, va='center', fontsize=9, fontweight='bold')
    
    ax.grid(axis='x', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Visualization 2 saved to {output_path}")
    plt.close()


def visualization_3_gdp_scatter(conn, output_path='viz3_gdp_vs_pollution.png'):
    """
    Visualization 3: GDP vs Air Quality Scatter Plot
    """
    # FIX: a.pm25_value -> a.value
    query = """
    SELECT 
        c.country_name,
        AVG(a.value) as avg_pm25,
        e.value as gdp_per_capita
    FROM countries c
    JOIN air_quality_data a ON c.country_id = a.country_id
    JOIN economic_data e ON c.country_id = e.country_id
    GROUP BY c.country_name
    """

    df = pd.read_sql_query(query, conn)
    if df.empty:
        print("! Skipping Visualization 3: No matching data found for GDP and PM2.5")
        return

    fig, ax = plt.subplots(figsize=(12, 8))

    # Normalize colors based on PM2.5
    norm = plt.Normalize(df['avg_pm25'].min(), df['avg_pm25'].max())
    colors = plt.cm.RdYlGn_r(norm(df['avg_pm25']))

    ax.scatter(df['gdp_per_capita'], df['avg_pm25'], s=200, c=colors, alpha=0.7, edgecolors='black', linewidth=1.2)

    texts = []
    for idx, row in df.iterrows():
        texts.append(ax.text(row['gdp_per_capita'], row['avg_pm25'], row['country_name'], 
                             fontsize=9, fontweight='bold', bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.3)))

    adjust_text(texts, expand_points=(1.2, 1.4), arrowprops=dict(arrowstyle="-", color='gray', lw=0.6))

    ax.set_xlabel('GDP Per Capita (USD)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Average PM2.5 Level (µg/m³)', fontsize=12, fontweight='bold')
    ax.set_title('GDP Per Capita vs Air Quality (PM2.5)', fontsize=14, fontweight='bold', pad=20)
    ax.axhline(y=15, color='red', linestyle='--', linewidth=1.3, alpha=0.6, label='WHO PM2.5 Guideline (15 µg/m³)')
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(True, alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Visualization 3 saved to {output_path}")
    plt.close()


def visualization_4_gdp_trend_top_polluters(conn, output_path='viz4_gdp_trend.png'):
    """
    Visualization 4: GDP Trend Over Time for Top 3 Most Polluting Countries
    """
    query = """
    SELECT 
        c.country_name,
        e.year,
        e.value AS gdp_per_capita
    FROM countries c
    JOIN economic_data e ON c.country_id = e.country_id
    WHERE c.country_name IN ('India', 'China', 'South Korea')
    ORDER BY e.year
    """
    df = pd.read_sql_query(query, conn)

    if df.empty:
        print("! Skipping Visualization 4: No GDP trend data found for selected countries")
        return

    fig, ax = plt.subplots(figsize=(11, 7))

    for country in df['country_name'].unique():
        country_df = df[df['country_name'] == country]
        ax.plot(country_df['year'], country_df['gdp_per_capita'], marker='o', linewidth=2.5, label=country)

    ax.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax.set_ylabel('GDP Per Capita (USD)', fontsize=12, fontweight='bold')
    ax.set_title('GDP Per Capita Trends Over Time', fontsize=14, fontweight='bold', pad=20)
    ax.legend(title='Country')
    ax.grid(True, alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Visualization 4 saved to {output_path}")
    plt.close()


def visualization_5_temp_vs_pm25(conn, output_path='viz5_temp_vs_pm25.png'):
    """
    Visualization 5: Average Temperature vs PM2.5 (Scatter Plot)
    """
    # FIX: a.pm25_value -> a.value
    query = """
    SELECT 
        c.country_name,
        AVG(w.temperature) as avg_temperature,
        AVG(a.value) as avg_pm25
    FROM countries c
    JOIN weather_data w ON c.country_id = w.country_id
    JOIN air_quality_data a ON c.country_id = a.country_id
    GROUP BY c.country_name
    """
    df = pd.read_sql_query(query, conn)

    if df.empty:
        print("! Skipping Visualization 5: No matching data found for Temperature and PM2.5")
        return

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.scatter(df['avg_temperature'], df['avg_pm25'], s=180, color="#00B894", edgecolors='black', linewidth=1, alpha=0.75)

    for _, row in df.iterrows():
        ax.text(row['avg_temperature'], row['avg_pm25'] + 1, row['country_name'], fontsize=9, ha='center')

    ax.set_xlabel('Average Temperature (°C)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Average PM2.5 Level (µg/m³)', fontsize=12, fontweight='bold')
    ax.set_title('Average Temperature vs Air Pollution (PM2.5)', fontsize=14, fontweight='bold', pad=20)
    ax.axhline(15, color='red', linestyle='--', alpha=0.6, label='WHO PM2.5 Guideline')
    ax.legend()
    ax.grid(True, alpha=0.3, linestyle='--')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Visualization 5 saved to {output_path}")
    plt.close()


def main():
    """Main function that creates all visualizations."""
    print("=" * 70)
    print("SI 201 Final Project - Data Visualizations")
    print("Team: Curious Finders")
    print("=" * 70)
    
    conn = connect_to_database()
    print("✓ Connected to database\n")
    
    visualization_1_temperature_bar_chart(conn)
    visualization_2_air_quality_bar_chart(conn)
    visualization_3_gdp_scatter(conn)
    visualization_4_gdp_trend_top_polluters(conn)
    visualization_5_temp_vs_pm25(conn)
    
    conn.close()
    print("\n" + "=" * 70)
    print("All visualizations processed!")
    print("=" * 70)


if __name__ == "__main__":
    main()