"""
SI 201 Final Project - Visualizations
Team: Curious Finders
Members: Hong Kiu Lui, Jessica Moon, Rachael Kim

This code creates all 3 visualizations as specified in the project proposal.
Uses Matplotlib for plotting and Seaborn for enhanced styling.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from data_analysis import connect_to_database, calculate_temp_and_air_quality
from data_analysis import calculate_gdp_vs_air_quality, calculate_multi_factor

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10


# VISUALIZATION 1: Temperature and Air Quality Comparison (Grouped Bar Chart)

def create_temp_air_quality_chart(df, output_path='viz1.png'):
    """
    Creates grouped horizontal bar chart showing temperature and PM2.5 by country.
    
    Input: 
        - df: DataFrame from Calculation 1 (country_name, avg_temperature, avg_pm25)
        - output_path: Path to save the image (default: 'viz1.png')
    Output: None (saves image file)
    Purpose: Compare which countries have comfortable temperatures AND clean air
    """
    try:
        # Sort by PM2.5 (cleanest first) and take top 15 if more than 15 countries
        df_sorted = df.sort_values('avg_pm25').head(15).copy()
        
        # Create figure with two y-axes (one for temp, one for PM2.5)
        fig, ax1 = plt.subplots(figsize=(14, 10))
        
        # Set positions for bars
        y_pos = np.arange(len(df_sorted))
        bar_height = 0.35
        
        # Plot temperature bars (blue)
        bars1 = ax1.barh(y_pos - bar_height/2, df_sorted['avg_temperature'], 
                         bar_height, label='Temperature (°C)', 
                         color='steelblue', alpha=0.8, edgecolor='navy')
        
        # Create second y-axis for PM2.5
        ax2 = ax1.twiny()
        
        # Plot PM2.5 bars (red/orange)
        bars2 = ax2.barh(y_pos + bar_height/2, df_sorted['avg_pm25'], 
                         bar_height, label='PM2.5 (µg/m³)', 
                         color='coral', alpha=0.8, edgecolor='darkred')
        
        # Set country names on y-axis
        ax1.set_yticks(y_pos)
        ax1.set_yticklabels(df_sorted['country_name'])
        
        # Labels and titles
        ax1.set_xlabel('Temperature (°C)', fontsize=12, fontweight='bold')
        ax2.set_xlabel('PM2.5 Level (µg/m³)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Country', fontsize=12, fontweight='bold')
        
        plt.title('Temperature and Air Quality Comparison by Country\n' +
                 '(Top 15 Countries by Air Quality)',
                 fontsize=16, fontweight='bold', pad=20)
        
        # Add WHO reference line for PM2.5
        ax2.axvline(x=15, color='red', linestyle='--', linewidth=2, 
                   label='WHO Limit (15 µg/m³)', alpha=0.7)
        
        # Add value labels on bars
        for i, (bar1, bar2) in enumerate(zip(bars1, bars2)):
            # Temperature label
            width1 = bar1.get_width()
            ax1.text(width1 + 1, bar1.get_y() + bar1.get_height()/2, 
                    f'{width1:.1f}°C',
                    va='center', fontsize=9, color='navy')
            
            # PM2.5 label
            width2 = bar2.get_width()
            ax2.text(width2 + 1, bar2.get_y() + bar2.get_height()/2,
                    f'{width2:.1f}',
                    va='center', fontsize=9, color='darkred')
        
        # Combine legends
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, 
                  loc='lower right', fontsize=11, framealpha=0.95)
        
        # Grid
        ax1.grid(axis='x', alpha=0.3)
        
        # Adjust layout and save
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Visualization 1 saved: {output_path}")
        return True
        
    except Exception as e:
        print(f"✗ Error creating Visualization 1: {e}")
        return False


# VISUALIZATION 2: Wealth vs Air Quality (Scatter Plot)

def create_gdp_scatter(df, output_path='viz2.png'):
    """
    Creates scatter plot showing GDP per capita vs PM2.5 levels.
    
    Input:
        - df: DataFrame from Calculation 2 (country_name, avg_pm25, gdp_per_capita)
        - output_path: Path to save the image (default: 'viz2.png')
    Output: None (saves image file)
    Purpose: Show whether wealth correlates with air quality
    """
    try:
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Create color gradient based on PM2.5 (green = clean, red = polluted)
        colors = []
        for pm25 in df['avg_pm25']:
            if pm25 < 10:
                colors.append('green')
            elif pm25 < 15:
                colors.append('yellowgreen')
            elif pm25 < 25:
                colors.append('orange')
            else:
                colors.append('red')
        
        # Create scatter plot
        scatter = ax.scatter(df['gdp_per_capita'], df['avg_pm25'], 
                            c=colors, s=300, alpha=0.7, edgecolors='black', linewidth=1.5)
        
        # Add country labels
        for _, row in df.iterrows():
            ax.annotate(row['country_name'], 
                       (row['gdp_per_capita'], row['avg_pm25']),
                       xytext=(5, 5), textcoords='offset points',
                       fontsize=10, fontweight='bold',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='white', 
                                alpha=0.7, edgecolor='gray'))
        
        # Reference lines
        ax.axhline(y=15, color='red', linestyle='--', linewidth=2, 
                  label='WHO PM2.5 Limit (15 µg/m³)', alpha=0.7)
        ax.axvline(x=20000, color='blue', linestyle='--', linewidth=2,
                  label='Developed Country Threshold ($20,000)', alpha=0.7)
        
        # Add quadrant labels
        max_gdp = df['gdp_per_capita'].max()
        max_pm25 = df['avg_pm25'].max()
        
        ax.text(max_gdp * 0.75, max_pm25 * 0.9, 'Rich & Polluted',
               fontsize=12, fontweight='bold', alpha=0.5, 
               ha='center', va='center',
               bbox=dict(boxstyle='round,pad=0.5', facecolor='orange', alpha=0.3))
        
        ax.text(max_gdp * 0.75, max_pm25 * 0.1, 'Rich & Clean\n(Ideal)',
               fontsize=12, fontweight='bold', alpha=0.5,
               ha='center', va='center',
               bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgreen', alpha=0.3))
        
        ax.text(max_gdp * 0.25, max_pm25 * 0.9, 'Poor & Polluted',
               fontsize=12, fontweight='bold', alpha=0.5,
               ha='center', va='center',
               bbox=dict(boxstyle='round,pad=0.5', facecolor='lightcoral', alpha=0.3))
        
        ax.text(max_gdp * 0.25, max_pm25 * 0.1, 'Poor & Clean',
               fontsize=12, fontweight='bold', alpha=0.5,
               ha='center', va='center',
               bbox=dict(boxstyle='round,pad=0.5', facecolor='lightyellow', alpha=0.3))
        
        # Labels and title
        ax.set_xlabel('GDP per Capita (US Dollars)', fontsize=13, fontweight='bold')
        ax.set_ylabel('Average PM2.5 Level (µg/m³)', fontsize=13, fontweight='bold')
        ax.set_title('Wealth vs Air Quality: Do Rich Countries Have Cleaner Air?',
                    fontsize=16, fontweight='bold', pad=20)
        
        # Legend
        ax.legend(loc='upper left', fontsize=11, framealpha=0.95)
        
        # Grid
        ax.grid(True, alpha=0.3)
        
        # Format x-axis as currency
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Adjust layout and save
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Visualization 2 saved: {output_path}")
        return True
        
    except Exception as e:
        print(f"✗ Error creating Visualization 2: {e}")
        return False


# VISUALIZATION 3: Multi-Factor Analysis (Scatter Plot with Color)

def create_multi_factor_scatter(df, output_path='viz3.png'):
    """
    Creates scatter plot with GDP, PM2.5, and temperature (shown as colors).
    
    Input:
        - df: DataFrame from Calculation 3 
              (country_name, avg_temp, avg_pm25, gdp_per_capita)
        - output_path: Path to save the image (default: 'viz3.png')
    Output: None (saves image file)
    Purpose: See patterns between economic development, air quality, and climate
    """
    try:
        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Create scatter plot with temperature as color
        scatter = ax.scatter(df['gdp_per_capita'], df['avg_pm25'], 
                            c=df['avg_temp'], s=300, alpha=0.7, 
                            cmap='coolwarm', edgecolors='black', linewidth=1.5,
                            vmin=df['avg_temp'].min(), vmax=df['avg_temp'].max())
        
        # Add colorbar for temperature
        cbar = plt.colorbar(scatter, ax=ax, pad=0.02)
        cbar.set_label('Average Temperature (°C)', fontsize=12, fontweight='bold')
        
        # Add country labels
        for _, row in df.iterrows():
            ax.annotate(row['country_name'], 
                       (row['gdp_per_capita'], row['avg_pm25']),
                       xytext=(5, 5), textcoords='offset points',
                       fontsize=10, fontweight='bold',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='white', 
                                alpha=0.8, edgecolor='gray'))
        
        # Reference line for WHO limit
        ax.axhline(y=15, color='red', linestyle='--', linewidth=2, 
                  label='WHO PM2.5 Limit (15 µg/m³)', alpha=0.7)
        
        # Labels and title
        ax.set_xlabel('GDP per Capita (US Dollars)', fontsize=13, fontweight='bold')
        ax.set_ylabel('Average PM2.5 Level (µg/m³)', fontsize=13, fontweight='bold')
        ax.set_title('Multi-Factor Analysis: GDP, Air Quality, and Climate\n' +
                    'Point Color = Temperature (Blue = Cold, Red = Hot)',
                    fontsize=16, fontweight='bold', pad=20)
        
        # Legend
        ax.legend(loc='upper left', fontsize=11, framealpha=0.95)
        
        # Grid
        ax.grid(True, alpha=0.3)
        
        # Format x-axis as currency
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))
        
        # Add text box with interpretation
        textstr = 'Ideal countries:\n• High GDP (right)\n• Low PM2.5 (bottom)\n• Moderate temp (yellow)'
        props = dict(boxstyle='round', facecolor='lightyellow', alpha=0.8, edgecolor='gray')
        ax.text(0.98, 0.98, textstr, transform=ax.transAxes, fontsize=11,
               verticalalignment='top', horizontalalignment='right', bbox=props)
        
        # Adjust layout and save
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Visualization 3 saved: {output_path}")
        return True
        
    except Exception as e:
        print(f"✗ Error creating Visualization 3: {e}")
        return False


def main():
    """
    Main execution function.
    Generates all 3 visualizations.
    """
    print("=" * 80)
    print("SI 201 FINAL PROJECT - VISUALIZATIONS")
    print("Team: Curious Finders")
    print("=" * 80)
    print()
    
    # Connect to database and get data
    print("Connecting to database and running calculations...")
    print()
    
    conn = connect_to_database()
    if conn is None:
        print("✗ Failed to connect to database. Exiting.")
        return
    
    # Get data for all visualizations
    calc1_df = calculate_temp_and_air_quality(conn)
    calc2_df = calculate_gdp_vs_air_quality(conn)
    calc3_df = calculate_multi_factor(conn)
    
    conn.close()
    
    # Check if all calculations succeeded
    if calc1_df is None or calc2_df is None or calc3_df is None:
        print()
        print("✗ One or more calculations failed. Cannot create visualizations.")
        return
    
    print()
    print("-" * 80)
    print()
    print("Creating visualizations...")
    print()
    
    # Create all visualizations
    viz1_success = create_temp_air_quality_chart(calc1_df, 'viz1.png')
    viz2_success = create_gdp_scatter(calc2_df, 'viz2.png')
    viz3_success = create_multi_factor_scatter(calc3_df, 'viz3.png')
    
    # Summary
    print()
    print("=" * 80)
    if viz1_success and viz2_success and viz3_success:
        print("✓ ALL VISUALIZATIONS COMPLETE!")
        print()
        print("Generated files:")
        print("  - viz1.png: Temperature and Air Quality Comparison (Bar Chart)")
        print("  - viz2.png: Wealth vs Air Quality (Scatter Plot)")
        print("  - viz3.png: Multi-Factor Analysis (Scatter Plot with Temperature)")
    else:
        print("✗ Some visualizations failed to generate.")
        print(f"  Visualization 1: {'✓' if viz1_success else '✗'}")
        print(f"  Visualization 2: {'✓' if viz2_success else '✗'}")
        print(f"  Visualization 3: {'✓' if viz3_success else '✗'}")
    print("=" * 80)


if __name__ == "__main__":
    main()