"""
Test Weather Transformations Locally
Uses Pandas instead of PySpark - same logic, runs on your laptop!
"""

import pandas as pd
import json
import glob
from datetime import datetime
import os

# ============================================
# TRANSFORMATION FUNCTIONS (Pandas version)
# ============================================

def add_temperature_conversions(df):
    """Add Fahrenheit and Kelvin temperatures."""
    print("🌡️  Adding temperature conversions...")
    
    df['temperature_fahrenheit'] = round((df['temperature_celsius'] * 9/5) + 32, 1)
    df['temperature_kelvin'] = round(df['temperature_celsius'] + 273.15, 1)
    
    print("  ✅ Added Fahrenheit and Kelvin")
    return df

def add_comfort_classification(df):
    """Classify comfort level based on heat index."""
    print("😌 Classifying comfort levels...")
    
    def classify_comfort(heat_index):
        if heat_index < 27:
            return "Comfortable"
        elif heat_index < 32:
            return "Caution"
        elif heat_index < 41:
            return "Extreme Caution"
        elif heat_index < 54:
            return "Danger"
        else:
            return "Extreme Danger"
    
    df['comfort_level'] = df['heat_index_celsius'].apply(classify_comfort)
    
    print("  ✅ Comfort levels classified")
    return df

def add_weather_severity(df):
    """Classify overall weather severity."""
    print("⚠️  Calculating weather severity...")
    
    def classify_severity(row):
        if row['precipitation_mm'] > 50:
            return "Extreme"
        elif row['precipitation_mm'] > 25:
            return "Severe"
        elif row['wind_speed_kmh'] > 60:
            return "Severe"
        elif row['visibility_km'] < 1:
            return "Severe"
        elif (row['precipitation_mm'] > 10 or 
              row['wind_speed_kmh'] > 40 or 
              row['visibility_km'] < 3):
            return "Moderate"
        elif row['uv_index'] > 10:
            return "Moderate"
        else:
            return "Normal"
    
    df['weather_severity'] = df.apply(classify_severity, axis=1)
    
    print("  ✅ Weather severity calculated")
    return df

def add_alert_level(df):
    """Determine if weather conditions warrant an alert."""
    print("🚨 Determining alert levels...")
    
    def classify_alert(row):
        if (row['heat_index_celsius'] > 54 or
            row['precipitation_mm'] > 50 or
            row['wind_speed_kmh'] > 80):
            return "CRITICAL"
        elif (row['heat_index_celsius'] > 41 or
              row['precipitation_mm'] > 25 or
              row['wind_speed_kmh'] > 60 or
              row['visibility_km'] < 2 or
              row['uv_index'] > 10):
            return "WARNING"
        elif (row['heat_index_celsius'] > 32 or
              row['precipitation_mm'] > 10 or
              row['wind_speed_kmh'] > 40 or
              row['uv_index'] > 8):
            return "WATCH"
        else:
            return "NORMAL"
    
    df['alert_level'] = df.apply(classify_alert, axis=1)
    
    print("  ✅ Alert levels assigned")
    return df

def add_time_features(df):
    """Extract time-based features from timestamp."""
    print("⏰ Adding time-based features...")
    
    df['timestamp_parsed'] = pd.to_datetime(df['timestamp'])
    df['hour_of_day'] = df['timestamp_parsed'].dt.hour
    df['day_of_week'] = df['timestamp_parsed'].dt.dayofweek + 1  # Monday=1
    df['is_weekend'] = df['day_of_week'].isin([6, 7])
    
    # Time of day
    def classify_time_of_day(hour):
        if 6 <= hour <= 11:
            return "Morning"
        elif 12 <= hour <= 17:
            return "Afternoon"
        elif 18 <= hour <= 21:
            return "Evening"
        else:
            return "Night"
    
    df['time_of_day'] = df['hour_of_day'].apply(classify_time_of_day)
    
    # Season
    month = df['timestamp_parsed'].dt.month
    df['season'] = month.apply(lambda m: 
        "Winter" if m in [12, 1, 2] else
        "Spring" if m in [3, 4, 5] else
        "Summer" if m in [6, 7, 8] else
        "Autumn"
    )
    
    print("  ✅ Time features added")
    return df

def add_quality_metrics(df):
    """Add data quality indicators."""
    print("✓ Adding data quality metrics...")
    
    df['is_valid_temperature'] = df['temperature_celsius'].between(-50, 60)
    df['is_valid_humidity'] = df['humidity_percent'].between(0, 100)
    df['is_valid_pressure'] = df['pressure_hpa'].between(950, 1050)
    
    df['data_quality_score'] = (
        df['is_valid_temperature'].astype(int) +
        df['is_valid_humidity'].astype(int) +
        df['is_valid_pressure'].astype(int)
    ) / 3 * 100
    
    print("  ✅ Quality metrics added")
    return df

# ============================================
# MAIN PROCESSING FUNCTION
# ============================================
def process_weather_data(df):
    """Apply all transformations."""
    print("\n" + "="*60)
    print("🔧 APPLYING TRANSFORMATIONS")
    print("="*60)
    
    df = add_temperature_conversions(df)
    df = add_comfort_classification(df)
    df = add_weather_severity(df)
    df = add_alert_level(df)
    df = add_time_features(df)
    df = add_quality_metrics(df)
    
    print("="*60)
    print("✅ ALL TRANSFORMATIONS COMPLETE")
    print("="*60)
    
    return df

# ============================================
# LOAD DATA FROM LOCAL FILES
# ============================================
def load_local_data():
    """Load weather data from local_data/raw/ folder."""
    print("\n📂 Loading data from local_data/raw/...")
    
    # Find all JSON files
    json_files = glob.glob('local_data/raw/**/*.json', recursive=True)
    
    if not json_files:
        print("❌ No data files found!")
        print("   Run: python scripts/test_local_pipeline.py first")
        return None
    
    print(f"✅ Found {len(json_files)} data files")
    
    # Load all files into a list
    all_readings = []
    
    for file in json_files[:10]:  # Process first 10 files for testing
        with open(file, 'r') as f:
            data = json.load(f)
            if 'readings' in data:
                all_readings.extend(data['readings'])
    
    if not all_readings:
        print("❌ No readings found in files!")
        return None
    
    print(f"✅ Loaded {len(all_readings)} weather readings")
    
    # Convert to DataFrame
    df = pd.DataFrame(all_readings)
    return df

# ============================================
# DISPLAY RESULTS
# ============================================
def display_results(original_df, processed_df):
    """Display before/after comparison and statistics."""
    
    print("\n" + "="*60)
    print("📊 TRANSFORMATION RESULTS")
    print("="*60)
    
    # Show original columns
    print("\n📋 Original Columns:")
    print(f"   {', '.join(original_df.columns)}")
    
    # Show new columns added
    new_columns = set(processed_df.columns) - set(original_df.columns)
    print("\n✨ New Columns Added:")
    for col in sorted(new_columns):
        print(f"   - {col}")
    
    print("\n" + "="*60)
    print("🔍 SAMPLE TRANSFORMATIONS")
    print("="*60)
    
    # Show sample record
    sample = processed_df.iloc[0]
    
    print("\n🌡️  Temperature Conversions:")
    print(f"   Original: {sample['temperature_celsius']}°C")
    print(f"   Fahrenheit: {sample['temperature_fahrenheit']}°F")
    print(f"   Kelvin: {sample['temperature_kelvin']}K")
    
    print("\n😌 Comfort Analysis:")
    print(f"   Heat Index: {sample['heat_index_celsius']}°C")
    print(f"   Comfort Level: {sample['comfort_level']}")
    
    print("\n⚠️  Severity & Alerts:")
    print(f"   Weather Severity: {sample['weather_severity']}")
    print(f"   Alert Level: {sample['alert_level']}")
    
    print("\n⏰ Time Features:")
    print(f"   Hour: {sample['hour_of_day']}:00")
    print(f"   Time of Day: {sample['time_of_day']}")
    print(f"   Season: {sample['season']}")
    print(f"   Weekend: {sample['is_weekend']}")
    
    print("\n✓ Data Quality:")
    print(f"   Quality Score: {sample['data_quality_score']:.1f}%")
    
    # Statistics
    print("\n" + "="*60)
    print("📈 OVERALL STATISTICS")
    print("="*60)
    
    print(f"\n📊 Total Records: {len(processed_df)}")
    print(f"🌍 Cities: {', '.join(processed_df['city'].unique())}")
    
    print("\n🌡️  Temperature Range:")
    print(f"   Min: {processed_df['temperature_celsius'].min():.1f}°C")
    print(f"   Max: {processed_df['temperature_celsius'].max():.1f}°C")
    print(f"   Avg: {processed_df['temperature_celsius'].mean():.1f}°C")
    
    print("\n😌 Comfort Distribution:")
    comfort_counts = processed_df['comfort_level'].value_counts()
    for level, count in comfort_counts.items():
        pct = (count / len(processed_df)) * 100
        print(f"   {level}: {count} ({pct:.1f}%)")
    
    print("\n⚠️  Weather Severity:")
    severity_counts = processed_df['weather_severity'].value_counts()
    for severity, count in severity_counts.items():
        pct = (count / len(processed_df)) * 100
        print(f"   {severity}: {count} ({pct:.1f}%)")
    
    print("\n🚨 Alert Levels:")
    alert_counts = processed_df['alert_level'].value_counts()
    for alert, count in alert_counts.items():
        pct = (count / len(processed_df)) * 100
        print(f"   {alert}: {count} ({pct:.1f}%)")
    
    # Show alerts if any
    alerts = processed_df[processed_df['alert_level'].isin(['WARNING', 'CRITICAL'])]
    if len(alerts) > 0:
        print(f"\n🚨 {len(alerts)} ACTIVE ALERTS:")
        for idx, row in alerts.iterrows():
            print(f"   - {row['city']}: {row['alert_level']} ({row['weather_condition']})")
    else:
        print("\n✅ No active weather alerts")

# ============================================
# SAVE PROCESSED DATA
# ============================================
def save_processed_data(processed_df):
    """Save processed data to local folder."""
    
    output_dir = "local_data/processed"
    os.makedirs(output_dir, exist_ok=True)
    
    # Save as CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f"{output_dir}/processed_weather_{timestamp}.csv"
    processed_df.to_csv(csv_file, index=False)
    print(f"\n💾 Saved processed data to: {csv_file}")
    
    # Save alerts separately
    alerts = processed_df[processed_df['alert_level'].isin(['WARNING', 'CRITICAL'])]
    if len(alerts) > 0:
        alerts_file = f"{output_dir}/weather_alerts_{timestamp}.csv"
        alerts.to_csv(alerts_file, index=False)
        print(f"🚨 Saved {len(alerts)} alerts to: {alerts_file}")
    
    return csv_file

# ============================================
# COMPARE WITH GLUE SCRIPT
# ============================================
def compare_with_glue_script():
    """Show how this relates to the Glue script."""
    
    print("\n" + "="*60)
    print("🔄 PANDAS vs PYSPARK COMPARISON")
    print("="*60)
    
    comparison = """
    This script tests the SAME transformation logic as glue_weather_etl.py
    
    | Operation          | Pandas (Local)      | PySpark (Glue)        |
    |--------------------|---------------------|----------------------|
    | Load data          | pd.read_json()      | spark.readStream     |
    | Filter             | df[condition]       | df.filter()          |
    | Add column         | df['new'] = ...     | df.withColumn()      |
    | Apply function     | df.apply()          | udf() or when()      |
    | Save               | df.to_csv()         | df.writeStream       |
    
    Key Differences:
    
    1. **Scale**
       - Pandas: Single machine (your laptop)
       - PySpark: Distributed across multiple machines
    
    2. **Data Size**
       - Pandas: Up to a few GB
       - PySpark: Terabytes or more
    
    3. **Execution**
       - Pandas: Eager (runs immediately)
       - PySpark: Lazy (runs when needed)
    
    4. **Streaming**
       - Pandas: Batch only
       - PySpark: Real-time streaming support
    
    ✅ Same business logic = Same transformations!
    
    When you deploy to AWS Glue, you'll use the PySpark version.
    """
    
    print(comparison)

# ============================================
# MAIN EXECUTION
# ============================================
def main():
    """Main execution flow."""
    
    print("="*60)
    print("🧪 TESTING WEATHER TRANSFORMATIONS LOCALLY")
    print("="*60)
    print("\n💡 This tests the same logic as glue_weather_etl.py")
    print("   but uses Pandas instead of PySpark (runs on your laptop!)")
    print("\n⚠️  Make sure you've generated data first:")
    print("   python scripts/test_local_pipeline.py")
    print("="*60)
    
    # Load data
    df = load_local_data()
    
    if df is None:
        print("\n❌ Could not load data. Exiting.")
        return
    
    # Keep copy of original for comparison
    original_df = df.copy()
    
    # Apply transformations
    processed_df = process_weather_data(df)
    
    # Display results
    display_results(original_df, processed_df)
    
    # Save processed data
    output_file = save_processed_data(processed_df)
    
    # Show comparison with Glue
    compare_with_glue_script()
    
    print("\n" + "="*60)
    print("✅ TRANSFORMATION TEST COMPLETE")
    print("="*60)
    print(f"\n📂 Processed data saved to: {output_file}")
    print("\n🎯 Next Steps:")
    print("   1. Review the processed data file")
    print("   2. Check the transformations match your requirements")
    print("   3. When you get AWS, deploy glue_weather_etl.py")
    print("="*60)

# ============================================
# ENTRY POINT
# ============================================
if __name__ == "__main__":
    main()