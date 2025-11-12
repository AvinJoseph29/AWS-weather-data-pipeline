"""
Local Pipeline Test - No AWS Required!
Simulates the entire data flow on your local machine
"""

import json
import requests
import time
import os
from datetime import datetime
from pathlib import Path

# ============================================
# CONFIGURATION
# ============================================
API_URL = "http://localhost:5000/api/weather"
LOCAL_DATA_DIR = "local_data/raw"
POLL_INTERVAL_SECONDS = 2

# Statistics
stats = {
    "api_calls": 0,
    "records_saved": 0,
    "errors": 0
}

# ============================================
# FUNCTION: Fetch Weather Data
# ============================================
def fetch_weather_data():
    """
    Fetch weather data from local API.
    Same logic as send_to_kinesis.py but saves locally.
    """
    try:
        print(f"\n{'='*60}")
        print(f"🔄 Fetching weather data from API...")
        
        response = requests.get(API_URL, timeout=10)
        
        if response.status_code != 200:
            print(f"❌ API error: Status {response.status_code}")
            stats["errors"] += 1
            return None
        
        data = response.json()
        stats["api_calls"] += 1
        
        print(f"✅ Fetched {len(data.get('readings', []))} readings")
        return data
        
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to API - Is it running?")
        print("   Start it with: cd api && python app.py")
        stats["errors"] += 1
        return None
        
    except Exception as e:
        print(f"❌ Error: {e}")
        stats["errors"] += 1
        return None

# ============================================
# FUNCTION: Save to Local Storage
# ============================================
def save_to_local_storage(weather_data):
    """
    Save weather data to local folders.
    Mimics the S3 folder structure with time-based partitioning.
    """
    if not weather_data or "readings" not in weather_data:
        return
    
    # Create timestamp-based folder path
    now = datetime.now()
    folder_path = os.path.join(
        LOCAL_DATA_DIR,
        f"year={now.year}",
        f"month={now.month:02d}",
        f"day={now.day:02d}",
        f"hour={now.hour:02d}"
    )
    
    # Create folders if they don't exist
    Path(folder_path).mkdir(parents=True, exist_ok=True)
    
    # Create filename with timestamp
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    filename = f"batch_{timestamp}.json"
    filepath = os.path.join(folder_path, filename)
    
    # Save data
    try:
        with open(filepath, 'w') as f:
            json.dump(weather_data, f, indent=2)
        
        stats["records_saved"] += len(weather_data.get("readings", []))
        
        print(f"💾 Saved to: {filepath}")
        print(f"   📦 {len(weather_data['readings'])} records saved")
        
        # Show sample data
        for reading in weather_data['readings'][:2]:  # Show first 2
            print(f"   📍 {reading['city']}: "
                  f"{reading['temperature_celsius']}°C, "
                  f"{reading['humidity_percent']}% humidity")
        
        if len(weather_data['readings']) > 2:
            print(f"   ... and {len(weather_data['readings']) - 2} more cities")
        
    except Exception as e:
        print(f"❌ Error saving file: {e}")
        stats["errors"] += 1

# ============================================
# FUNCTION: Print Statistics
# ============================================
def print_statistics():
    """Print pipeline statistics."""
    print("\n" + "="*60)
    print("📊 PIPELINE STATISTICS")
    print("="*60)
    print(f"🔄 API Calls: {stats['api_calls']}")
    print(f"💾 Records Saved: {stats['records_saved']}")
    print(f"❌ Errors: {stats['errors']}")
    print("="*60)

# ============================================
# FUNCTION: Show Data Structure
# ============================================
def show_data_structure():
    """
    Show the folder structure created (like tree command).
    """
    print("\n" + "="*60)
    print("📁 LOCAL DATA STRUCTURE")
    print("="*60)
    
    if not os.path.exists(LOCAL_DATA_DIR):
        print("No data yet!")
        return
    
    for root, dirs, files in os.walk(LOCAL_DATA_DIR):
        level = root.replace(LOCAL_DATA_DIR, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        
        sub_indent = ' ' * 2 * (level + 1)
        for file in files[:3]:  # Show first 3 files per folder
            print(f"{sub_indent}{file}")
        if len(files) > 3:
            print(f"{sub_indent}... and {len(files) - 3} more files")

# ============================================
# MAIN LOOP
# ============================================
def main():
    """
    Main execution loop - simulates the entire pipeline locally.
    """
    print("="*60)
    print("🌦️  LOCAL WEATHER DATA PIPELINE TEST")
    print("="*60)
    print(f"📡 API: {API_URL}")
    print(f"💾 Data folder: {LOCAL_DATA_DIR}")
    print(f"⏱️  Poll interval: {POLL_INTERVAL_SECONDS} seconds")
    print("\n💡 This simulates: API → Kinesis → S3")
    print("   (but saves locally instead of AWS)")
    print("="*60)
    print("\n⚠️  Make sure your Weather API is running!")
    print("   Terminal 1: cd api && python app.py")
    print("   Terminal 2: python scripts/test_local_pipeline.py")
    print("\n🛑 Press Ctrl+C to stop")
    print("="*60)
    
    iteration = 0
    
    try:
        while True:
            iteration += 1
            
            print(f"\n{'🔄 ' + str(iteration) + ' ':=^60}")
            
            # Fetch data
            weather_data = fetch_weather_data()
            
            # Save data
            if weather_data:
                save_to_local_storage(weather_data)
            
            # Show stats every 10 iterations
            if iteration % 10 == 0:
                print_statistics()
                show_data_structure()
            
            # Wait
            print(f"⏸️  Waiting {POLL_INTERVAL_SECONDS} seconds...")
            time.sleep(POLL_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print("\n\n" + "="*60)
        print("🛑 PIPELINE STOPPED")
        print("="*60)
        print_statistics()
        show_data_structure()
        
        print("\n📂 Your data is in: ./" + LOCAL_DATA_DIR)
        print("\n✅ When you get AWS, use:")
        print("   - send_to_kinesis.py (producer)")
        print("   - kinesis_to_s3.py (consumer)")

# ============================================
# ENTRY POINT
# ============================================
if __name__ == "__main__":
    # Create data directory
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    
    # Run main loop
    main()