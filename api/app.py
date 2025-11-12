"""
Real-Time Weather Data API
Generates simulated weather data from multiple cities
"""

from flask import Flask, jsonify
from datetime import datetime
import random
import math

app = Flask(__name__)

# Configuration: Cities to generate weather for
CITIES = [
    {"id": "STATION_001", "name": "Mumbai", "country": "India", "lat": 19.0760, "lon": 72.8777},
    {"id": "STATION_002", "name": "Delhi", "country": "India", "lat": 28.7041, "lon": 77.1025},
    {"id": "STATION_003", "name": "Bangalore", "country": "India", "lat": 12.9716, "lon": 77.5946},
    {"id": "STATION_004", "name": "Chennai", "country": "India", "lat": 13.0827, "lon": 80.2707},
    {"id": "STATION_005", "name": "Kolkata", "country": "India", "lat": 22.5726, "lon": 88.3639},
]

WEATHER_CONDITIONS = [
    "Clear Sky", "Partly Cloudy", "Cloudy", "Overcast",
    "Light Rain", "Moderate Rain", "Heavy Rain", "Thunderstorm",
    "Fog", "Mist", "Haze"
]

def calculate_heat_index(temp_c, humidity):
    """
    Calculate heat index (feels like temperature)
    Simplified formula for demonstration
    """
    temp_f = (temp_c * 9/5) + 32
    
    if temp_f < 80:
        return temp_c
    
    # Simplified heat index formula
    hi = (0.5 * (temp_f + 61.0 + ((temp_f - 68.0) * 1.2) + (humidity * 0.094)))
    
    if hi > 79:
        hi = (-42.379 + 2.04901523 * temp_f + 10.14333127 * humidity 
              - 0.22475541 * temp_f * humidity - 0.00683783 * temp_f * temp_f 
              - 0.05481717 * humidity * humidity + 0.00122874 * temp_f * temp_f * humidity 
              + 0.00085282 * temp_f * humidity * humidity 
              - 0.00000199 * temp_f * temp_f * humidity * humidity)
    
    # Convert back to Celsius
    hi_c = (hi - 32) * 5/9
    return round(hi_c, 1)

def generate_weather_reading(city):
    """
    Generate realistic weather data for a city
    """
    # Base temperature varies by time of day
    hour = datetime.now().hour
    
    # Simulate daily temperature cycle
    base_temp = 25 + 10 * math.sin((hour - 6) * math.pi / 12)
    
    # Add some randomness and city-specific variation
    temp_variation = random.uniform(-3, 3)
    temperature = round(base_temp + temp_variation, 1)
    
    # Humidity tends to be higher at night
    base_humidity = 65 + 15 * math.sin((hour + 6) * math.pi / 12)
    humidity = round(base_humidity + random.uniform(-10, 10), 1)
    humidity = max(20, min(100, humidity))  # Keep between 20-100%
    
    # Wind speed
    wind_speed = round(random.uniform(5, 25), 1)
    wind_directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    wind_direction = random.choice(wind_directions)
    
    # Atmospheric pressure
    pressure = round(random.uniform(1005, 1025), 1)
    
    # Precipitation (70% chance of no rain)
    precipitation = 0.0 if random.random() < 0.7 else round(random.uniform(0.1, 15), 1)
    
    # Weather condition based on precipitation
    if precipitation > 10:
        condition = "Heavy Rain"
    elif precipitation > 5:
        condition = "Moderate Rain"
    elif precipitation > 0:
        condition = "Light Rain"
    else:
        condition = random.choice(["Clear Sky", "Partly Cloudy", "Cloudy", "Haze"])
    
    # Visibility
    if precipitation > 5:
        visibility = round(random.uniform(2, 5), 1)
    elif condition == "Fog":
        visibility = round(random.uniform(0.5, 2), 1)
    else:
        visibility = round(random.uniform(8, 15), 1)
    
    # UV Index (0-11+ scale, higher during midday)
    if 10 <= hour <= 16:
        uv_index = random.randint(6, 11)
    elif 8 <= hour <= 18:
        uv_index = random.randint(3, 7)
    else:
        uv_index = random.randint(0, 2)
    
    # Calculate heat index
    heat_index = calculate_heat_index(temperature, humidity)
    
    return {
        "station_id": city["id"],
        "city": city["name"],
        "country": city["country"],
        "latitude": city["lat"],
        "longitude": city["lon"],
        "timestamp": datetime.now().isoformat(),
        "temperature_celsius": temperature,
        "humidity_percent": humidity,
        "pressure_hpa": pressure,
        "wind_speed_kmh": wind_speed,
        "wind_direction": wind_direction,
        "precipitation_mm": precipitation,
        "weather_condition": condition,
        "visibility_km": visibility,
        "uv_index": uv_index,
        "heat_index_celsius": heat_index
    }

@app.route('/api/weather', methods=['GET'])
def get_weather():
    """
    Main endpoint: Returns current weather for all cities
    """
    readings = []
    
    for city in CITIES:
        reading = generate_weather_reading(city)
        readings.append(reading)
    
    return jsonify({"readings": readings})

@app.route('/api/weather/<station_id>', methods=['GET'])
def get_weather_by_station(station_id):
    """
    Get weather for a specific station
    """
    city = next((c for c in CITIES if c["id"] == station_id), None)
    
    if not city:
        return jsonify({"error": "Station not found"}), 404
    
    reading = generate_weather_reading(city)
    return jsonify(reading)

@app.route('/health', methods=['GET'])
def health_check():
    """
    Health check endpoint
    """
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_stations": len(CITIES)
    })

if __name__ == '__main__':
    print("  Weather Data API Starting...")
    print(f" Monitoring {len(CITIES)} weather stations")
    print(" Access API at: http://localhost:5000/api/weather")
    print(" Health check at: http://localhost:5000/health")
    
    app.run(host='0.0.0.0', port=5000, debug=True)