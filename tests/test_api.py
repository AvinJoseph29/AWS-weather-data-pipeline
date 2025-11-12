"""
Unit Tests for Weather API
Tests the Weather Data Generator API
"""

import pytest
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from api.app import app, generate_weather_reading, calculate_heat_index, CITIES


# ============================================
# Test Fixtures
# ============================================

@pytest.fixture
def client():
    """Flask test client"""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def sample_city():
    """Sample city data for testing"""
    return CITIES[0]


# ============================================
# API Endpoint Tests
# ============================================

def test_health_endpoint(client):
    """Test /health endpoint"""
    response = client.get('/health')
    assert response.status_code == 200
    
    data = response.get_json()
    assert 'status' in data
    assert data['status'] == 'healthy'
    assert 'active_stations' in data
    assert data['active_stations'] == 5


def test_get_all_weather(client):
    """Test /api/weather endpoint returns all cities"""
    response = client.get('/api/weather')
    assert response.status_code == 200
    
    data = response.get_json()
    assert 'readings' in data
    assert isinstance(data['readings'], list)
    assert len(data['readings']) == 5  # 5 cities


def test_get_weather_by_station(client):
    """Test /api/weather/<station_id> endpoint"""
    response = client.get('/api/weather/STATION_001')
    assert response.status_code == 200
    
    data = response.get_json()
    assert data['station_id'] == 'STATION_001'
    assert data['city'] == 'Mumbai'
    assert 'temperature_celsius' in data


def test_invalid_station_id(client):
    """Test invalid station ID returns 404"""
    response = client.get('/api/weather/INVALID_ID')
    assert response.status_code == 404
    
    data = response.get_json()
    assert 'error' in data


# ============================================
# Heat Index Calculation Tests
# ============================================

def test_heat_index_low_temperature():
    """Test heat index calculation for low temperature"""
    # Below 27°C should return original temperature
    heat_index = calculate_heat_index(25, 70)
    assert heat_index == 25


def test_heat_index_high_temperature():
    """Test heat index calculation for high temperature"""
    # High temp + high humidity should increase heat index
    heat_index = calculate_heat_index(35, 80)
    assert heat_index > 35
    assert heat_index < 50  # Sanity check


def test_heat_index_extreme():
    """Test heat index with extreme values"""
    heat_index = calculate_heat_index(40, 90)
    assert heat_index > 40
    assert isinstance(heat_index, float)


# ============================================
# Weather Reading Generation Tests
# ============================================

def test_generate_weather_reading_structure(sample_city):
    """Test generated weather reading has correct structure"""
    reading = generate_weather_reading(sample_city)
    
    # Check required fields exist
    required_fields = [
        'station_id', 'city', 'country', 'latitude', 'longitude',
        'timestamp', 'temperature_celsius', 'humidity_percent',
        'pressure_hpa', 'wind_speed_kmh', 'wind_direction',
        'precipitation_mm', 'weather_condition', 'visibility_km',
        'uv_index', 'heat_index_celsius'
    ]
    
    for field in required_fields:
        assert field in reading, f"Missing field: {field}"


def test_generate_weather_reading_data_types(sample_city):
    """Test generated data has correct types"""
    reading = generate_weather_reading(sample_city)
    
    assert isinstance(reading['station_id'], str)
    assert isinstance(reading['city'], str)
    assert isinstance(reading['temperature_celsius'], (int, float))
    assert isinstance(reading['humidity_percent'], (int, float))
    assert isinstance(reading['uv_index'], int)


def test_generate_weather_reading_ranges(sample_city):
    """Test generated values are within realistic ranges"""
    reading = generate_weather_reading(sample_city)
    
    # Temperature: reasonable range for Earth
    assert -50 <= reading['temperature_celsius'] <= 60
    
    # Humidity: 0-100%
    assert 0 <= reading['humidity_percent'] <= 100
    
    # Pressure: realistic atmospheric pressure
    assert 900 <= reading['pressure_hpa'] <= 1100
    
    # UV Index: 0-15
    assert 0 <= reading['uv_index'] <= 15
    
    # Wind speed: non-negative
    assert reading['wind_speed_kmh'] >= 0
    
    # Precipitation: non-negative
    assert reading['precipitation_mm'] >= 0
    
    # Visibility: positive
    assert reading['visibility_km'] > 0


def test_generate_weather_reading_consistency(sample_city):
    """Test multiple readings from same city have consistent structure"""
    reading1 = generate_weather_reading(sample_city)
    reading2 = generate_weather_reading(sample_city)
    
    # Should have same fields
    assert reading1.keys() == reading2.keys()
    
    # City data should be same
    assert reading1['city'] == reading2['city']
    assert reading1['station_id'] == reading2['station_id']


# ============================================
# Configuration Tests
# ============================================

def test_cities_configuration():
    """Test CITIES configuration is valid"""
    assert len(CITIES) == 5
    
    for city in CITIES:
        assert 'id' in city
        assert 'name' in city
        assert 'country' in city
        assert 'lat' in city
        assert 'lon' in city
        
        # Check data types
        assert isinstance(city['id'], str)
        assert isinstance(city['name'], str)
        assert isinstance(city['lat'], (int, float))
        assert isinstance(city['lon'], (int, float))


def test_station_ids_unique():
    """Test all station IDs are unique"""
    station_ids = [city['id'] for city in CITIES]
    assert len(station_ids) == len(set(station_ids))


# ============================================
# API Response Format Tests
# ============================================

def test_weather_response_json_format(client):
    """Test API returns valid JSON"""
    response = client.get('/api/weather')
    assert response.content_type == 'application/json'


def test_weather_timestamp_format(client):
    """Test timestamp is in ISO format"""
    response = client.get('/api/weather')
    data = response.get_json()
    
    reading = data['readings'][0]
    timestamp = reading['timestamp']
    
    # Should be ISO format string
    assert isinstance(timestamp, str)
    assert 'T' in timestamp  # ISO format has 'T' separator


def test_weather_response_not_empty(client):
    """Test API doesn't return empty data"""
    response = client.get('/api/weather')
    data = response.get_json()
    
    assert len(data['readings']) > 0
    
    for reading in data['readings']:
        assert reading['temperature_celsius'] is not None
        assert reading['humidity_percent'] is not None


# ============================================
# Error Handling Tests
# ============================================

def test_api_handles_invalid_route(client):
    """Test API handles invalid routes gracefully"""
    response = client.get('/api/invalid_route')
    assert response.status_code == 404


def test_api_only_accepts_get(client):
    """Test API only accepts GET requests"""
    response = client.post('/api/weather')
    assert response.status_code == 405  # Method Not Allowed


# ============================================
# Integration Tests
# ============================================

def test_full_weather_data_flow(client):
    """Test complete flow from API call to data validation"""
    # Get weather data
    response = client.get('/api/weather')
    assert response.status_code == 200
    
    data = response.get_json()
    readings = data['readings']
    
    # Validate all readings
    for reading in readings:
        # Should have valid temperature
        assert -50 <= reading['temperature_celsius'] <= 60
        
        # Heat index should be calculated
        assert 'heat_index_celsius' in reading
        
        # Should have city information
        assert len(reading['city']) > 0
        assert reading['city'] in ['Mumbai', 'Delhi', 'Bangalore', 'Chennai', 'Kolkata']


def test_api_performance(client):
    """Test API responds quickly"""
    import time
    
    start = time.time()
    response = client.get('/api/weather')
    duration = time.time() - start
    
    assert response.status_code == 200
    assert duration < 1.0  # Should respond in under 1 second


# ============================================
# Parametrized Tests
# ============================================

@pytest.mark.parametrize("station_id,city_name", [
    ("STATION_001", "Mumbai"),
    ("STATION_002", "Delhi"),
    ("STATION_003", "Bangalore"),
    ("STATION_004", "Chennai"),
    ("STATION_005", "Kolkata"),
])
def test_all_stations_accessible(client, station_id, city_name):
    """Test all stations are accessible by ID"""
    response = client.get(f'/api/weather/{station_id}')
    assert response.status_code == 200
    
    data = response.get_json()
    assert data['station_id'] == station_id
    assert data['city'] == city_name


@pytest.mark.parametrize("temp,humidity,expected_higher", [
    (30, 50, False),  # Comfortable
    (35, 80, True),   # High heat index
    (40, 70, True),   # Very high heat index
])
def test_heat_index_variations(temp, humidity, expected_higher):
    """Test heat index with different temperature/humidity combinations"""
    heat_index = calculate_heat_index(temp, humidity)
    
    if expected_higher:
        assert heat_index > temp
    
    assert isinstance(heat_index, (int, float))