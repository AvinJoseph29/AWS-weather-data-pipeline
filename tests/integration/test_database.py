"""
Integration Tests for Database Operations
Tests PostgreSQL connectivity and data operations
"""

import pytest
import psycopg2
import os
from datetime import datetime


# ============================================
# Test Configuration
# ============================================

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'weather_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres123')
}


# ============================================
# Fixtures
# ============================================

@pytest.fixture(scope='module')
def db_connection():
    """Create database connection for tests"""
    conn = psycopg2.connect(**DB_CONFIG)
    yield conn
    conn.close()


@pytest.fixture
def db_cursor(db_connection):
    """Create database cursor"""
    cursor = db_connection.cursor()
    yield cursor
    db_connection.rollback()  # Rollback after each test
    cursor.close()


@pytest.fixture
def sample_weather_data():
    """Sample weather data for testing"""
    return {
        'station_id': 'TEST_001',
        'city': 'TestCity',
        'country': 'TestCountry',
        'latitude': 12.34,
        'longitude': 56.78,
        'reading_timestamp': datetime.now(),
        'reading_date': datetime.now().date(),
        'temperature_celsius': 25.5,
        'temperature_fahrenheit': 77.9,
        'temperature_kelvin': 298.7,
        'heat_index_celsius': 26.0,
        'humidity_percent': 65.0,
        'pressure_hpa': 1013.0,
        'wind_speed_kmh': 15.0,
        'wind_direction': 'NW',
        'precipitation_mm': 0.0,
        'visibility_km': 10.0,
        'uv_index': 5,
        'weather_condition': 'Clear',
        'comfort_level': 'Comfortable',
        'weather_severity': 'Normal',
        'alert_level': 'NORMAL',
        'hour_of_day': 14,
        'day_of_week': 3,
        'is_weekend': False,
        'time_of_day': 'Afternoon',
        'season': 'Summer',
        'data_quality_score': 100.0
    }


# ============================================
# Connection Tests
# ============================================

def test_database_connection(db_connection):
    """Test database connection is successful"""
    assert db_connection is not None
    assert db_connection.closed == 0


def test_database_version(db_cursor):
    """Test PostgreSQL version"""
    db_cursor.execute("SELECT version();")
    version = db_cursor.fetchone()[0]
    assert 'PostgreSQL' in version


# ============================================
# Schema Tests
# ============================================

def test_tables_exist(db_cursor):
    """Test required tables exist"""
    db_cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    
    tables = [row[0] for row in db_cursor.fetchall()]
    
    assert 'weather_readings' in tables
    assert 'daily_weather_summary' in tables


def test_weather_readings_columns(db_cursor):
    """Test weather_readings table has required columns"""
    db_cursor.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'weather_readings'
    """)
    
    columns = [row[0] for row in db_cursor.fetchall()]
    
    required_columns = [
        'id', 'station_id', 'city', 'reading_timestamp',
        'temperature_celsius', 'humidity_percent', 'alert_level'
    ]
    
    for col in required_columns:
        assert col in columns, f"Missing column: {col}"


def test_views_exist(db_cursor):
    """Test required views exist"""
    db_cursor.execute("""
        SELECT table_name 
        FROM information_schema.views 
        WHERE table_schema = 'public'
    """)
    
    views = [row[0] for row in db_cursor.fetchall()]
    
    assert 'recent_weather_alerts' in views
    assert 'current_weather_status' in views


# ============================================
# CRUD Operations Tests
# ============================================

def test_insert_weather_reading(db_cursor, db_connection, sample_weather_data):
    """Test inserting weather reading"""
    insert_query = """
        INSERT INTO weather_readings (
            station_id, city, country, latitude, longitude,
            reading_timestamp, reading_date,
            temperature_celsius, temperature_fahrenheit, temperature_kelvin,
            heat_index_celsius, humidity_percent, pressure_hpa,
            wind_speed_kmh, wind_direction,
            precipitation_mm, visibility_km,
            uv_index, weather_condition,
            comfort_level, weather_severity, alert_level,
            hour_of_day, day_of_week, is_weekend,
            time_of_day, season,
            data_quality_score
        ) VALUES (
            %(station_id)s, %(city)s, %(country)s, %(latitude)s, %(longitude)s,
            %(reading_timestamp)s, %(reading_date)s,
            %(temperature_celsius)s, %(temperature_fahrenheit)s, %(temperature_kelvin)s,
            %(heat_index_celsius)s, %(humidity_percent)s, %(pressure_hpa)s,
            %(wind_speed_kmh)s, %(wind_direction)s,
            %(precipitation_mm)s, %(visibility_km)s,
            %(uv_index)s, %(weather_condition)s,
            %(comfort_level)s, %(weather_severity)s, %(alert_level)s,
            %(hour_of_day)s, %(day_of_week)s, %(is_weekend)s,
            %(time_of_day)s, %(season)s,
            %(data_quality_score)s
        ) RETURNING id;
    """
    
    db_cursor.execute(insert_query, sample_weather_data)
    inserted_id = db_cursor.fetchone()[0]
    
    assert inserted_id is not None
    assert isinstance(inserted_id, int)


def test_query_weather_reading(db_cursor, db_connection, sample_weather_data):
    """Test querying inserted weather reading"""
    # Insert test data
    insert_query = """
        INSERT INTO weather_readings (
            station_id, city, reading_timestamp, reading_date,
            temperature_celsius, alert_level
        ) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
    """
    
    db_cursor.execute(insert_query, (
        sample_weather_data['station_id'],
        sample_weather_data['city'],
        sample_weather_data['reading_timestamp'],
        sample_weather_data['reading_date'],
        sample_weather_data['temperature_celsius'],
        sample_weather_data['alert_level']
    ))
    
    inserted_id = db_cursor.fetchone()[0]
    
    # Query the data
    db_cursor.execute(
        "SELECT station_id, city, temperature_celsius FROM weather_readings WHERE id = %s",
        (inserted_id,)
    )
    
    result = db_cursor.fetchone()
    assert result[0] == sample_weather_data['station_id']
    assert result[1] == sample_weather_data['city']


def test_update_weather_reading(db_cursor, db_connection, sample_weather_data):
    """Test updating weather reading"""
    # Insert
    insert_query = """
        INSERT INTO weather_readings (station_id, city, reading_timestamp, reading_date, temperature_celsius)
        VALUES (%s, %s, %s, %s, %s) RETURNING id;
    """
    
    db_cursor.execute(insert_query, (
        sample_weather_data['station_id'],
        sample_weather_data['city'],
        sample_weather_data['reading_timestamp'],
        sample_weather_data['reading_date'],
        20.0
    ))
    
    inserted_id = db_cursor.fetchone()[0]
    
    # Update
    db_cursor.execute(
        "UPDATE weather_readings SET temperature_celsius = %s WHERE id = %s",
        (25.0, inserted_id)
    )
    
    # Verify
    db_cursor.execute(
        "SELECT temperature_celsius FROM weather_readings WHERE id = %s",
        (inserted_id,)
    )
    
    result = db_cursor.fetchone()[0]
    assert result == 25.0


# ============================================
# Constraint Tests
# ============================================

def test_unique_constraint(db_cursor, db_connection, sample_weather_data):
    """Test unique constraint on station_id and reading_timestamp"""
    insert_query = """
        INSERT INTO weather_readings (station_id, city, reading_timestamp, reading_date, temperature_celsius)
        VALUES (%s, %s, %s, %s, %s);
    """
    
    # Insert first record
    db_cursor.execute(insert_query, (
        sample_weather_data['station_id'],
        sample_weather_data['city'],
        sample_weather_data['reading_timestamp'],
        sample_weather_data['reading_date'],
        25.0
    ))
    
    # Try to insert duplicate
    with pytest.raises(psycopg2.IntegrityError):
        db_cursor.execute(insert_query, (
            sample_weather_data['station_id'],
            sample_weather_data['city'],
            sample_weather_data['reading_timestamp'],
            sample_weather_data['reading_date'],
            26.0
        ))


def test_check_constraint_temperature(db_cursor, db_connection):
    """Test temperature check constraint"""
    insert_query = """
        INSERT INTO weather_readings (station_id, city, reading_timestamp, reading_date, temperature_celsius)
        VALUES (%s, %s, %s, %s, %s);
    """
    
    # Invalid temperature (< -50)
    with pytest.raises(psycopg2.IntegrityError):
        db_cursor.execute(insert_query, (
            'TEST_001', 'TestCity', datetime.now(), datetime.now().date(), -100.0
        ))


def test_check_constraint_humidity(db_cursor, db_connection):
    """Test humidity check constraint"""
    insert_query = """
        INSERT INTO weather_readings (station_id, city, reading_timestamp, reading_date, humidity_percent)
        VALUES (%s, %s, %s, %s, %s);
    """
    
    # Invalid humidity (> 100)
    with pytest.raises(psycopg2.IntegrityError):
        db_cursor.execute(insert_query, (
            'TEST_001', 'TestCity', datetime.now(), datetime.now().date(), 150.0
        ))


# ============================================
# Index Tests
# ============================================

def test_indexes_exist(db_cursor):
    """Test required indexes exist"""
    db_cursor.execute("""
        SELECT indexname 
        FROM pg_indexes 
        WHERE tablename = 'weather_readings'
    """)
    
    indexes = [row[0] for row in db_cursor.fetchall()]
    
    # Should have indexes on commonly queried columns
    index_columns = ['station_id', 'reading_date', 'alert_level']
    for col in index_columns:
        assert any(col in idx for idx in indexes), f"Missing index on {col}"


# ============================================
# Aggregation Tests
# ============================================

def test_daily_summary_aggregation(db_cursor, db_connection):
    """Test daily summary can be calculated"""
    # Insert test data
    test_date = datetime.now().date()
    
    for i in range(5):
        db_cursor.execute("""
            INSERT INTO weather_readings (
                station_id, city, reading_timestamp, reading_date,
                temperature_celsius, humidity_percent, alert_level
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            f'TEST_{i}', 'TestCity', 
            datetime.now(), test_date,
            20.0 + i, 60.0 + i, 'NORMAL'
        ))
    
    # Query aggregation
    db_cursor.execute("""
        SELECT 
            city,
            COUNT(*) as reading_count,
            AVG(temperature_celsius) as avg_temp,
            MAX(temperature_celsius) as max_temp,
            MIN(temperature_celsius) as min_temp
        FROM weather_readings
        WHERE city = 'TestCity'
        GROUP BY city
    """)
    
    result = db_cursor.fetchone()
    assert result[1] == 5  # 5 readings
    assert 20.0 <= result[2] <= 25.0  # avg temp
    assert result[3] == 24.0  # max temp
    assert result[4] == 20.0  # min temp


# ============================================
# View Tests
# ============================================

def test_recent_weather_alerts_view(db_cursor, db_connection):
    """Test recent_weather_alerts view works"""
    # Insert a warning
    db_cursor.execute("""
        INSERT INTO weather_readings (
            station_id, city, reading_timestamp, reading_date,
            temperature_celsius, alert_level
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        'TEST_ALERT', 'TestCity', 
        datetime.now(), datetime.now().date(),
        40.0, 'WARNING'
    ))
    
    # Query view
    db_cursor.execute("SELECT * FROM recent_weather_alerts WHERE city = 'TestCity'")
    results = db_cursor.fetchall()
    
    assert len(results) > 0


def test_current_weather_status_view(db_cursor, db_connection):
    """Test current_weather_status view works"""
    # Insert recent data
    db_cursor.execute("""
        INSERT INTO weather_readings (
            station_id, city, reading_timestamp, reading_date,
            temperature_celsius, weather_condition
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        'TEST_CURRENT', 'CurrentCity', 
        datetime.now(), datetime.now().date(),
        25.0, 'Sunny'
    ))
    
    db_connection.commit()
    
    # Query view
    db_cursor.execute("SELECT * FROM current_weather_status WHERE city = 'CurrentCity'")
    results = db_cursor.fetchall()
    
    assert len(results) > 0


# ============================================
# Performance Tests
# ============================================

def test_query_performance(db_cursor, db_connection):
    """Test query performance with multiple records"""
    import time
    
    # Insert 100 records
    for i in range(100):
        db_cursor.execute("""
            INSERT INTO weather_readings (
                station_id, city, reading_timestamp, reading_date, temperature_celsius
            ) VALUES (%s, %s, %s, %s, %s)
        """, (
            f'PERF_{i}', 'PerfCity', 
            datetime.now(), datetime.now().date(),
            25.0
        ))
    
    db_connection.commit()
    
    # Measure query time
    start = time.time()
    db_cursor.execute("SELECT COUNT(*) FROM weather_readings WHERE city = 'PerfCity'")
    duration = time.time() - start
    
    assert duration < 1.0  # Should complete in under 1 second


# ============================================
# Data Integrity Tests
# ============================================

def test_transaction_rollback(db_cursor, db_connection):
    """Test transaction rollback works"""
    # Insert data
    db_cursor.execute("""
        INSERT INTO weather_readings (station_id, city, reading_timestamp, reading_date, temperature_celsius)
        VALUES (%s, %s, %s, %s, %s)
    """, ('ROLLBACK_TEST', 'RollbackCity', datetime.now(), datetime.now().date(), 25.0))
    
    # Rollback
    db_connection.rollback()
    
    # Verify data not persisted
    db_cursor.execute("SELECT COUNT(*) FROM weather_readings WHERE station_id = 'ROLLBACK_TEST'")
    count = db_cursor.fetchone()[0]
    assert count == 0


def test_null_handling(db_cursor, db_connection):
    """Test handling of NULL values in non-required fields"""
    # Insert with some NULL fields
    db_cursor.execute("""
        INSERT INTO weather_readings (
            station_id, city, reading_timestamp, reading_date,
            temperature_celsius, humidity_percent, precipitation_mm
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        'NULL_TEST', 'NullCity', 
        datetime.now(), datetime.now().date(),
        25.0, None, None
    ))
    
    # Query and verify
    db_cursor.execute("""
        SELECT humidity_percent, precipitation_mm 
        FROM weather_readings 
        WHERE station_id = 'NULL_TEST'
    """)
    
    result = db_cursor.fetchone()
    assert result[0] is None
    assert result[1] is None


# ============================================
# Alert Level Tests
# ============================================

def test_alert_level_distribution(db_cursor, db_connection):
    """Test alert level query and distribution"""
    alert_levels = ['NORMAL', 'WATCH', 'WARNING', 'CRITICAL']
    
    # Insert various alert levels
    for i, level in enumerate(alert_levels):
        db_cursor.execute("""
            INSERT INTO weather_readings (
                station_id, city, reading_timestamp, reading_date,
                temperature_celsius, alert_level
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            f'ALERT_{i}', 'AlertCity', 
            datetime.now(), datetime.now().date(),
            25.0, level
        ))
    
    # Query distribution
    db_cursor.execute("""
        SELECT 
            alert_level,
            COUNT(*) as count
        FROM weather_readings
        WHERE city = 'AlertCity'
        GROUP BY alert_level
        ORDER BY count DESC
    """)
    
    results = db_cursor.fetchall()
    assert len(results) == 4


# ============================================
# Cleanup Tests
# ============================================

def test_cascade_delete(db_cursor, db_connection):
    """Test cascade delete if configured"""
    # This test depends on your foreign key configuration
    # Placeholder for cascade delete testing
    pass


# ============================================
# Concurrent Access Tests
# ============================================

def test_concurrent_inserts(db_connection):
    """Test concurrent inserts don't cause deadlocks"""
    import threading
    
    def insert_record(thread_id):
        cursor = db_connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO weather_readings (
                    station_id, city, reading_timestamp, reading_date, temperature_celsius
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                f'CONCURRENT_{thread_id}', 'ConcurrentCity',
                datetime.now(), datetime.now().date(), 25.0
            ))
            db_connection.commit()
        except Exception as e:
            print(f"Thread {thread_id} error: {e}")
        finally:
            cursor.close()
    
    # Create multiple threads
    threads = []
    for i in range(5):
        thread = threading.Thread(target=insert_record, args=(i,))
        threads.append(thread)
        thread.start()
    
    # Wait for all threads
    for thread in threads:
        thread.join()
    
    # Verify all records inserted
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather_readings WHERE city = 'ConcurrentCity'")
    count = cursor.fetchone()[0]
    cursor.close()
    
    assert count >= 5  # At least 5 records inserted