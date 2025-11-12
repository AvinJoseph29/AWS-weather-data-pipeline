-- ============================================
-- Weather Data Pipeline - PostgreSQL Schema
-- ============================================

-- Drop tables if they exist (for clean setup)
DROP TABLE IF EXISTS daily_weather_summary CASCADE;
DROP TABLE IF EXISTS weather_readings CASCADE;

-- ============================================
-- Table 1: weather_readings
-- Stores all weather measurements
-- ============================================
CREATE TABLE weather_readings (
    -- Primary Key
    id SERIAL PRIMARY KEY,
    
    -- Station Information
    station_id VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    latitude DECIMAL(10, 6),
    longitude DECIMAL(10, 6),
    
    -- Timestamp
    reading_timestamp TIMESTAMP NOT NULL,
    
    -- Temperature Measurements (3 units)
    temperature_celsius DECIMAL(5, 2),
    temperature_fahrenheit DECIMAL(5, 2),
    temperature_kelvin DECIMAL(6, 2),
    heat_index_celsius DECIMAL(5, 2),
    
    -- Atmospheric Conditions
    humidity_percent DECIMAL(5, 2),
    pressure_hpa DECIMAL(6, 2),
    
    -- Wind Conditions
    wind_speed_kmh DECIMAL(5, 2),
    wind_direction VARCHAR(10),
    
    -- Precipitation & Visibility
    precipitation_mm DECIMAL(6, 2),
    visibility_km DECIMAL(5, 2),
    
    -- UV & Weather Condition
    uv_index INTEGER,
    weather_condition VARCHAR(100),
    
    -- YOUR UNIQUE TRANSFORMATIONS
    comfort_level VARCHAR(50),
    weather_severity VARCHAR(50),
    alert_level VARCHAR(50),
    
    -- Time Features
    hour_of_day INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN,
    time_of_day VARCHAR(20),
    season VARCHAR(20),
    
    -- Data Quality
    data_quality_score DECIMAL(5, 2),
    
    -- Metadata
    reading_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT unique_reading UNIQUE (station_id, reading_timestamp),
    CONSTRAINT valid_temperature CHECK (temperature_celsius BETWEEN -50 AND 60),
    CONSTRAINT valid_humidity CHECK (humidity_percent BETWEEN 0 AND 100),
    CONSTRAINT valid_uv CHECK (uv_index BETWEEN 0 AND 15)
);

-- Indexes for better query performance
CREATE INDEX idx_station_id ON weather_readings(station_id);
CREATE INDEX idx_reading_date ON weather_readings(reading_date);
CREATE INDEX idx_reading_timestamp ON weather_readings(reading_timestamp);
CREATE INDEX idx_alert_level ON weather_readings(alert_level);
CREATE INDEX idx_city ON weather_readings(city);

-- Add comment to table
COMMENT ON TABLE weather_readings IS 'Stores all weather measurements with transformations';

-- ============================================
-- Table 2: daily_weather_summary
-- Aggregated daily statistics per city
-- ============================================
CREATE TABLE daily_weather_summary (
    -- Primary Key
    id SERIAL PRIMARY KEY,
    
    -- Location & Date
    city VARCHAR(100) NOT NULL,
    summary_date DATE NOT NULL,
    
    -- Temperature Statistics
    avg_temperature_celsius DECIMAL(5, 2),
    min_temperature_celsius DECIMAL(5, 2),
    max_temperature_celsius DECIMAL(5, 2),
    avg_heat_index_celsius DECIMAL(5, 2),
    
    -- Other Averages
    avg_humidity_percent DECIMAL(5, 2),
    avg_pressure_hpa DECIMAL(6, 2),
    avg_wind_speed_kmh DECIMAL(5, 2),
    
    -- Precipitation
    total_precipitation_mm DECIMAL(6, 2),
    max_precipitation_mm DECIMAL(6, 2),
    
    -- Weather Conditions
    dominant_weather_condition VARCHAR(100),
    dominant_comfort_level VARCHAR(50),
    
    -- Alert Statistics
    total_readings INTEGER,
    normal_count INTEGER,
    watch_count INTEGER,
    warning_count INTEGER,
    critical_count INTEGER,
    alert_percentage DECIMAL(5, 2),
    
    -- Severity Statistics
    normal_severity_count INTEGER,
    moderate_severity_count INTEGER,
    severe_severity_count INTEGER,
    extreme_severity_count INTEGER,
    
    -- Data Quality
    avg_quality_score DECIMAL(5, 2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT unique_daily_summary UNIQUE (city, summary_date)
);

-- Indexes
CREATE INDEX idx_summary_city ON daily_weather_summary(city);
CREATE INDEX idx_summary_date ON daily_weather_summary(summary_date);

-- Add comment
COMMENT ON TABLE daily_weather_summary IS 'Daily aggregated weather statistics per city';

-- ============================================
-- Function: Update daily summary timestamp
-- ============================================
CREATE OR REPLACE FUNCTlION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger to auto-update updated_at
CREATE TRIGGER update_daily_summary_updated_at
    BEFORE UPDATE ON daily_weather_summary
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- View: Recent Weather Alerts
-- Quick view of recent critical weather
-- ============================================
CREATE OR REPLACE VIEW recent_weather_alerts AS
SELECT 
    city,
    reading_timestamp,
    temperature_celsius,
    heat_index_celsius,
    precipitation_mm,
    wind_speed_kmh,
    weather_condition,
    alert_level,
    comfort_level,
    weather_severity
FROM weather_readings
WHERE 
    alert_level IN ('WARNING', 'CRITICAL')
    AND reading_timestamp >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY 
    reading_timestamp DESC;

COMMENT ON VIEW recent_weather_alerts IS 'Recent weather warnings and critical alerts';

-- ============================================
-- View: Current Weather Status
-- Latest reading per city
-- ============================================
CREATE OR REPLACE VIEW current_weather_status AS
WITH latest_readings AS (
    SELECT 
        city,
        MAX(reading_timestamp) as latest_time
    FROM weather_readings
    GROUP BY city
)
SELECT 
    w.city,
    w.reading_timestamp,
    w.temperature_celsius,
    w.temperature_fahrenheit,
    w.heat_index_celsius,
    w.humidity_percent,
    w.wind_speed_kmh,
    w.weather_condition,
    w.comfort_level,
    w.alert_level,
    w.weather_severity
FROM weather_readings w
INNER JOIN latest_readings lr 
    ON w.city = lr.city 
    AND w.reading_timestamp = lr.latest_time
ORDER BY w.city;

COMMENT ON VIEW current_weather_status IS 'Latest weather reading per city';

-- ============================================
-- Sample Queries for Testing
-- ============================================

-- Query 1: Check if tables exist
-- SELECT table_name FROM information_schema.tables 
-- WHERE table_schema = 'public';

-- Query 2: Count readings per city
-- SELECT city, COUNT(*) as reading_count 
-- FROM weather_readings 
-- GROUP BY city;

-- Query 3: Average temperature by city
-- SELECT 
--     city,
--     ROUND(AVG(temperature_celsius), 1) as avg_temp,
--     ROUND(AVG(heat_index_celsius), 1) as avg_heat_index
-- FROM weather_readings
-- GROUP BY city
-- ORDER BY avg_temp DESC;

-- Query 4: Alert distribution
-- SELECT 
--     alert_level,
--     COUNT(*) as count,
--     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
-- FROM weather_readings
-- GROUP BY alert_level
-- ORDER BY count DESC;

-- Query 5: Daily summary for last 7 days
-- SELECT 
--     summary_date,
--     city,
--     avg_temperature_celsius,
--     total_precipitation_mm,
--     alert_percentage
-- FROM daily_weather_summary
-- WHERE summary_date >= CURRENT_DATE - INTERVAL '7 days'
-- ORDER BY summary_date DESC, city;

-- ============================================
-- Grant Permissions (adjust username as needed)
-- ============================================
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_username;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO your_username;

-- ============================================
-- Success Message
-- ============================================
SELECT 
    'Database schema created successfully!' as message,
    COUNT(*) as table_count
FROM information_schema.tables 
WHERE table_schema = 'public' 
    AND table_type = 'BASE TABLE';