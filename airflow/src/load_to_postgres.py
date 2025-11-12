"""
Load Weather Data to PostgreSQL
Reads processed weather data from S3 and loads into PostgreSQL
"""

import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
import boto3
import logging
import io
from datetime import datetime, timedelta
import sys

# ============================================
# LOGGING SETUP
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION
# ============================================
# S3 Configuration
ALERTS_BUCKET = "weather-alerts-bucket84h674"
AWS_REGION = "eu-north-1"

# PostgreSQL Configuration
DB_CONFIG = {
    'host': 'postgres',  # Docker service name
    'port': 5432,
    'database': 'weather_db',
    'user': 'postgres',
    'password': 'postgres123'
}

# Statistics
stats = {
    'files_processed': 0,
    'total_records': 0,
    'inserted_records': 0,
    'duplicate_records': 0,
    'failed_records': 0
}

# ============================================
# FUNCTION: Connect to S3
# ============================================
def create_s3_client():
    """
    Create and return S3 client.
    
    Returns:
        boto3.client: S3 client
    """
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        logger.info("✅ Connected to S3")
        return s3_client
    except Exception as e:
        logger.error(f"❌ Failed to connect to S3: {e}")
        sys.exit(1)

# ============================================
# FUNCTION: Connect to PostgreSQL
# ============================================
def create_postgres_connection():
    """
    Create and return PostgreSQL connection.
    
    Returns:
        tuple: (connection, cursor)
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        logger.info("✅ Connected to PostgreSQL")
        return conn, cursor
    except psycopg2.Error as e:
        logger.error(f"❌ Failed to connect to PostgreSQL: {e}")
        sys.exit(1)

# ============================================
# FUNCTION: Get Yesterday's Data from S3
# ============================================
def get_yesterdays_data(s3_client):
    """
    Download yesterday's weather alert data from S3.
    
    Args:
        s3_client: boto3 S3 client
        
    Returns:
        pd.DataFrame: Weather data
        
    Note:
        Reads Parquet files from S3 alerts bucket
        Filters data from yesterday only
    """
    logger.info("📥 Fetching yesterday's data from S3...")
    
    # Calculate yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    year = yesterday.year
    month = f"{yesterday.month:02d}"
    day = f"{yesterday.day:02d}"
    
    # Construct S3 prefix (folder path)
    prefix = f"year={year}/month={month}/day={day}/"
    
    logger.info(f"   Looking in: s3://{ALERTS_BUCKET}/{prefix}")
    
    try:
        # List all files in the folder
        response = s3_client.list_objects_v2(
            Bucket=ALERTS_BUCKET,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            logger.warning(f"⚠️  No data found for {year}-{month}-{day}")
            return pd.DataFrame()
        
        # Get all parquet files
        parquet_files = [
            obj['Key'] for obj in response['Contents']
            if obj['Key'].endswith('.parquet')
        ]
        
        if not parquet_files:
            logger.warning(f"⚠️  No parquet files found")
            return pd.DataFrame()
        
        logger.info(f"   Found {len(parquet_files)} files")
        stats['files_processed'] = len(parquet_files)
        
        # Read all files and combine
        dataframes = []
        
        for file_key in parquet_files:
            try:
                # Download file
                obj = s3_client.get_object(Bucket=ALERTS_BUCKET, Key=file_key)
                
                # Read parquet from bytes
                df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
                dataframes.append(df)
                
                logger.info(f"   ✅ Read {len(df)} records from {file_key}")
                
            except Exception as e:
                logger.error(f"   ❌ Failed to read {file_key}: {e}")
                continue
        
        if not dataframes:
            logger.warning("⚠️  No data could be read from files")
            return pd.DataFrame()
        
        # Combine all dataframes
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        stats['total_records'] = len(combined_df)
        logger.info(f"✅ Loaded {len(combined_df)} total records")
        
        return combined_df
        
    except Exception as e:
        logger.error(f"❌ Error fetching data from S3: {e}")
        return pd.DataFrame()

# ============================================
# FUNCTION: Get Local Data (for testing)
# ============================================
def get_local_processed_data():
    """
    Read processed data from local folder for testing.
    Use this when you don't have AWS yet!
    
    Returns:
        pd.DataFrame: Weather data
    """
    logger.info("📂 Loading processed data from local folder...")
    
    import glob
    
    # Find processed CSV files
    csv_files = glob.glob('local_data/processed/processed_weather_*.csv')
    
    if not csv_files:
        logger.error("❌ No processed data files found!")
        logger.error("   Run: python scripts/test_transformations.py first")
        return pd.DataFrame()
    
    # Read the most recent file
    latest_file = max(csv_files)
    logger.info(f"   Reading: {latest_file}")
    
    df = pd.read_csv(latest_file)
    
    # Filter only alerts (to simulate S3 alerts bucket)
    df = df[df['alert_level'].isin(['WARNING', 'CRITICAL'])]
    
    stats['total_records'] = len(df)
    logger.info(f"✅ Loaded {len(df)} alert records")
    
    return df

# ============================================
# FUNCTION: Clean and Prepare Data
# ============================================
def prepare_data_for_postgres(df):
    """
    Clean and prepare data for PostgreSQL insertion.
    
    Args:
        df (pd.DataFrame): Raw data
        
    Returns:
        pd.DataFrame: Cleaned data ready for insertion
    """
    logger.info("🧹 Cleaning and preparing data...")
    
    if df.empty:
        return df
    
    # Remove duplicates (based on station_id and timestamp)
    original_count = len(df)
    df = df.drop_duplicates(subset=['station_id', 'timestamp'])
    duplicates_removed = original_count - len(df)
    
    if duplicates_removed > 0:
        logger.info(f"   Removed {duplicates_removed} duplicate records")
        stats['duplicate_records'] = duplicates_removed
    
    # Convert timestamp to datetime
    df['reading_timestamp'] = pd.to_datetime(df['timestamp'])
    df['reading_date'] = df['reading_timestamp'].dt.date
    
    # Ensure numeric columns are properly typed
    numeric_columns = [
        'temperature_celsius', 'temperature_fahrenheit', 'temperature_kelvin',
        'heat_index_celsius', 'humidity_percent', 'pressure_hpa',
        'wind_speed_kmh', 'precipitation_mm', 'visibility_km',
        'data_quality_score'
    ]
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Ensure integer columns
    integer_columns = ['uv_index', 'hour_of_day', 'day_of_week']
    for col in integer_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Convert boolean
    if 'is_weekend' in df.columns:
        df['is_weekend'] = df['is_weekend'].astype(bool)
    
    # Remove any rows with null critical fields
    critical_fields = ['station_id', 'city', 'reading_timestamp']
    df = df.dropna(subset=critical_fields)
    
    logger.info(f"✅ Data prepared: {len(df)} records ready for insertion")
    
    return df

# ============================================
# FUNCTION: Insert Data into PostgreSQL
# ============================================
def insert_data_to_postgres(conn, cursor, df):
    """
    Insert weather data into PostgreSQL.
    
    Uses batch insertion for performance.
    Handles duplicates gracefully (ON CONFLICT DO NOTHING).
    
    Args:
        conn: PostgreSQL connection
        cursor: PostgreSQL cursor
        df (pd.DataFrame): Data to insert
    """
    if df.empty:
        logger.warning("⚠️  No data to insert")
        return
    
    logger.info(f"💾 Inserting {len(df)} records into PostgreSQL...")
    
    # SQL INSERT statement
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
            %s, %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s
        )
        ON CONFLICT (station_id, reading_timestamp) DO NOTHING;
    """
    
    # Prepare data tuples
    data_tuples = []
    
    for idx, row in df.iterrows():
        try:
            data_tuple = (
                row.get('station_id'),
                row.get('city'),
                row.get('country'),
                row.get('latitude'),
                row.get('longitude'),
                row.get('reading_timestamp'),
                row.get('reading_date'),
                row.get('temperature_celsius'),
                row.get('temperature_fahrenheit'),
                row.get('temperature_kelvin'),
                row.get('heat_index_celsius'),
                row.get('humidity_percent'),
                row.get('pressure_hpa'),
                row.get('wind_speed_kmh'),
                row.get('wind_direction'),
                row.get('precipitation_mm'),
                row.get('visibility_km'),
                row.get('uv_index'),
                row.get('weather_condition'),
                row.get('comfort_level'),
                row.get('weather_severity'),
                row.get('alert_level'),
                row.get('hour_of_day'),
                row.get('day_of_week'),
                row.get('is_weekend'),
                row.get('time_of_day'),
                row.get('season'),
                row.get('data_quality_score')
            )
            data_tuples.append(data_tuple)
        except Exception as e:
            logger.error(f"   ❌ Error preparing row {idx}: {e}")
            stats['failed_records'] += 1
            continue
    
    # Batch insert for performance
    try:
        execute_batch(cursor, insert_query, data_tuples, page_size=100)
        conn.commit()
        
        # Get count of actually inserted rows
        cursor.execute("SELECT COUNT(*) FROM weather_readings")
        total_count = cursor.fetchone()[0]
        
        stats['inserted_records'] = len(data_tuples)
        
        logger.info(f"✅ Successfully inserted {len(data_tuples)} records")
        logger.info(f"   Total records in database: {total_count}")
        
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"❌ Error inserting data: {e}")
        stats['failed_records'] += len(data_tuples)
        raise

# ============================================
# FUNCTION: Generate Daily Summary
# ============================================
def generate_daily_summary(conn, cursor):
    """
    Generate daily aggregated statistics per city.
    
    This creates summary records in daily_weather_summary table.
    """
    logger.info("📊 Generating daily summary statistics...")
    
    summary_query = """
        INSERT INTO daily_weather_summary (
            city, summary_date,
            avg_temperature_celsius, min_temperature_celsius, max_temperature_celsius,
            avg_heat_index_celsius, avg_humidity_percent, avg_pressure_hpa,
            avg_wind_speed_kmh, total_precipitation_mm, max_precipitation_mm,
            dominant_weather_condition, dominant_comfort_level,
            total_readings, normal_count, watch_count, warning_count, critical_count,
            alert_percentage,
            normal_severity_count, moderate_severity_count, 
            severe_severity_count, extreme_severity_count,
            avg_quality_score
        )
        SELECT 
            city,
            reading_date,
            ROUND(AVG(temperature_celsius), 2),
            ROUND(MIN(temperature_celsius), 2),
            ROUND(MAX(temperature_celsius), 2),
            ROUND(AVG(heat_index_celsius), 2),
            ROUND(AVG(humidity_percent), 2),
            ROUND(AVG(pressure_hpa), 2),
            ROUND(AVG(wind_speed_kmh), 2),
            ROUND(SUM(precipitation_mm), 2),
            ROUND(MAX(precipitation_mm), 2),
            MODE() WITHIN GROUP (ORDER BY weather_condition),
            MODE() WITHIN GROUP (ORDER BY comfort_level),
            COUNT(*),
            SUM(CASE WHEN alert_level = 'NORMAL' THEN 1 ELSE 0 END),
            SUM(CASE WHEN alert_level = 'WATCH' THEN 1 ELSE 0 END),
            SUM(CASE WHEN alert_level = 'WARNING' THEN 1 ELSE 0 END),
            SUM(CASE WHEN alert_level = 'CRITICAL' THEN 1 ELSE 0 END),
            ROUND(
                SUM(CASE WHEN alert_level IN ('WARNING', 'CRITICAL') THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                2
            ),
            SUM(CASE WHEN weather_severity = 'Normal' THEN 1 ELSE 0 END),
            SUM(CASE WHEN weather_severity = 'Moderate' THEN 1 ELSE 0 END),
            SUM(CASE WHEN weather_severity = 'Severe' THEN 1 ELSE 0 END),
            SUM(CASE WHEN weather_severity = 'Extreme' THEN 1 ELSE 0 END),
            ROUND(AVG(data_quality_score), 2)
        FROM weather_readings
        WHERE reading_date = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY city, reading_date
        ON CONFLICT (city, summary_date) 
        DO UPDATE SET
            avg_temperature_celsius = EXCLUDED.avg_temperature_celsius,
            min_temperature_celsius = EXCLUDED.min_temperature_celsius,
            max_temperature_celsius = EXCLUDED.max_temperature_celsius,
            updated_at = CURRENT_TIMESTAMP;
    """
    
    try:
        cursor.execute(summary_query)
        rows_affected = cursor.rowcount
        conn.commit()
        
        logger.info(f"✅ Generated summaries for {rows_affected} cities")
        
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"❌ Error generating summary: {e}")
        raise

# ============================================
# FUNCTION: Print Statistics
# ============================================
def print_statistics():
    """Print job execution statistics."""
    logger.info("=" * 60)
    logger.info("📊 JOB STATISTICS")
    logger.info("=" * 60)
    logger.info(f"📂 Files Processed: {stats['files_processed']}")
    logger.info(f"📥 Total Records: {stats['total_records']}")
    logger.info(f"💾 Inserted Records: {stats['inserted_records']}")
    logger.info(f"🔄 Duplicate Records: {stats['duplicate_records']}")
    logger.info(f"❌ Failed Records: {stats['failed_records']}")
    
    if stats['total_records'] > 0:
        success_rate = (stats['inserted_records'] / stats['total_records']) * 100
        logger.info(f"✅ Success Rate: {success_rate:.1f}%")
    
    logger.info("=" * 60)

# ============================================
# MAIN EXECUTION
# ============================================
def main(use_local=False):
    """
    Main ETL job execution.
    
    Args:
        use_local (bool): If True, use local files instead of S3
    """
    logger.info("=" * 60)
    logger.info("🚀 WEATHER DATA LOADING JOB STARTING")
    logger.info("=" * 60)
    logger.info(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🌍 Mode: {'LOCAL' if use_local else 'AWS S3'}")
    logger.info("=" * 60)
    
    try:
        # Step 1: Get data
        if use_local:
            df = get_local_processed_data()
        else:
            s3_client = create_s3_client()
            df = get_yesterdays_data(s3_client)
        
        if df.empty:
            logger.warning("⚠️  No data to process. Job complete.")
            return
        
        # Step 2: Clean data
        df = prepare_data_for_postgres(df)
        
        if df.empty:
            logger.warning("⚠️  No valid data after cleaning. Job complete.")
            return
        
        # Step 3: Connect to PostgreSQL
        conn, cursor = create_postgres_connection()
        
        # Step 4: Insert data
        insert_data_to_postgres(conn, cursor, df)
        
        # Step 5: Generate daily summary
        generate_daily_summary(conn, cursor)
        
        # Step 6: Close connection
        cursor.close()
        conn.close()
        
        logger.info("=" * 60)
        logger.info("✅ JOB COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
        # Print statistics
        print_statistics()
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"❌ JOB FAILED: {e}")
        logger.error("=" * 60)
        raise

# ============================================
# ENTRY POINT
# ============================================
if __name__ == "__main__":
    # For testing locally without AWS
    # Change to False when using real AWS S3
    USE_LOCAL = True
    
    main(use_local=USE_LOCAL)