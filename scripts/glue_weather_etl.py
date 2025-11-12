"""
AWS Glue ETL Job: Weather Data Transformation
Reads from Kinesis, transforms data, writes to S3

This is the HEART of your pipeline - YOUR unique transformations!
"""

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from datetime import datetime
import sys

# ============================================
# INITIALIZE SPARK & GLUE CONTEXTS
# ============================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ============================================
# LOGGING SETUP
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger("WeatherETL")

# ============================================
# CONFIGURATION
# ============================================
KINESIS_STREAM_NAME = "weather_data_stream"
AWS_REGION = "eu-north-1"

# Output paths
PROCESSED_OUTPUT = "s3://processed-weather-data-bucket84h674/"
ALERTS_OUTPUT = "s3://weather-alerts-bucket84h674/"

# Create dated folder for alerts
today_folder = datetime.now().strftime("year=%Y/month=%m/day=%d")
ALERTS_OUTPUT_PATH = f"{ALERTS_OUTPUT}{today_folder}/"

# ============================================
# SCHEMA DEFINITION
# ============================================
def get_weather_schema():
    """
    Define the schema for incoming weather data.
    
    This ensures Spark knows the data types and structure.
    Important for performance and data quality!
    """
    logger.info("📋 Defining weather data schema...")
    
    schema = StructType([
        StructField("readings", ArrayType(
            StructType([
                StructField("station_id", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("timestamp", StringType(), True),
                StructField("temperature_celsius", DoubleType(), True),
                StructField("humidity_percent", DoubleType(), True),
                StructField("pressure_hpa", DoubleType(), True),
                StructField("wind_speed_kmh", DoubleType(), True),
                StructField("wind_direction", StringType(), True),
                StructField("precipitation_mm", DoubleType(), True),
                StructField("weather_condition", StringType(), True),
                StructField("visibility_km", DoubleType(), True),
                StructField("uv_index", IntegerType(), True),
                StructField("heat_index_celsius", DoubleType(), True)
            ])
        ), True)
    ])
    
    logger.info("✅ Schema defined successfully")
    return schema

# ============================================
# FUNCTION: Read from Kinesis
# ============================================
def read_from_kinesis():
    """
    Create streaming DataFrame from Kinesis.
    
    Returns:
        DataFrame: Raw streaming data from Kinesis
        
    Note:
        - Uses TRIM_HORIZON to read all available data
        - For production, might use LATEST to skip old data
    """
    logger.info(f"📡 Connecting to Kinesis stream: {KINESIS_STREAM_NAME}")
    
    try:
        raw_df = (
            spark.readStream
            .format("aws-kinesis")
            .option("streamName", KINESIS_STREAM_NAME)
            .option("region", AWS_REGION)
            .option("initialPosition", "TRIM_HORIZON")
            .option("awsSTSRoleARN", "arn:aws:iam::YOUR_ACCOUNT:role/GlueRole")
            .load()
        )
        
        logger.info("✅ Successfully connected to Kinesis stream")
        return raw_df
        
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kinesis: {e}")
        sys.exit(1)

# ============================================
# FUNCTION: Parse and Flatten Data
# ============================================
def parse_and_flatten(raw_df, schema):
    """
    Parse JSON from Kinesis and flatten nested structure.
    
    Kinesis data comes as:
    - Binary data in 'data' column
    - Nested JSON structure
    
    We need to:
    1. Cast binary to string
    2. Parse JSON with schema
    3. Explode array to individual rows
    4. Flatten nested fields
    
    Args:
        raw_df: Raw DataFrame from Kinesis
        schema: Expected data schema
        
    Returns:
        DataFrame: Flattened weather readings
    """
    logger.info("🔄 Parsing and flattening data...")
    
    # Step 1: Parse JSON
    parsed_df = raw_df.withColumn(
        "data",
        from_json(col("data").cast("string"), schema)
    ).select("data.*")
    
    logger.info("  ✅ JSON parsed")
    
    # Step 2: Explode array (one row per reading)
    exploded_df = parsed_df.select(
        explode(col("readings")).alias("reading")
    )
    
    logger.info("  ✅ Array exploded")
    
    # Step 3: Flatten nested structure
    flattened_df = exploded_df.select(
        col("reading.station_id").alias("station_id"),
        col("reading.city").alias("city"),
        col("reading.country").alias("country"),
        col("reading.latitude").alias("latitude"),
        col("reading.longitude").alias("longitude"),
        col("reading.timestamp").alias("timestamp"),
        col("reading.temperature_celsius").alias("temperature_celsius"),
        col("reading.humidity_percent").alias("humidity_percent"),
        col("reading.pressure_hpa").alias("pressure_hpa"),
        col("reading.wind_speed_kmh").alias("wind_speed_kmh"),
        col("reading.wind_direction").alias("wind_direction"),
        col("reading.precipitation_mm").alias("precipitation_mm"),
        col("reading.weather_condition").alias("weather_condition"),
        col("reading.visibility_km").alias("visibility_km"),
        col("reading.uv_index").alias("uv_index"),
        col("reading.heat_index_celsius").alias("heat_index_celsius")
    )
    
    logger.info("  ✅ Data flattened")
    logger.info("✅ Parsing complete")
    
    return flattened_df

# ============================================
# TRANSFORMATION 1: Temperature Conversions
# ============================================
def add_temperature_conversions(df):
    """
    Add Fahrenheit and Kelvin temperatures.
    
    YOUR UNIQUE TRANSFORMATION #1
    
    Formulas:
    - Fahrenheit = (Celsius × 9/5) + 32
    - Kelvin = Celsius + 273.15
    """
    logger.info("🌡️  Adding temperature conversions...")
    
    df = df.withColumn(
        "temperature_fahrenheit",
        round((col("temperature_celsius") * 9/5) + 32, 1)
    )
    
    df = df.withColumn(
        "temperature_kelvin",
        round(col("temperature_celsius") + 273.15, 1)
    )
    
    logger.info("  ✅ Added Fahrenheit and Kelvin")
    return df

# ============================================
# TRANSFORMATION 2: Comfort Index Classification
# ============================================
def add_comfort_classification(df):
    """
    Classify comfort level based on heat index.
    
    YOUR UNIQUE TRANSFORMATION #2
    
    Classification:
    - < 27°C: Comfortable
    - 27-32°C: Caution (fatigue possible)
    - 32-41°C: Extreme Caution (heat exhaustion possible)
    - 41-54°C: Danger (heat stroke likely)
    - > 54°C: Extreme Danger (imminent heat stroke)
    
    Source: NOAA Heat Index Chart
    """
    logger.info("😌 Classifying comfort levels...")
    
    df = df.withColumn(
        "comfort_level",
        when(col("heat_index_celsius") < 27, "Comfortable")
        .when(col("heat_index_celsius") < 32, "Caution")
        .when(col("heat_index_celsius") < 41, "Extreme Caution")
        .when(col("heat_index_celsius") < 54, "Danger")
        .otherwise("Extreme Danger")
    )
    
    logger.info("  ✅ Comfort levels classified")
    return df

# ============================================
# TRANSFORMATION 3: Weather Severity
# ============================================
def add_weather_severity(df):
    """
    Classify overall weather severity.
    
    YOUR UNIQUE TRANSFORMATION #3
    
    Considers multiple factors:
    - Precipitation (rain/storm)
    - Wind speed (dangerous winds)
    - Visibility (fog/hazardous)
    - UV index (sun exposure risk)
    """
    logger.info("⚠️  Calculating weather severity...")
    
    df = df.withColumn(
        "weather_severity",
        when(col("precipitation_mm") > 50, "Extreme")
        .when(col("precipitation_mm") > 25, "Severe")
        .when(col("wind_speed_kmh") > 60, "Severe")
        .when(col("visibility_km") < 1, "Severe")
        .when(
            (col("precipitation_mm") > 10) | 
            (col("wind_speed_kmh") > 40) | 
            (col("visibility_km") < 3),
            "Moderate"
        )
        .when(col("uv_index") > 10, "Moderate")
        .otherwise("Normal")
    )
    
    logger.info("  ✅ Weather severity calculated")
    return df

# ============================================
# TRANSFORMATION 4: Alert Level
# ============================================
def add_alert_level(df):
    """
    Determine if weather conditions warrant an alert.
    
    YOUR UNIQUE TRANSFORMATION #4
    
    Alert Criteria:
    - Extreme heat (heat index > 41°C)
    - Heavy precipitation (> 25mm)
    - High winds (> 60 km/h)
    - Low visibility (< 2km)
    - Extreme UV (> 10)
    """
    logger.info("🚨 Determining alert levels...")
    
    df = df.withColumn(
        "alert_level",
        when(
            (col("heat_index_celsius") > 54) |
            (col("precipitation_mm") > 50) |
            (col("wind_speed_kmh") > 80),
            "CRITICAL"
        )
        .when(
            (col("heat_index_celsius") > 41) |
            (col("precipitation_mm") > 25) |
            (col("wind_speed_kmh") > 60) |
            (col("visibility_km") < 2) |
            (col("uv_index") > 10),
            "WARNING"
        )
        .when(
            (col("heat_index_celsius") > 32) |
            (col("precipitation_mm") > 10) |
            (col("wind_speed_kmh") > 40) |
            (col("uv_index") > 8),
            "WATCH"
        )
        .otherwise("NORMAL")
    )
    
    logger.info("  ✅ Alert levels assigned")
    return df

# ============================================
# TRANSFORMATION 5: Time-based Features
# ============================================
def add_time_features(df):
    """
    Extract time-based features from timestamp.
    
    YOUR UNIQUE TRANSFORMATION #5
    
    Features:
    - Hour of day (0-23)
    - Day of week (Monday=1)
    - Is weekend (boolean)
    - Season (based on month)
    - Time of day (Morning/Afternoon/Evening/Night)
    """
    logger.info("⏰ Adding time-based features...")
    
    # Convert string timestamp to timestamp type
    df = df.withColumn(
        "timestamp_parsed",
        to_timestamp(col("timestamp"))
    )
    
    # Extract hour
    df = df.withColumn(
        "hour_of_day",
        hour(col("timestamp_parsed"))
    )
    
    # Extract day of week
    df = df.withColumn(
        "day_of_week",
        dayofweek(col("timestamp_parsed"))
    )
    
    # Is weekend
    df = df.withColumn(
        "is_weekend",
        when(col("day_of_week").isin([1, 7]), True).otherwise(False)
    )
    
    # Time of day classification
    df = df.withColumn(
        "time_of_day",
        when(col("hour_of_day").between(6, 11), "Morning")
        .when(col("hour_of_day").between(12, 17), "Afternoon")
        .when(col("hour_of_day").between(18, 21), "Evening")
        .otherwise("Night")
    )
    
    # Season (simplified - Northern Hemisphere)
    df = df.withColumn(
        "season",
        when(month(col("timestamp_parsed")).isin([12, 1, 2]), "Winter")
        .when(month(col("timestamp_parsed")).isin([3, 4, 5]), "Spring")
        .when(month(col("timestamp_parsed")).isin([6, 7, 8]), "Summer")
        .otherwise("Autumn")
    )
    
    logger.info("  ✅ Time features added")
    return df

# ============================================
# TRANSFORMATION 6: Data Quality Metrics
# ============================================
def add_quality_metrics(df):
    """
    Add data quality indicators.
    
    YOUR UNIQUE TRANSFORMATION #6
    
    Quality checks:
    - Temperature in valid range
    - Humidity 0-100%
    - Pressure realistic
    - No null critical fields
    """
    logger.info("✓ Adding data quality metrics...")
    
    df = df.withColumn(
        "is_valid_temperature",
        col("temperature_celsius").between(-50, 60)
    )
    
    df = df.withColumn(
        "is_valid_humidity",
        col("humidity_percent").between(0, 100)
    )
    
    df = df.withColumn(
        "is_valid_pressure",
        col("pressure_hpa").between(950, 1050)
    )
    
    df = df.withColumn(
        "data_quality_score",
        (
            col("is_valid_temperature").cast("int") +
            col("is_valid_humidity").cast("int") +
            col("is_valid_pressure").cast("int")
        ) / 3 * 100
    )
    
    logger.info("  ✅ Quality metrics added")
    return df

# ============================================
# MAIN TRANSFORMATION PIPELINE
# ============================================
def apply_transformations(flattened_df):
    """
    Apply all transformations in sequence.
    
    This is YOUR data transformation logic!
    Each function adds value to the raw data.
    """
    logger.info("="*60)
    logger.info("🔧 APPLYING TRANSFORMATIONS")
    logger.info("="*60)
    
    # Apply each transformation
    processed_df = flattened_df
    processed_df = add_temperature_conversions(processed_df)
    processed_df = add_comfort_classification(processed_df)
    processed_df = add_weather_severity(processed_df)
    processed_df = add_alert_level(processed_df)
    processed_df = add_time_features(processed_df)
    processed_df = add_quality_metrics(processed_df)
    
    logger.info("="*60)
    logger.info("✅ ALL TRANSFORMATIONS COMPLETE")
    logger.info("="*60)
    
    return processed_df

# ============================================
# FUNCTION: Write Processed Data to S3
# ============================================
def write_processed_data(processed_df):
    """
    Write all processed data to S3.
    
    Format: Parquet (columnar, compressed)
    Partitioning: By date/hour for efficient queries
    Mode: Append (add new data without overwriting)
    """
    logger.info("💾 Writing processed data to S3...")
    
    query = (
        processed_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", PROCESSED_OUTPUT)
        .option("checkpointLocation", f"{PROCESSED_OUTPUT}checkpoints/")
        .partitionBy("year", "month", "day", "hour")
        .start()
    )
    
    logger.info(f"  ✅ Writing to: {PROCESSED_OUTPUT}")
    return query

# ============================================
# FUNCTION: Write Weather Alerts to S3
# ============================================
def write_weather_alerts(processed_df):
    """
    Filter and write only weather alerts to separate bucket.
    
    Only includes readings with WARNING or CRITICAL alerts.
    This creates a focused dataset for monitoring.
    """
    logger.info("🚨 Filtering and writing weather alerts...")
    
    # Filter only alerts
    alerts_df = processed_df.filter(
        col("alert_level").isin(["WARNING", "CRITICAL"])
    )
    
    # Select relevant columns for alerts
    alerts_df = alerts_df.select(
        "station_id",
        "city",
        "country",
        "timestamp",
        "temperature_celsius",
        "heat_index_celsius",
        "precipitation_mm",
        "wind_speed_kmh",
        "visibility_km",
        "uv_index",
        "weather_condition",
        "comfort_level",
        "weather_severity",
        "alert_level"
    )
    
    query = (
        alerts_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", ALERTS_OUTPUT_PATH)
        .option("checkpointLocation", f"{ALERTS_OUTPUT}checkpoints/")
        .start()
    )
    
    logger.info(f"  ✅ Writing alerts to: {ALERTS_OUTPUT_PATH}")
    return query

# ============================================
# MAIN EXECUTION
# ============================================
def main():
    """
    Main ETL pipeline execution.
    
    Flow:
    1. Read from Kinesis
    2. Parse and flatten
    3. Apply transformations
    4. Write to two destinations:
       - All processed data → Processed bucket
       - Weather alerts only → Alerts bucket
    5. Keep running (streaming job)
    """
    logger.info("="*60)
    logger.info("🌦️  WEATHER ETL JOB STARTING")
    logger.info("="*60)
    logger.info(f"📡 Source: Kinesis ({KINESIS_STREAM_NAME})")
    logger.info(f"📦 Output: S3 ({PROCESSED_OUTPUT})")
    logger.info(f"🚨 Alerts: S3 ({ALERTS_OUTPUT_PATH})")
    logger.info("="*60)
    
    try:
        # Step 1: Read from Kinesis
        raw_df = read_from_kinesis()
        
        # Step 2: Get schema
        schema = get_weather_schema()
        
        # Step 3: Parse and flatten
        flattened_df = parse_and_flatten(raw_df, schema)
        
        # Step 4: Apply transformations
        processed_df = apply_transformations(flattened_df)
        
        # Step 5: Write processed data
        processed_query = write_processed_data(processed_df)
        
        # Step 6: Write alerts
        alerts_query = write_weather_alerts(processed_df)
        
        logger.info("="*60)
        logger.info("✅ ETL JOB RUNNING")
        logger.info("="*60)
        logger.info("📊 Processing weather data in real-time...")
        logger.info("⏸️  Job will run until manually stopped")
        logger.info("="*60)
        
        # Keep job running
        processed_query.awaitTermination()
        alerts_query.awaitTermination()
        
    except Exception as e:
        logger.error("="*60)
        logger.error(f"❌ ETL JOB FAILED: {e}")
        logger.error("="*60)
        raise

# ============================================
# ENTRY POINT
# ============================================
if __name__ == "__main__":
    main()