"""
Weather Data Pipeline - Airflow DAG
Orchestrates daily data loading from S3 to PostgreSQL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import logging
import sys
import os

# Add parent directory to path to import our module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

# Import our ETL function
from load_to_postgres import main as load_weather_data

# ============================================
# LOGGING SETUP
# ============================================
logger = logging.getLogger('weather_dag')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# ============================================
# DAG DEFAULT ARGUMENTS
# ============================================
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['alerts@weatherpipeline.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# ============================================
# TASK FUNCTIONS
# ============================================

def start_job(**context):
    """
    Task 1: Job initialization
    Logs start time and execution date
    """
    execution_date = context['execution_date']
    logger.info("=" * 60)
    logger.info("🚀 WEATHER DATA PIPELINE STARTING")
    logger.info("=" * 60)
    logger.info(f"📅 Execution Date: {execution_date}")
    logger.info(f"⏰ Start Time: {datetime.now()}")
    logger.info("=" * 60)
    
    # Store in XCom for other tasks
    context['task_instance'].xcom_push(
        key='start_time',
        value=datetime.now().isoformat()
    )

def check_prerequisites(**context):
    """
    Task 2: Check if all prerequisites are met
    - S3 bucket accessible (or local files exist)
    - PostgreSQL is up
    - Tables exist
    """
    logger.info("✓ Checking prerequisites...")
    
    # Check if using local or AWS
    use_local = True  # Change to False when using AWS
    
    if use_local:
        # Check local files exist
        import glob
        csv_files = glob.glob('local_data/processed/processed_weather_*.csv')
        
        if not csv_files:
            logger.error("❌ No local processed data files found!")
            logger.error("   Run: python scripts/test_transformations.py")
            raise FileNotFoundError("No processed data available")
        
        logger.info(f"✅ Found {len(csv_files)} local data files")
    else:
        # Check S3 access (when using AWS)
        import boto3
        try:
            s3 = boto3.client('s3', region_name='eu-north-1')
            s3.head_bucket(Bucket='weather-alerts-bucket84h674')
            logger.info("✅ S3 bucket accessible")
        except Exception as e:
            logger.error(f"❌ Cannot access S3: {e}")
            raise
    
    # Check PostgreSQL connection
    import psycopg2
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='weather_db',
            user='postgres',
            password='postgres123'
        )
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('weather_readings', 'daily_weather_summary')
        """)
        
        tables = cursor.fetchall()
        
        if len(tables) != 2:
            logger.error("❌ Required tables not found!")
            logger.error("   Run: docker exec -it weather_postgres psql -U postgres -d weather_db -f /docker-entrypoint-initdb.d/create_tables.sql")
            raise Exception("Database tables not initialized")
        
        logger.info("✅ PostgreSQL accessible, tables exist")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ PostgreSQL check failed: {e}")
        raise
    
    logger.info("✅ All prerequisites met")

def load_data_task(**context):
    """
    Task 3: Load data from S3 to PostgreSQL
    This is the main ETL task
    """
    logger.info("💾 Starting data load...")
    
    use_local = True  # Change to False when using AWS
    
    try:
        # Call our ETL function
        load_weather_data(use_local=use_local)
        
        logger.info("✅ Data load completed successfully")
        
        # Store success in XCom
        context['task_instance'].xcom_push(
            key='load_status',
            value='success'
        )
        
    except Exception as e:
        logger.error(f"❌ Data load failed: {e}")
        context['task_instance'].xcom_push(
            key='load_status',
            value='failed'
        )
        raise

def validate_data(**context):
    """
    Task 4: Validate loaded data
    Check record counts, data quality, etc.
    """
    logger.info("✓ Validating loaded data...")
    
    import psycopg2
    
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='weather_db',
        user='postgres',
        password='postgres123'
    )
    cursor = conn.cursor()
    
    # Check 1: Total records
    cursor.execute("SELECT COUNT(*) FROM weather_readings")
    total_records = cursor.fetchone()[0]
    logger.info(f"   Total records in DB: {total_records}")
    
    if total_records == 0:
        logger.error("❌ No records found in database!")
        raise ValueError("Database is empty")
    
    # Check 2: Today's records
    cursor.execute("""
        SELECT COUNT(*) 
        FROM weather_readings 
        WHERE reading_date = CURRENT_DATE - INTERVAL '1 day'
    """)
    today_records = cursor.fetchone()[0]
    logger.info(f"   Yesterday's records: {today_records}")
    
    # Check 3: Data quality
    cursor.execute("""
        SELECT 
            ROUND(AVG(data_quality_score), 2) as avg_quality,
            MIN(data_quality_score) as min_quality
        FROM weather_readings
        WHERE reading_date = CURRENT_DATE - INTERVAL '1 day'
    """)
    quality_result = cursor.fetchone()
    
    if quality_result:
        avg_quality, min_quality = quality_result
        logger.info(f"   Avg quality score: {avg_quality}%")
        logger.info(f"   Min quality score: {min_quality}%")
        
        if avg_quality < 80:
            logger.warning(f"⚠️  Low average quality: {avg_quality}%")
    
    # Check 4: Alert distribution
    cursor.execute("""
        SELECT 
            alert_level,
            COUNT(*) as count
        FROM weather_readings
        WHERE reading_date = CURRENT_DATE - INTERVAL '1 day'
        GROUP BY alert_level
    """)
    
    alert_dist = cursor.fetchall()
    logger.info("   Alert distribution:")
    for alert_level, count in alert_dist:
        logger.info(f"      {alert_level}: {count}")
    
    cursor.close()
    conn.close()
    
    logger.info("✅ Data validation passed")

def generate_report(**context):
    """
    Task 5: Generate summary report
    Creates a text report of the pipeline execution
    """
    logger.info("📊 Generating execution report...")
    
    import psycopg2
    
    # Get start time from XCom
    start_time_str = context['task_instance'].xcom_pull(
        task_ids='start_job',
        key='start_time'
    )
    start_time = datetime.fromisoformat(start_time_str)
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Get load status
    load_status = context['task_instance'].xcom_pull(
        task_ids='load_data',
        key='load_status'
    )
    
    # Query database for stats
    conn = psycopg2.connect(
        host='postgres',
        port=5432,
        database='weather_db',
        user='postgres',
        password='postgres123'
    )
    cursor = conn.cursor()
    
    # Get yesterday's summary
    cursor.execute("""
        SELECT 
            city,
            avg_temperature_celsius,
            total_precipitation_mm,
            alert_percentage,
            total_readings
        FROM daily_weather_summary
        WHERE summary_date = CURRENT_DATE - INTERVAL '1 day'
        ORDER BY city
    """)
    
    summaries = cursor.fetchall()
    
    # Build report
    report = f"""
{'=' * 60}
WEATHER DATA PIPELINE - EXECUTION REPORT
{'=' * 60}

📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
⏱️  Duration: {duration.total_seconds():.1f} seconds
✅ Status: {load_status.upper()}

{'=' * 60}
DAILY SUMMARIES (Yesterday)
{'=' * 60}

"""
    
    for city, avg_temp, precip, alert_pct, readings in summaries:
        report += f"""
City: {city}
  • Avg Temperature: {avg_temp}°C
  • Total Precipitation: {precip}mm
  • Alert Percentage: {alert_pct}%
  • Total Readings: {readings}
"""
    
    report += f"\n{'=' * 60}\n"
    
    logger.info(report)
    
    # Store report in XCom
    context['task_instance'].xcom_push(
        key='execution_report',
        value=report
    )
    
    cursor.close()
    conn.close()
    
    logger.info("✅ Report generated")

def cleanup_task(**context):
    """
    Task 6: Cleanup and finalization
    Log completion, clean temporary files, etc.
    """
    logger.info("🧹 Running cleanup tasks...")
    
    # Log completion
    execution_date = context['execution_date']
    logger.info(f"   Pipeline completed for {execution_date}")
    
    # Could add:
    # - Delete temporary files
    # - Archive old data
    # - Send notifications
    # - Update monitoring dashboard
    
    logger.info("✅ Cleanup complete")

def end_job(**context):
    """
    Task 7: Job finalization
    Final logging and status update
    """
    start_time_str = context['task_instance'].xcom_pull(
        task_ids='start_job',
        key='start_time'
    )
    start_time = datetime.fromisoformat(start_time_str)
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("=" * 60)
    logger.info("✅ WEATHER DATA PIPELINE COMPLETED")
    logger.info("=" * 60)
    logger.info(f"⏰ End Time: {end_time}")
    logger.info(f"⏱️  Total Duration: {duration.total_seconds():.1f} seconds")
    logger.info("=" * 60)

# ============================================
# DAG DEFINITION
# ============================================

# Create the DAG
dag = DAG(
    dag_id='weather_data_pipeline',
    default_args=default_args,
    description='Daily weather data loading from S3 to PostgreSQL',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    catchup=False,  # Don't run for past dates
    max_active_runs=1,  # Only one run at a time
    tags=['weather', 'etl', 'daily']
)

# ============================================
# TASK DEFINITIONS
# ============================================

# Task 1: Start
start_task = PythonOperator(
    task_id='start_job',
    python_callable=start_job,
    provide_context=True,
    dag=dag
)

# Task 2: Check Prerequisites
check_task = PythonOperator(
    task_id='check_prerequisites',
    python_callable=check_prerequisites,
    provide_context=True,
    dag=dag
)

# Dummy task for branching
ready_to_load = DummyOperator(
    task_id='ready_to_load',
    dag=dag
)

# Task 3: Load Data
load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_task,
    provide_context=True,
    dag=dag
)

# Task 4: Validate Data
validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag
)

# Task 5: Generate Report
report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag
)

# Task 6: Cleanup
cleanup_task = PythonOperator(
    task_id='cleanup',
    python_callable=cleanup_task,
    provide_context=True,
    dag=dag
)

# Task 7: End
end_task = PythonOperator(
    task_id='end_job',
    python_callable=end_job,
    provide_context=True,
    dag=dag
)

# ============================================
# TASK DEPENDENCIES
# ============================================

# Define the workflow
start_task >> check_task >> ready_to_load >> load_task >> validate_task >> report_task >> cleanup_task >> end_task

# Visual representation:
#
#   start_job
#       ↓
#   check_prerequisites
#       ↓
#   ready_to_load
#       ↓
#   load_data
#       ↓
#   validate_data
#       ↓
#   generate_report
#       ↓
#   cleanup
#       ↓
#   end_job

"""
DAG Schedule Information:
========================

Schedule: '0 2 * * *' (Cron expression)
- Minute: 0 (at the top of the hour)
- Hour: 2 (2 AM)
- Day of Month: * (every day)
- Month: * (every month)
- Day of Week: * (every day of week)

Result: Runs daily at 2:00 AM

Why 2 AM?
- Low traffic time
- Data from previous day is complete
- Before business hours start
- Gives time to fix issues before users arrive

To change schedule:
- '0 */6 * * *' = Every 6 hours
- '0 0 * * 0' = Weekly on Sunday at midnight
- '0 12 * * 1-5' = Weekdays at noon
"""