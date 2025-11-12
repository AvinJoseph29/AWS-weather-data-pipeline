"""
Weather Data Consumer: Kinesis → S3 Raw Bucket
Reads weather data from Kinesis and stores in S3 as raw JSON
"""

import boto3
import logging
import time
import json
import sys
from datetime import datetime
from botocore.exceptions import ClientError

# ============================================
# LOGGING CONFIGURATION
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/consumer.log')
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION
# ============================================
RAW_DATA_BUCKET = "raw-weather-data-bucket84h674"
STREAM_NAME = "weather_data_stream"
SHARD_ID = "shardId-000000000000"  # Default shard ID for 1-shard stream
AWS_REGION = "eu-north-1"
POLL_INTERVAL_SECONDS = 1
RECORD_LIMIT = 10  # Fetch max 10 records per poll

# Statistics
stats = {
    "total_records_read": 0,
    "total_batches_saved": 0,
    "failed_reads": 0,
    "failed_writes": 0,
    "start_time": datetime.now()
}

# ============================================
# FUNCTION: Create Kinesis Client
# ============================================
def create_kinesis_client():
    """
    Create and return AWS Kinesis client.
    
    Returns:
        boto3.client: Kinesis client
    """
    try:
        kinesis = boto3.client('kinesis', region_name=AWS_REGION)
        
        # Test connection
        kinesis.describe_stream(StreamName=STREAM_NAME)
        
        logger.info(f"Connected to Kinesis stream: {STREAM_NAME}")
        return kinesis
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'ResourceNotFoundException':
            logger.error(f"Kinesis stream '{STREAM_NAME}' not found!")
        else:
            logger.error(f"Kinesis connection error: {e}")
            
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

# ============================================
# FUNCTION: Create S3 Client
# ============================================
def create_s3_client():
    """
    Create and return AWS S3 client.
    
    Returns:
        boto3.client: S3 client
    """
    try:
        s3 = boto3.client('s3', region_name=AWS_REGION)
        
        # Test connection by checking if bucket exists
        s3.head_bucket(Bucket=RAW_DATA_BUCKET)
        
        logger.info(f"Connected to S3 bucket: {RAW_DATA_BUCKET}")
        return s3
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == '404':
            logger.error(f"S3 bucket '{RAW_DATA_BUCKET}' not found!")
            logger.error("Create it using Terraform first.")
        elif error_code == '403':
            logger.error(f"No permission to access bucket '{RAW_DATA_BUCKET}'")
        else:
            logger.error(f"S3 connection error: {e}")
            
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

# ============================================
# FUNCTION: Get Shard Iterator
# ============================================
def get_shard_iterator(kinesis, stream_name, shard_id):
    """
    Get shard iterator for reading from Kinesis.
    
    Args:
        kinesis: Kinesis client
        stream_name (str): Name of Kinesis stream
        shard_id (str): ID of shard to read from
        
    Returns:
        str: Shard iterator token
        
    Iterator Types:
        - TRIM_HORIZON: Start from oldest available record
        - LATEST: Start from newest record (skip old data)
        - AT_SEQUENCE_NUMBER: Start from specific sequence
        - AFTER_SEQUENCE_NUMBER: Start after specific sequence
    """
    try:
        logger.info(f"Getting shard iterator for {stream_name}/{shard_id}")
        
        response = kinesis.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'  # Read all data from beginning
        )
        
        iterator = response['ShardIterator']
        logger.info(f"Got shard iterator: {iterator[:30]}...")
        return iterator
        
    except ClientError as e:
        logger.error(f"Failed to get shard iterator: {e}")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

# ============================================
# FUNCTION: Process and Save Records
# ============================================
def process_and_save_records(kinesis, s3, shard_iterator):
    """
    Main processing loop:
    1. Read records from Kinesis
    2. Batch them together
    3. Save to S3 with time-based partitioning
    
    Args:
        kinesis: Kinesis client
        s3: S3 client
        shard_iterator (str): Current shard iterator
        
    S3 Folder Structure:
        raw-weather-data-bucket/
        └── kinesis/
            └── year=2025/
                └── month=10/
                    └── day=25/
                        └── hour=14/
                            └── batch_<start_seq>_<end_seq>.json
    """
    iteration = 0
    
    try:
        while True:
            iteration += 1
            
            try:
                # Fetch records from Kinesis
                response = kinesis.get_records(
                    ShardIterator=shard_iterator,
                    Limit=RECORD_LIMIT
                )
                
                records = response.get('Records', [])
                
                if records:
                    logger.info(f"\n{'='*60}")
                    logger.info(f"Iteration {iteration}: Received {len(records)} records")
                    logger.info(f"{'='*60}")
                    
                    # Parse and batch records
                    batch = []
                    
                    # Use timestamp from first record for partitioning
                    first_timestamp = records[0]['ApproximateArrivalTimestamp']
                    folder_path = first_timestamp.strftime("year=%Y/month=%m/day=%d/hour=%H")
                    
                    # Decode and parse each record
                    for record in records:
                        try:
                            data = record['Data'].decode('utf-8')
                            parsed_data = json.loads(data)
                            batch.append(parsed_data)
                            
                            # Log sample data
                            if 'city' in parsed_data:
                                logger.info(f" {parsed_data['city']}: "
                                          f"{parsed_data.get('temperature_celsius', 'N/A')}°C")
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse record: {e}")
                            stats["failed_reads"] += 1
                            continue
                    
                    # Update statistics
                    stats["total_records_read"] += len(batch)
                    
                    # Save batch to S3
                    if batch:
                        first_seq = records[0]['SequenceNumber']
                        last_seq = records[-1]['SequenceNumber']
                        
                        # Create S3 key with partition path
                        s3_key = f"kinesis/{folder_path}/batch_{first_seq}_{last_seq}.json"
                        
                        try:
                            # Convert batch to JSON
                            batch_json = json.dumps(batch, indent=2)
                            
                            # Upload to S3
                            s3.put_object(
                                Bucket=RAW_DATA_BUCKET,
                                Key=s3_key,
                                Body=batch_json,
                                ContentType='application/json'
                            )
                            
                            stats["total_batches_saved"] += 1
                            
                            logger.info(f"Saved to S3: s3://{RAW_DATA_BUCKET}/{s3_key}")
                            logger.info(f"  Batch size: {len(batch)} records | "
                                      f"{len(batch_json)} bytes")
                            
                        except ClientError as e:
                            logger.error(f"Failed to save to S3: {e}")
                            stats["failed_writes"] += 1
                            
                        except Exception as e:
                            logger.error(f"Unexpected error saving to S3: {e}")
                            stats["failed_writes"] += 1
                
                else:
                    # No records available
                    if iteration % 30 == 0:  # Log every 30 seconds
                        logger.info(f"No new records (iteration {iteration})")
                
                # Get next shard iterator
                shard_iterator = response.get('NextShardIterator')
                
                if not shard_iterator:
                    logger.warning("No next shard iterator - stream may have ended")
                    break
                
                # Print statistics every 50 iterations
                if iteration % 50 == 0:
                    print_statistics()
                
                # Wait before next poll
                time.sleep(POLL_INTERVAL_SECONDS)
                
            except ClientError as e:
                logger.error(f"Kinesis read error: {e}")
                stats["failed_reads"] += 1
                time.sleep(POLL_INTERVAL_SECONDS)
                
            except Exception as e:
                logger.error(f"Unexpected error in processing loop: {e}")
                time.sleep(POLL_INTERVAL_SECONDS)
                
    except KeyboardInterrupt:
        logger.info("\n\n" + "="*60)
        logger.info("CONSUMER STOPPED BY USER (Ctrl+C)")
        logger.info("="*60)
        print_statistics()

# ============================================
# FUNCTION: Print Statistics
# ============================================
def print_statistics():
    """
    Print current consumer statistics.
    """
    runtime = datetime.now() - stats["start_time"]
    hours, remainder = divmod(runtime.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    logger.info("=" * 60)
    logger.info("CONSUMER STATISTICS")
    logger.info("=" * 60)
    logger.info(f"Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    logger.info(f"Records Read: {stats['total_records_read']} "
                f"(Failed: {stats['failed_reads']})")
    logger.info(f"Batches Saved: {stats['total_batches_saved']} "
                f"(Failed: {stats['failed_writes']})")
    
    # Calculate success rates
    total_attempts = stats['total_records_read'] + stats['failed_reads']
    if total_attempts > 0:
        read_success_rate = (stats['total_records_read'] / total_attempts * 100)
        logger.info(f"Read Success Rate: {read_success_rate:.1f}%")
    
    total_write_attempts = stats['total_batches_saved'] + stats['failed_writes']
    if total_write_attempts > 0:
        write_success_rate = (stats['total_batches_saved'] / total_write_attempts * 100)
        logger.info(f"Write Success Rate: {write_success_rate:.1f}%")
    
    logger.info("=" * 60)

# ============================================
# MAIN EXECUTION
# ============================================
def main():
    """
    Main execution:
    1. Connect to Kinesis and S3
    2. Get shard iterator
    3. Start processing records
    """
    logger.info("=" * 60)
    logger.info("WEATHER DATA CONSUMER STARTING")
    logger.info("=" * 60)
    logger.info(f"Kinesis Stream: {STREAM_NAME}")
    logger.info(f"S3 Bucket: {RAW_DATA_BUCKET}")
    logger.info(f"AWS Region: {AWS_REGION}")
    logger.info(f"⏱Poll Interval: {POLL_INTERVAL_SECONDS} seconds")
    logger.info("=" * 60)
    
    # Create logs directory
    import os
    os.makedirs('logs', exist_ok=True)
    
    # Initialize clients
    kinesis = create_kinesis_client()
    s3 = create_s3_client()
    
    # Get shard iterator
    shard_iterator = get_shard_iterator(kinesis, STREAM_NAME, SHARD_ID)
    
    # Start processing
    logger.info("Starting Kinesis → S3 streaming...")
    process_and_save_records(kinesis, s3, shard_iterator)

# ============================================
# SCRIPT ENTRY POINT
# ============================================
if __name__ == "__main__":
    main()