"""
Weather Data Producer: API → Kinesis Stream
Fetches real-time weather data and sends to AWS Kinesis
"""

import json
import logging
import requests
import boto3
import time
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
        logging.FileHandler('logs/producer.log')
    ]
)
logger = logging.getLogger(__name__)

# ============================================
# CONFIGURATION
# ============================================
API_URL = "http://host.docker.internal:5000/api/weather"
KINESIS_STREAM_NAME = "weather_data_stream"
AWS_REGION = "eu-north-1"
POLL_INTERVAL_SECONDS = 2

# Statistics tracking
stats = {
    "total_api_calls": 0,
    "total_records_sent": 0,
    "failed_api_calls": 0,
    "failed_kinesis_sends": 0,
    "start_time": datetime.now()
}

# ============================================
# FUNCTION: Fetch Weather Data from API
# ============================================
def fetch_weather_data(url):
    """
    Fetch weather data from the Weather API.
    
    Args:
        url (str): API endpoint URL
        
    Returns:
        dict: Weather data with readings list, or None if failed
        
    Example response:
    {
        "readings": [
            {
                "station_id": "STATION_001",
                "city": "Mumbai",
                "temperature_celsius": 28.5,
                ...
            },
            ...
        ]
    }
    """
    try:
        stats["total_api_calls"] += 1
        
        # Send GET request with timeout
        response = requests.get(url, timeout=10)
        
        # Check if request was successful
        if response.status_code != 200:
            logger.error(f"API returned status code {response.status_code}")
            stats["failed_api_calls"] += 1
            return None
        
        # Parse JSON response
        data = response.json()
        
        # Validate response structure
        if "readings" not in data or not isinstance(data["readings"], list):
            logger.error("Invalid API response format - missing 'readings' key")
            stats["failed_api_calls"] += 1
            return None
        
        logger.info(f"Fetched {len(data['readings'])} weather readings from API")
        return data
        
    except requests.exceptions.Timeout:
        logger.error("API request timeout (>10 seconds)")
        stats["failed_api_calls"] += 1
        return None
        
    except requests.exceptions.ConnectionError:
        logger.error("Could not connect to Weather API - is it running?")
        stats["failed_api_calls"] += 1
        return None
        
    except json.JSONDecodeError:
        logger.error("API returned invalid JSON")
        stats["failed_api_calls"] += 1
        return None
        
    except Exception as e:
        logger.error(f"Unexpected error fetching data: {e}")
        stats["failed_api_calls"] += 1
        return None

# ============================================
# FUNCTION: Initialize Kinesis Client
# ============================================
def create_kinesis_client():
    """
    Create and return AWS Kinesis client.
    
    Returns:
        boto3.client: Kinesis client object
        
    Note:
        - Uses AWS credentials from ~/.aws/credentials or environment variables
        - Region is set to eu-north-1 (Stockholm)
    """
    try:
        kinesis_client = boto3.client(
            'kinesis',
            region_name=AWS_REGION
        )
        
        # Test connection by describing the stream
        kinesis_client.describe_stream(StreamName=KINESIS_STREAM_NAME)
        
        logger.info(f"Connected to Kinesis stream: {KINESIS_STREAM_NAME}")
        return kinesis_client
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'ResourceNotFoundException':
            logger.error(f"Kinesis stream '{KINESIS_STREAM_NAME}' does not exist!")
            logger.error("   Create it using Terraform first.")
        elif error_code == 'UnrecognizedClientException':
            logger.error("Invalid AWS credentials")
            logger.error("   Check ~/.aws/credentials or environment variables")
        else:
            logger.error(f"Kinesis error: {e}")
            
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error creating Kinesis client: {e}")
        sys.exit(1)

# ============================================
# FUNCTION: Send Data to Kinesis
# ============================================
def send_to_kinesis(kinesis_client, reading):
    """
    Send a single weather reading to Kinesis stream.
    
    Args:
        kinesis_client: Boto3 Kinesis client
        reading (dict): Weather reading data
        
    Returns:
        bool: True if successful, False otherwise
        
    Kinesis Partition Key Strategy:
        - Use station_id as partition key
        - This ensures all data from same station goes to same shard
        - Enables ordered processing per station
    """
    try:
        # Convert reading to JSON string
        data_string = json.dumps(reading)
        
        # Extract partition key (station_id)
        partition_key = reading.get("station_id", "unknown")
        
        # Send to Kinesis
        response = kinesis_client.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=data_string.encode('utf-8'),
            PartitionKey=partition_key
        )
        
        # Check if successful
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            stats["total_records_sent"] += 1
            
            # Get shard ID and sequence number
            shard_id = response['ShardId']
            sequence_number = response['SequenceNumber']
            
            logger.info(
                f"📤 Sent: {reading['city']} → "
                f"Shard: {shard_id} | "
                f"Seq: {sequence_number[:10]}..."
            )
            return True
        else:
            logger.error(f"Kinesis returned non-200 status: {response}")
            stats["failed_kinesis_sends"] += 1
            return False
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'ProvisionedThroughputExceededException':
            logger.error("Kinesis throughput exceeded - sending too fast!")
            logger.error("Increase POLL_INTERVAL_SECONDS or add more shards")
        else:
            logger.error(f"Kinesis error: {e}")
            
        stats["failed_kinesis_sends"] += 1
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error sending to Kinesis: {e}")
        stats["failed_kinesis_sends"] += 1
        return False

# ============================================
# FUNCTION: Print Statistics
# ============================================
def print_statistics():
    """
    Print current pipeline statistics.
    Useful for monitoring pipeline health.
    """
    runtime = datetime.now() - stats["start_time"]
    hours, remainder = divmod(runtime.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    logger.info("=" * 60)
    logger.info("PIPELINE STATISTICS")
    logger.info("=" * 60)
    logger.info(f"Runtime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    logger.info(f"API Calls: {stats['total_api_calls']} "
                f"(Failed: {stats['failed_api_calls']})")
    logger.info(f"Records Sent: {stats['total_records_sent']} "
                f"(Failed: {stats['failed_kinesis_sends']})")
    
    # Calculate success rate
    if stats['total_api_calls'] > 0:
        api_success_rate = (
            (stats['total_api_calls'] - stats['failed_api_calls']) 
            / stats['total_api_calls'] * 100
        )
        logger.info(f"API Success Rate: {api_success_rate:.1f}%")
    
    if stats['total_records_sent'] + stats['failed_kinesis_sends'] > 0:
        kinesis_success_rate = (
            stats['total_records_sent'] 
            / (stats['total_records_sent'] + stats['failed_kinesis_sends']) * 100
        )
        logger.info(f"Kinesis Success Rate: {kinesis_success_rate:.1f}%")
    
    logger.info("=" * 60)

# ============================================
# MAIN EXECUTION LOOP
# ============================================
def main():
    """
    Main execution loop:
    1. Connect to Kinesis
    2. Continuously fetch weather data
    3. Send to Kinesis
    4. Wait and repeat
    """
    logger.info("=" * 60)
    logger.info("WEATHER DATA PRODUCER STARTING")
    logger.info("=" * 60)
    logger.info(f"API URL: {API_URL}")
    logger.info(f"Kinesis Stream: {KINESIS_STREAM_NAME}")
    logger.info(f"AWS Region: {AWS_REGION}")
    logger.info(f"Poll Interval: {POLL_INTERVAL_SECONDS} seconds")
    logger.info("=" * 60)
    
    # Initialize Kinesis client
    kinesis_client = create_kinesis_client()
    
    # Statistics counter
    iteration = 0
    
    try:
        while True:
            iteration += 1
            logger.info(f"\n{'='*60}")
            logger.info(f"Iteration {iteration}")
            logger.info(f"{'='*60}")
            
            # Fetch weather data from API
            weather_data = fetch_weather_data(API_URL)
            
            if weather_data is None:
                logger.warning("No data fetched. Skipping this iteration.")
            else:
                # Send each reading to Kinesis
                readings = weather_data.get("readings", [])
                
                for reading in readings:
                    send_to_kinesis(kinesis_client, reading)
                    
                    # Small delay between sends to avoid throttling
                    time.sleep(0.1)
            
            # Print statistics every 10 iterations
            if iteration % 10 == 0:
                print_statistics()
            
            # Wait before next iteration
            logger.info(f"Waiting {POLL_INTERVAL_SECONDS} seconds before next poll...")
            time.sleep(POLL_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        logger.info("\n\n" + "="*60)
        logger.info("PRODUCER STOPPED BY USER (Ctrl+C)")
        logger.info("="*60)
        print_statistics()
        
    except Exception as e:
        logger.error(f"\nFATAL ERROR: {e}")
        print_statistics()
        sys.exit(1)

# ============================================
# SCRIPT ENTRY POINT
# ============================================
if __name__ == "__main__":
    # Create logs directory if it doesn't exist
    import os
    os.makedirs('logs', exist_ok=True)
    
    # Run main loop
    main()