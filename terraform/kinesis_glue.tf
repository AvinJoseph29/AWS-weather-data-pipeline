# ============================================
# Kinesis Data Stream
# Real-time data ingestion
# ============================================

resource "aws_kinesis_stream" "flight_stream" {
  name             = var.kinesis_stream_name
  shard_count      = var.shard_count
  retention_period = 24  # Hours (min: 24, max: 8760 = 365 days)
  
  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded"
  ]
  
  stream_mode_details {
    stream_mode = "PROVISIONED"  # or "ON_DEMAND"
  }
  
  tags = {
    Name = var.kinesis_stream_name
  }
}

# ============================================
# AWS Glue Catalog Database
# Metadata repository for tables and schemas
# ============================================

resource "aws_glue_catalog_database" "flight_db" {
  name        = var.glue_catalog_name
  description = "Weather data catalog database"
  
  # Optional: Store in specific S3 location
  # location_uri = "s3://${aws_s3_bucket.processed_data_bucket.id}/catalog/"
}

# ============================================
# AWS Glue Job
# ETL job for data transformation
# ============================================

resource "aws_glue_job" "flight_transform" {
  name     = var.glue_job_name
  role_arn = aws_iam_role.glue_role.arn
  
  # Glue version (3.0 = Spark 3.1, Python 3.7)
  glue_version = "3.0"
  
  # Worker configuration
  worker_type       = var.glue_job_worker_type
  number_of_workers = var.glue_job_number_of_workers
  
  # Execution timeout (in minutes)
  timeout = 60
  
  # Maximum retries
  max_retries = 1
  
  # Command configuration
  command {
    name            = "glueetl"
    script_location = "s3://${var.aws_s3_glue_script_bucket_name}/scripts/glue_weather_etl.py"
    python_version  = "3"
  }
  
  # Default arguments
  default_arguments = {
    # Temporary directory for Spark
    "--TempDir" = "s3://${var.aws_s3_glue_script_bucket_name}/temp/"
    
    # Job language
    "--job-language" = "python"
    
    # Enable CloudWatch logging
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    
    # Spark configuration
    "--enable-spark-ui"              = "true"
    "--spark-event-logs-path"        = "s3://${var.aws_s3_log_bucket_name}/spark-logs/"
    "--enable-job-insights"          = "true"
    
    # Additional JARs (for Kinesis connector)
    "--extra-jars" = "s3://${var.aws_s3_glue_script_bucket_name}/jars/spark-sql-kinesis-connector.jar"
    
    # Custom parameters (accessible in script)
    "--KINESIS_STREAM_NAME"      = var.kinesis_stream_name
    "--RAW_BUCKET"               = var.aws_s3_raw_data_bucket_name
    "--PROCESSED_BUCKET"         = var.aws_s3_processed_data_bucket_name
    "--ALERTS_BUCKET"            = var.aws_s3_landed_flight_bucket_name
    "--AWS_REGION"               = var.aws_region
  }
  
  # Execution class (FLEX for lower cost, slower startup)
  execution_class = "STANDARD"  # or "FLEX"
  
  # Security configuration (for encryption)
  # security_configuration = aws_glue_security_configuration.main.name
  
  tags = {
    Name = var.glue_job_name
  }
}

# ============================================
# Glue Security Configuration (Optional)
# ============================================

# Uncomment to enable encryption
# resource "aws_glue_security_configuration" "main" {
#   name = "${local.name_prefix}-glue-security-config"
#
#   encryption_configuration {
#     cloudwatch_encryption {
#       cloudwatch_encryption_mode = "SSE-KMS"
#     }
#
#     job_bookmarks_encryption {
#       job_bookmarks_encryption_mode = "CSE-KMS"
#     }
#
#     s3_encryption {
#       s3_encryption_mode = "SSE-S3"
#     }
#   }
# }

# ============================================
# Glue Trigger (Optional - Auto-run job)
# ============================================

# Uncomment to automatically trigger Glue job on schedule
# resource "aws_glue_trigger" "daily_trigger" {
#   name     = "${var.glue_job_name}-trigger"
#   type     = "SCHEDULED"
#   schedule = "cron(0 2 * * ? *)"  # Daily at 2 AM UTC
#
#   actions {
#     job_name = aws_glue_job.flight_transform.name
#   }
# }

# ============================================
# CloudWatch Log Group for Glue
# ============================================

resource "aws_cloudwatch_log_group" "glue_job_logs" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = 7  # Keep logs for 7 days
  
  tags = {
    Name = "${var.glue_job_name}-logs"
  }
}

# ============================================
# Outputs
# ============================================

output "kinesis_stream_id" {
  description = "Kinesis stream ID"
  value       = aws_kinesis_stream.flight_stream.id
}

output "glue_database_id" {
  description = "Glue catalog database ID"
  value       = aws_glue_catalog_database.flight_db.id
}

output "glue_job_id" {
  description = "Glue job ID"
  value       = aws_glue_job.flight_transform.id
}

# ============================================
# Kinesis Explanation
# ============================================

# Kinesis Shard Capacity:
# - 1 shard = 1 MB/s input OR 1,000 records/s
# - 1 shard = 2 MB/s output
#
# Example calculation:
# - Weather reading: ~500 bytes
# - 5 cities × 1 reading/sec = 5 readings/sec = 2.5 KB/s
# - 1 shard is more than enough!
#
# When to scale:
# - If > 1,000 records/sec: Add more shards
# - If > 1 MB/s input: Add more shards
#
# Cost:
# - $0.015 per shard-hour = ~$11/month per shard
# - $0.014 per 1M PUT requests

# ============================================
# Glue Worker Types
# ============================================

# G.1X (Standard):
# - 4 vCPU, 16 GB memory, 64 GB disk
# - $0.44 per DPU-hour
# - Best for: Most workloads
#
# G.2X (High Memory):
# - 8 vCPU, 32 GB memory, 128 GB disk
# - $0.88 per DPU-hour
# - Best for: Memory-intensive jobs
#
# G.025X (Micro):
# - 0.25 DPU
# - $0.44 per DPU-hour
# - Best for: Small jobs, development
#
# Example cost calculation:
# - 2 workers × G.1X = 2 DPU
# - Job runs 10 minutes = 0.167 hours
# - Cost: 2 × 0.167 × $0.44 = $0.15 per run
# - Daily (1 run): $0.15 × 30 days = $4.50/month

# ============================================
# Glue Best Practices
# ============================================

# 1. Start Small:
#    - Use 2 workers for development
#    - Scale up based on performance metrics
#
# 2. Monitor Metrics:
#    - CPU utilization
#    - Memory usage
#    - Job duration
#    - Data processed
#
# 3. Optimize Scripts:
#    - Use partitioning
#    - Minimize shuffles
#    - Cache when appropriate
#    - Use pushdown predicates
#
# 4. Cost Optimization:
#    - Use FLEX execution class for non-urgent jobs
#    - Stop job when done (don't let it run idle)
#    - Use job bookmarks to process only new data
#
# 5. Security:
#    - Enable encryption
#    - Use VPC endpoints
#    - Rotate IAM credentials
#    - Monitor access logs