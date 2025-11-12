# ============================================
# S3 Buckets
# Storage for raw, processed, and script data
# ============================================

# ============================================
# 1. Raw Data Bucket
# Stores raw weather data from Kinesis
# ============================================

resource "aws_s3_bucket" "raw_data_bucket" {
  bucket        = var.aws_s3_raw_data_bucket_name
  force_destroy = true  # Allows deletion even if not empty (careful in prod!)
  
  tags = {
    Name        = var.aws_s3_raw_data_bucket_name
    Purpose     = "Raw weather data from Kinesis"
    DataType    = "Raw"
    Criticality = "High"
  }
}

resource "aws_s3_bucket_versioning" "raw_data_versioning" {
  bucket = aws_s3_bucket.raw_data_bucket.id
  
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data_encryption" {
  bucket = aws_s3_bucket.raw_data_bucket.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_data_lifecycle" {
  bucket = aws_s3_bucket.raw_data_bucket.id
  
  rule {
    id     = "archive-old-data"
    status = "Enabled"
    
    # Move to cheaper storage after 30 days
    transition {
      days          = 30
      storage_class = "STANDARD_IA"  # Infrequent Access
    }
    
    # Move to Glacier after 90 days
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    # Delete after 365 days
    expiration {
      days = 365
    }
  }
}

# ============================================
# 2. Processed Data Bucket
# Stores transformed weather data from Glue
# ============================================

resource "aws_s3_bucket" "processed_data_bucket" {
  bucket        = var.aws_s3_processed_data_bucket_name
  force_destroy = true
  
  tags = {
    Name        = var.aws_s3_processed_data_bucket_name
    Purpose     = "Processed weather data"
    DataType    = "Processed"
    Criticality = "High"
  }
}

resource "aws_s3_bucket_versioning" "processed_data_versioning" {
  bucket = aws_s3_bucket.processed_data_bucket.id
  
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_data_encryption" {
  bucket = aws_s3_bucket.processed_data_bucket.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "processed_data_lifecycle" {
  bucket = aws_s3_bucket.processed_data_bucket.id
  
  rule {
    id     = "archive-processed-data"
    status = "Enabled"
    
    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 180
      storage_class = "GLACIER"
    }
    
    expiration {
      days = 730  # 2 years
    }
  }
}

# ============================================
# 3. Weather Alerts Bucket
# Stores extreme weather events
# ============================================

resource "aws_s3_bucket" "landed_flight_bucket" {
  bucket        = var.aws_s3_landed_flight_bucket_name
  force_destroy = true
  
  tags = {
    Name        = var.aws_s3_landed_flight_bucket_name
    Purpose     = "Weather alerts and extreme conditions"
    DataType    = "Alerts"
    Criticality = "Critical"
  }
}

resource "aws_s3_bucket_versioning" "landed_flight_versioning" {
  bucket = aws_s3_bucket.landed_flight_bucket.id
  
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landed_flight_encryption" {
  bucket = aws_s3_bucket.landed_flight_bucket.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "landed_flight_lifecycle" {
  bucket = aws_s3_bucket.landed_flight_bucket.id
  
  rule {
    id     = "retain-alerts"
    status = "Enabled"
    
    # Keep alerts longer (critical data)
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 365
      storage_class = "GLACIER"
    }
    
    expiration {
      days = 1825  # 5 years
    }
  }
}

# ============================================
# 4. Glue Scripts Bucket
# Stores ETL scripts and JAR files
# ============================================

resource "aws_s3_bucket" "glue_script_bucket" {
  bucket        = var.aws_s3_glue_script_bucket_name
  force_destroy = true
  
  tags = {
    Name        = var.aws_s3_glue_script_bucket_name
    Purpose     = "Glue ETL scripts and dependencies"
    DataType    = "Code"
    Criticality = "Medium"
  }
}

resource "aws_s3_bucket_versioning" "glue_script_versioning" {
  bucket = aws_s3_bucket.glue_script_bucket.id
  
  versioning_configuration {
    status = "Enabled"  # Always version code!
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_script_encryption" {
  bucket = aws_s3_bucket.glue_script_bucket.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# No lifecycle policy for scripts - keep them indefinitely

# ============================================
# 5. Logs Bucket
# Stores application and service logs
# ============================================

resource "aws_s3_bucket" "log_bucket" {
  bucket        = var.aws_s3_log_bucket_name
  force_destroy = true
  
  tags = {
    Name        = var.aws_s3_log_bucket_name
    Purpose     = "Application and service logs"
    DataType    = "Logs"
    Criticality = "Medium"
  }
}

resource "aws_s3_bucket_versioning" "log_versioning" {
  bucket = aws_s3_bucket.log_bucket.id
  
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "log_encryption" {
  bucket = aws_s3_bucket.log_bucket.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "log_lifecycle" {
  bucket = aws_s3_bucket.log_bucket.id
  
  rule {
    id     = "delete-old-logs"
    status = "Enabled"
    
    # Logs don't need long retention
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    expiration {
      days = 90
    }
  }
}

# ============================================
# Bucket Policies (Optional - for public access)
# ============================================

# Example: Make logs readable by CloudWatch
# resource "aws_s3_bucket_policy" "log_bucket_policy" {
#   bucket = aws_s3_bucket.log_bucket.id
#
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Sid       = "AWSCloudWatchLogsAccess"
#         Effect    = "Allow"
#         Principal = {
#           Service = "logs.amazonaws.com"
#         }
#         Action   = "s3:PutObject"
#         Resource = "${aws_s3_bucket.log_bucket.arn}/*"
#       }
#     ]
#   })
# }

# ============================================
# Outputs
# ============================================

output "raw_data_bucket_arn" {
  description = "Raw data bucket ARN"
  value       = aws_s3_bucket.raw_data_bucket.arn
}

output "processed_data_bucket_arn" {
  description = "Processed data bucket ARN"
  value       = aws_s3_bucket.processed_data_bucket.arn
}

output "alerts_bucket_arn" {
  description = "Weather alerts bucket ARN"
  value       = aws_s3_bucket.landed_flight_bucket.arn
}

output "glue_scripts_bucket_arn" {
  description = "Glue scripts bucket ARN"
  value       = aws_s3_bucket.glue_script_bucket.arn
}

output "logs_bucket_arn" {
  description = "Logs bucket ARN"
  value       = aws_s3_bucket.log_bucket.arn
}

# ============================================
# S3 Cost Optimization
# ============================================

# Storage Classes and Pricing (approximate):
#
# 1. STANDARD: $0.023/GB/month
#    - Frequent access
#    - High performance
#    - Use for: Current data, hot data
#
# 2. STANDARD_IA: $0.0125/GB/month
#    - Infrequent access
#    - Lower cost, retrieval fee
#    - Use for: Data accessed monthly
#
# 3. GLACIER: $0.004/GB/month
#    - Archive storage
#    - Much lower cost, hours to retrieve
#    - Use for: Compliance, historical data
#
# 4. GLACIER_DEEP_ARCHIVE: $0.00099/GB/month
#    - Lowest cost
#    - 12 hours to retrieve
#    - Use for: Long-term archives

# Lifecycle Best Practices:
# - Transition frequently accessed data after 30 days
# - Archive old data to Glacier after 90 days
# - Delete data that's no longer needed
# - Use versioning for critical data
# - Enable encryption at rest
# - Use bucket policies for access control

# Example Cost Calculation:
# - 100 GB raw data: $2.30/month (STANDARD)
# - After 30 days → IA: $1.25/month
# - After 90 days → Glacier: $0.40/month
# - Total savings: ~82% after lifecycle transitions