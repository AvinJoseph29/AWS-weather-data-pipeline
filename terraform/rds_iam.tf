# ============================================
# RDS PostgreSQL Instance
# Managed relational database
# ============================================

resource "aws_db_instance" "postgres_instance" {
  # Basic configuration
  identifier     = var.db_instance_identifier
  engine         = "postgres"
  engine_version = var.db_engine_version
  instance_class = var.db_instance_class
  
  # Storage
  allocated_storage     = var.db_allocated_storage
  max_allocated_storage = var.db_allocated_storage * 2  # Auto-scaling limit
  storage_type          = "gp3"  # General Purpose SSD v3
  storage_encrypted     = var.enable_encryption
  
  # Database credentials
  username = var.db_username
  password = var.db_password  # Use AWS Secrets Manager in production!
  
  # Network
  db_subnet_group_name   = aws_db_subnet_group.rds_subnet_group.name
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  publicly_accessible    = true  # Set false for production!
  
  # High availability
  multi_az = var.db_multi_az
  
  # Backup
  backup_retention_period  = var.db_backup_retention_period
  backup_window            = "03:00-04:00"  # UTC
  maintenance_window       = "Mon:04:00-Mon:05:00"  # UTC
  delete_automated_backups = true
  
  # Monitoring
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]
  monitoring_interval             = var.enable_monitoring ? 60 : 0
  monitoring_role_arn             = var.enable_monitoring ? aws_iam_role.rds_monitoring_role[0].arn : null
  performance_insights_enabled    = var.enable_monitoring
  
  # Protection
  deletion_protection = var.enable_deletion_protection
  skip_final_snapshot = true  # Set false for production!
  # final_snapshot_identifier = "${var.db_instance_identifier}-final-snapshot"
  
  # Parameters
  parameter_group_name = aws_db_parameter_group.postgres_params.name
  
  # Apply changes immediately (careful in production!)
  apply_immediately = true
  
  tags = {
    Name = var.db_instance_identifier
  }
}

# ============================================
# RDS Parameter Group
# Database configuration parameters
# ============================================

resource "aws_db_parameter_group" "postgres_params" {
  name   = "${local.name_prefix}-postgres-params"
  family = "postgres16"
  
  # Optimize for small instance
  parameter {
    name  = "shared_buffers"
    value = "256MB"
  }
  
  parameter {
    name  = "max_connections"
    value = "100"
  }
  
  parameter {
    name  = "work_mem"
    value = "4MB"
  }
  
  # Logging
  parameter {
    name  = "log_connections"
    value = "1"
  }
  
  parameter {
    name  = "log_disconnections"
    value = "1"
  }
  
  tags = {
    Name = "${local.name_prefix}-postgres-params"
  }
}

# ============================================
# RDS Monitoring Role
# ============================================

resource "aws_iam_role" "rds_monitoring_role" {
  count = var.enable_monitoring ? 1 : 0
  
  name = "${local.name_prefix}-rds-monitoring-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "monitoring.rds.amazonaws.com"
        }
      }
    ]
  })
  
  tags = {
    Name = "${local.name_prefix}-rds-monitoring-role"
  }
}

resource "aws_iam_role_policy_attachment" "rds_monitoring_policy" {
  count = var.enable_monitoring ? 1 : 0
  
  role       = aws_iam_role.rds_monitoring_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# ============================================
# IAM Role for AWS Glue
# ============================================

resource "aws_iam_role" "glue_role" {
  name = "${local.name_prefix}-glue-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
  
  tags = {
    Name = "${local.name_prefix}-glue-role"
  }
}

# Attach AWS managed policy for Glue
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Custom policy for S3 and Kinesis access
resource "aws_iam_role_policy" "glue_custom_policy" {
  name = "${local.name_prefix}-glue-policy"
  role = aws_iam_role.glue_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_data_bucket.arn,
          "${aws_s3_bucket.raw_data_bucket.arn}/*",
          aws_s3_bucket.processed_data_bucket.arn,
          "${aws_s3_bucket.processed_data_bucket.arn}/*",
          aws_s3_bucket.landed_flight_bucket.arn,
          "${aws_s3_bucket.landed_flight_bucket.arn}/*",
          aws_s3_bucket.glue_script_bucket.arn,
          "${aws_s3_bucket.glue_script_bucket.arn}/*",
          aws_s3_bucket.log_bucket.arn,
          "${aws_s3_bucket.log_bucket.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards",
          "kinesis:SubscribeToShard"
        ]
        Resource = aws_kinesis_stream.flight_stream.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/aws-glue/*"
      }
    ]
  })
}

# ============================================
# IAM Role for Lambda (if using Lambda)
# ============================================

resource "aws_iam_role" "lambda_role" {
  name = "${local.name_prefix}-lambda-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
  
  tags = {
    Name = "${local.name_prefix}-lambda-role"
  }
}

# Lambda basic execution role
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Lambda VPC execution role
resource "aws_iam_role_policy_attachment" "lambda_vpc_execution" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# Custom Lambda policy
resource "aws_iam_role_policy" "lambda_custom_policy" {
  name = "${local.name_prefix}-lambda-policy"
  role = aws_iam_role.lambda_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.processed_data_bucket.arn,
          "${aws_s3_bucket.processed_data_bucket.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords"
        ]
        Resource = aws_kinesis_stream.flight_stream.arn
      }
    ]
  })
}

# ============================================
# IAM User for Local Development (Optional)
# ============================================

# Uncomment to create IAM user for local scripts
# resource "aws_iam_user" "developer" {
#   name = "${local.name_prefix}-developer"
#   
#   tags = {
#     Name = "${local.name_prefix}-developer"
#   }
# }
#
# resource "aws_iam_access_key" "developer_key" {
#   user = aws_iam_user.developer.name
# }
#
# resource "aws_iam_user_policy" "developer_policy" {
#   name = "${local.name_prefix}-developer-policy"
#   user = aws_iam_user.developer.name
#   
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Action = [
#           "s3:*",
#           "kinesis:*",
#           "glue:*"
#         ]
#         Resource = "*"
#       }
#     ]
#   })
# }

# ============================================
# Outputs
# ============================================

output "rds_database_name" {
  description = "RDS database name"
  value       = "postgres"
}

output "rds_username" {
  description = "RDS master username"
  value       = var.db_username
  sensitive   = true
}

output "rds_port" {
  description = "RDS port"
  value       = aws_db_instance.postgres_instance.port
}

output "glue_role_arn" {
  description = "Glue IAM role ARN"
  value       = aws_iam_role.glue_role.arn
}

output "lambda_role_arn" {
  description = "Lambda IAM role ARN"
  value       = aws_iam_role.lambda_role.arn
}

# ============================================
# RDS Cost Optimization
# ============================================

# Instance Types and Pricing (approximate):
#
# Free Tier Eligible:
# - db.t3.micro: 1 vCPU, 1 GB RAM - FREE (750 hours/month)
#
# Other Options:
# - db.t3.small: 2 vCPU, 2 GB RAM - ~$25/month
# - db.t3.medium: 2 vCPU, 4 GB RAM - ~$50/month
# - db.r6g.large: 2 vCPU, 16 GB RAM - ~$120/month (memory-optimized)
#
# Storage: ~$0.115/GB/month (gp3)
# Multi-AZ: Doubles the instance cost
# Backups: Free up to DB size

# Cost Optimization Tips:
# 1. Start with db.t3.micro (free tier)
# 2. Enable Multi-AZ only in production
# 3. Use automated backups (free)
# 4. Delete manual snapshots when not needed
# 5. Monitor CPU/memory and right-size
# 6. Use Reserved Instances for 1-3 year savings

# ============================================
# RDS Best Practices
# ============================================

# 1. Security:
#    ✅ Use private subnets in production
#    ✅ Store passwords in Secrets Manager
#    ✅ Enable encryption at rest
#    ✅ Enable SSL/TLS for connections
#    ✅ Use IAM database authentication

# 2. High Availability:
#    ✅ Enable Multi-AZ for production
#    ✅ Use read replicas for read-heavy workloads
#    ✅ Enable automated backups
#    ✅ Test disaster recovery procedures

# 3. Performance:
#    ✅ Right-size instance based on metrics
#    ✅ Use Performance Insights
#    ✅ Optimize queries and indexes
#    ✅ Monitor slow query logs
#    ✅ Use connection pooling

# 4. Maintenance:
#    ✅ Schedule maintenance windows
#    ✅ Enable auto minor version upgrade
#    ✅ Monitor disk space
#    ✅ Regular parameter tuning
#    ✅ Keep PostgreSQL updated

# 5. Monitoring:
#    ✅ Enable Enhanced Monitoring
#    ✅ Set up CloudWatch alarms
#    ✅ Monitor key metrics:
#       - CPU utilization
#       - Free storage space
#       - Database connections
#       - Read/write IOPS
#       - Replication lag (Multi-AZ)

# ============================================
# IAM Best Practices
# ============================================

# 1. Least Privilege:
#    - Grant minimum permissions needed
#    - Use specific resource ARNs
#    - Avoid wildcards when possible

# 2. Separation of Concerns:
#    - Different roles for different services
#    - Don't reuse roles across services
#    - Use service-specific policies

# 3. Security:
#    - Enable MFA for sensitive operations
#    - Rotate credentials regularly
#    - Use temporary credentials when possible
#    - Monitor IAM access with CloudTrail

# 4. Organization:
#    - Use meaningful role names
#    - Add descriptions and tags
#    - Document policy purposes
#    - Regular access reviews

# Connection String Format:
# postgresql://username:password@endpoint:5432/database
#
# Example:
# postgresql://postgres:PASSWORD@weather-postgres.xxx.eu-north-1.rds.amazonaws.com:5432/postgres