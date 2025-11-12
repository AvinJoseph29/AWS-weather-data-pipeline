# ============================================
# Terraform Variables
# Define all configurable parameters
# ============================================

# ============================================
# Global Variables
# ============================================

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "weather-pipeline"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "eu-north-1"
}

# ============================================
# S3 Bucket Variables
# ============================================

variable "aws_s3_raw_data_bucket_name" {
  description = "S3 bucket for raw weather data from Kinesis"
  type        = string
}

variable "aws_s3_log_bucket_name" {
  description = "S3 bucket for application and Glue logs"
  type        = string
}

variable "aws_s3_glue_script_bucket_name" {
  description = "S3 bucket for Glue ETL scripts and JARs"
  type        = string
}

variable "aws_s3_landed_flight_bucket_name" {
  description = "S3 bucket for weather alerts (landed data)"
  type        = string
}

variable "aws_s3_processed_data_bucket_name" {
  description = "S3 bucket for processed and transformed weather data"
  type        = string
}

# ============================================
# Kinesis Variables
# ============================================

variable "kinesis_stream_name" {
  description = "Name of the Kinesis data stream"
  type        = string
  default     = "weather_data_stream"
}

variable "shard_count" {
  description = "Number of shards for Kinesis stream (1 shard = 1MB/s input)"
  type        = number
  default     = 1
  
  validation {
    condition     = var.shard_count > 0 && var.shard_count <= 10
    error_message = "Shard count must be between 1 and 10."
  }
}

# ============================================
# Glue Variables
# ============================================

variable "glue_catalog_name" {
  description = "Name of the Glue catalog database"
  type        = string
  default     = "weather_catalog"
}

variable "glue_job_name" {
  description = "Name of the Glue ETL job"
  type        = string
  default     = "weather_etl_job"
}

variable "glue_job_worker_type" {
  description = "Glue job worker type (G.1X, G.2X, G.025X)"
  type        = string
  default     = "G.1X"
  
  validation {
    condition     = contains(["G.1X", "G.2X", "G.025X"], var.glue_job_worker_type)
    error_message = "Worker type must be G.1X, G.2X, or G.025X."
  }
}

variable "glue_job_number_of_workers" {
  description = "Number of workers for Glue job"
  type        = number
  default     = 2
  
  validation {
    condition     = var.glue_job_number_of_workers >= 2 && var.glue_job_number_of_workers <= 100
    error_message = "Number of workers must be between 2 and 100."
  }
}

# ============================================
# RDS PostgreSQL Variables
# ============================================

variable "db_instance_identifier" {
  description = "Identifier for the RDS instance"
  type        = string
  default     = "weather-postgres"
}

variable "db_username" {
  description = "Master username for PostgreSQL"
  type        = string
  default     = "postgres"
}

variable "db_password" {
  description = "Master password for PostgreSQL (store in AWS Secrets Manager!)"
  type        = string
  sensitive   = true
  
  validation {
    condition     = length(var.db_password) >= 8
    error_message = "Password must be at least 8 characters long."
  }
}

variable "db_instance_class" {
  description = "RDS instance class (db.t3.micro for free tier)"
  type        = string
  default     = "db.t3.micro"
}

variable "db_allocated_storage" {
  description = "Allocated storage for RDS in GB"
  type        = number
  default     = 20
  
  validation {
    condition     = var.db_allocated_storage >= 20 && var.db_allocated_storage <= 100
    error_message = "Storage must be between 20 and 100 GB."
  }
}

variable "db_engine_version" {
  description = "PostgreSQL engine version"
  type        = string
  default     = "16.6"
}

variable "db_multi_az" {
  description = "Enable Multi-AZ deployment for high availability"
  type        = bool
  default     = false
}

variable "db_backup_retention_period" {
  description = "Number of days to retain automated backups"
  type        = number
  default     = 7
  
  validation {
    condition     = var.db_backup_retention_period >= 0 && var.db_backup_retention_period <= 35
    error_message = "Backup retention must be between 0 and 35 days."
  }
}

# ============================================
# VPC & Networking Variables
# ============================================

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
  
  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "Must be a valid IPv4 CIDR block."
  }
}

variable "subnet1_cidr" {
  description = "CIDR block for subnet 1"
  type        = string
  default     = "10.0.1.0/24"
}

variable "subnet2_cidr" {
  description = "CIDR block for subnet 2"
  type        = string
  default     = "10.0.2.0/24"
}

variable "enable_dns_hostnames" {
  description = "Enable DNS hostnames in VPC"
  type        = bool
  default     = true
}

variable "enable_dns_support" {
  description = "Enable DNS support in VPC"
  type        = bool
  default     = true
}

# ============================================
# Resource Tags
# ============================================

variable "additional_tags" {
  description = "Additional tags to apply to all resources"
  type        = map(string)
  default     = {}
}

# ============================================
# Feature Flags
# ============================================

variable "enable_versioning" {
  description = "Enable versioning for S3 buckets"
  type        = bool
  default     = true
}

variable "enable_encryption" {
  description = "Enable encryption at rest for all resources"
  type        = bool
  default     = true
}

variable "enable_monitoring" {
  description = "Enable enhanced monitoring for RDS"
  type        = bool
  default     = true
}

variable "enable_deletion_protection" {
  description = "Enable deletion protection for RDS (recommended for prod)"
  type        = bool
  default     = false
}

variable "enable_auto_scaling" {
  description = "Enable auto-scaling for Glue DPU usage"
  type        = bool
  default     = false
}