# ============================================
# Main Terraform Configuration
# Weather Data Pipeline Infrastructure
# ============================================

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  # Optional: Store state in S3 (for team collaboration)
  # Uncomment when you have AWS and create a state bucket
  # backend "s3" {
  #   bucket         = "weather-pipeline-terraform-state"
  #   key            = "terraform.tfstate"
  #   region         = "eu-north-1"
  #   encrypt        = true
  #   dynamodb_table = "terraform-state-lock"
  # }
}

# ============================================
# AWS Provider Configuration
# ============================================
provider "aws" {
  region = var.aws_region
  
  # Tags applied to all resources
  default_tags {
    tags = {
      Project     = "WeatherDataPipeline"
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = "DataEngineer"
      CreatedDate = timestamp()
    }
  }
}

# ============================================
# Data Sources
# ============================================

# Get current AWS account ID
data "aws_caller_identity" "current" {}

# Get current AWS region
data "aws_region" "current" {}

# Get available availability zones
data "aws_availability_zones" "available" {
  state = "available"
}

# ============================================
# Local Variables
# ============================================
locals {
  # Common naming prefix
  name_prefix = "${var.project_name}-${var.environment}"
  
  # AWS Account ID
  account_id = data.aws_caller_identity.current.account_id
  
  # Region
  region = data.aws_region.current.name
  
  # Availability zones
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
  
  # Common tags
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# ============================================
# Outputs for Reference
# ============================================
output "account_id" {
  description = "AWS Account ID"
  value       = local.account_id
}

output "region" {
  description = "AWS Region"
  value       = local.region
}

output "availability_zones" {
  description = "Availability Zones in use"
  value       = local.azs
}

output "vpc_id" {
  description = "VPC ID"
  value       = aws_vpc.main.id
}

output "subnet_ids" {
  description = "Subnet IDs"
  value       = [aws_subnet.subnet1.id, aws_subnet.subnet2.id]
}

output "s3_bucket_names" {
  description = "Created S3 bucket names"
  value = {
    raw_data     = aws_s3_bucket.raw_data_bucket.id
    processed    = aws_s3_bucket.processed_data_bucket.id
    alerts       = aws_s3_bucket.landed_flight_bucket.id
    glue_scripts = aws_s3_bucket.glue_script_bucket.id
    logs         = aws_s3_bucket.log_bucket.id
  }
}

output "kinesis_stream_name" {
  description = "Kinesis stream name"
  value       = aws_kinesis_stream.flight_stream.name
}

output "kinesis_stream_arn" {
  description = "Kinesis stream ARN"
  value       = aws_kinesis_stream.flight_stream.arn
}

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = aws_db_instance.postgres_instance.endpoint
  sensitive   = true
}

output "glue_job_name" {
  description = "Glue ETL job name"
  value       = aws_glue_job.flight_transform.name
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.flight_db.name
}

# ============================================
# Command Examples (commented)
# ============================================

# Initialize Terraform (run first time):
# terraform init

# Validate configuration:
# terraform validate

# Plan changes (preview what will be created):
# terraform plan

# Apply changes (create resources):
# terraform apply

# View outputs:
# terraform output

# Destroy everything (cleanup):
# terraform destroy

# Format code:
# terraform fmt -recursive