# ============================================
# VPC (Virtual Private Cloud)
# Isolated network for all resources
# ============================================

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = var.enable_dns_support
  enable_dns_hostnames = var.enable_dns_hostnames
  
  tags = {
    Name = "${local.name_prefix}-vpc"
  }
}

# ============================================
# Internet Gateway
# Allows VPC resources to access internet
# ============================================

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
  
  tags = {
    Name = "${local.name_prefix}-igw"
  }
}

# ============================================
# Subnets
# Divide VPC into smaller networks
# ============================================

# Subnet 1 in first availability zone
resource "aws_subnet" "subnet1" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.subnet1_cidr
  availability_zone       = local.azs[0]
  map_public_ip_on_launch = true
  
  tags = {
    Name = "${local.name_prefix}-subnet-1"
    AZ   = local.azs[0]
  }
}

# Subnet 2 in second availability zone
resource "aws_subnet" "subnet2" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.subnet2_cidr
  availability_zone       = local.azs[1]
  map_public_ip_on_launch = true
  
  tags = {
    Name = "${local.name_prefix}-subnet-2"
    AZ   = local.azs[1]
  }
}

# ============================================
# RDS Subnet Group
# Groups subnets for RDS Multi-AZ deployment
# ============================================

resource "aws_db_subnet_group" "rds_subnet_group" {
  name        = "${local.name_prefix}-rds-subnet-group"
  description = "Subnet group for RDS PostgreSQL"
  subnet_ids  = [aws_subnet.subnet1.id, aws_subnet.subnet2.id]
  
  tags = {
    Name = "${local.name_prefix}-rds-subnet-group"
  }
}

# ============================================
# Route Table
# Defines how traffic is routed
# ============================================

resource "aws_route_table" "public_rt" {
  vpc_id = aws_vpc.main.id
   
  # Route all traffic (0.0.0.0/0) to Internet Gateway
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
  
  tags = {
    Name = "${local.name_prefix}-public-rt"
  }
}

# ============================================
# Route Table Associations
# Connect subnets to route table
# ============================================

resource "aws_route_table_association" "public_rt_assoc_1" {
  subnet_id      = aws_subnet.subnet1.id
  route_table_id = aws_route_table.public_rt.id
}

resource "aws_route_table_association" "public_rt_assoc_2" {
  subnet_id      = aws_subnet.subnet2.id
  route_table_id = aws_route_table.public_rt.id
}

# ============================================
# Network ACL (Optional - Using default)
# Additional layer of security
# ============================================

# Note: We're using the default Network ACL which allows all traffic
# For production, consider creating custom NACLs with specific rules

# ============================================
# VPC Flow Logs (Optional but recommended)
# ============================================

# Uncomment to enable VPC Flow Logs for network monitoring
# resource "aws_flow_log" "main" {
#   iam_role_arn    = aws_iam_role.flow_log_role.arn
#   log_destination = aws_cloudwatch_log_group.flow_log.arn
#   traffic_type    = "ALL"
#   vpc_id          = aws_vpc.main.id
#
#   tags = {
#     Name = "${local.name_prefix}-vpc-flow-log"
#   }
# }

# ============================================
# Outputs
# ============================================

output "vpc_cidr" {
  description = "VPC CIDR block"
  value       = aws_vpc.main.cidr_block
}

output "subnet1_id" {
  description = "Subnet 1 ID"
  value       = aws_subnet.subnet1.id
}

output "subnet2_id" {
  description = "Subnet 2 ID"
  value       = aws_subnet.subnet2.id
}

output "internet_gateway_id" {
  description = "Internet Gateway ID"
  value       = aws_internet_gateway.igw.id
}

output "route_table_id" {
  description = "Public route table ID"
  value       = aws_route_table.public_rt.id
}

output "rds_subnet_group_name" {
  description = "RDS subnet group name"
  value       = aws_db_subnet_group.rds_subnet_group.name
}

# ============================================
# Explanation
# ============================================

# VPC Architecture:
#
# ┌─────────────────────────────────────────────┐
# │ VPC (10.0.0.0/16)                          │
# │                                             │
# │  ┌──────────────────┐  ┌──────────────────┐│
# │  │ Subnet 1         │  │ Subnet 2         ││
# │  │ (10.0.1.0/24)    │  │ (10.0.2.0/24)    ││
# │  │ AZ: eu-north-1a  │  │ AZ: eu-north-1b  ││
# │  │                  │  │                  ││
# │  │ - RDS Primary    │  │ - RDS Standby    ││
# │  │ - Glue Jobs      │  │ - Failover       ││
# │  └──────────────────┘  └──────────────────┘│
# │          ↕                      ↕           │
# │      Route Table                            │
# │          ↕                                  │
# │   Internet Gateway                          │
# └─────────────────────────────────────────────┘
#              ↕
#         Internet
#
# Why this design:
# - Two subnets in different AZs for high availability
# - Public subnets with IGW for internet access
# - RDS can be in either subnet (Multi-AZ if enabled)
# - Simple design, easy to understand and maintain