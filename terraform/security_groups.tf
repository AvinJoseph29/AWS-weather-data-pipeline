# ============================================
# Security Groups
# Act as virtual firewalls for resources
# ============================================

# ============================================
# Security Group for AWS Glue
# ============================================

resource "aws_security_group" "glue_sg" {
  name        = "${local.name_prefix}-glue-sg"
  description = "Security group for AWS Glue jobs"
  vpc_id      = aws_vpc.main.id
  
  # Ingress rule: Allow all traffic within the security group
  # This allows Glue workers to communicate with each other
  ingress {
    description = "Allow internal communication"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"  # All protocols
    self        = true   # From this security group
  }
  
  # Egress rule: Allow all outbound traffic
  # Glue needs to access S3, Kinesis, etc.
  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = {
    Name = "${local.name_prefix}-glue-sg"
  }
}

# ============================================
# Security Group for RDS PostgreSQL
# ============================================

resource "aws_security_group" "rds_sg" {
  name        = "${local.name_prefix}-rds-sg"
  description = "Security group for RDS PostgreSQL"
  vpc_id      = aws_vpc.main.id
  
  # Ingress rule: Allow PostgreSQL from Glue security group
  ingress {
    description     = "PostgreSQL from Glue"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.glue_sg.id]
  }
  
  # Ingress rule: Allow PostgreSQL from VPC CIDR
  # This allows resources within VPC to connect to RDS
  ingress {
    description = "PostgreSQL from VPC"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  
  # Egress rule: Allow all outbound traffic
  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = {
    Name = "${local.name_prefix}-rds-sg"
  }
}

# ============================================
# Security Group for Lambda (if needed)
# ============================================

resource "aws_security_group" "lambda_sg" {
  name        = "${local.name_prefix}-lambda-sg"
  description = "Security group for Lambda functions"
  vpc_id      = aws_vpc.main.id
  
  # Egress rule: Allow all outbound traffic
  # Lambda needs to access various AWS services
  egress {
    description = "Allow all outbound traffic"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = {
    Name = "${local.name_prefix}-lambda-sg"
  }
}

# ============================================
# Security Group Rules for RDS from Lambda
# ============================================

resource "aws_security_group_rule" "rds_from_lambda" {
  description              = "PostgreSQL from Lambda"
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.rds_sg.id
  source_security_group_id = aws_security_group.lambda_sg.id
}

# ============================================
# Outputs
# ============================================

output "glue_security_group_id" {
  description = "Glue security group ID"
  value       = aws_security_group.glue_sg.id
}

output "rds_security_group_id" {
  description = "RDS security group ID"
  value       = aws_security_group.rds_sg.id
}

output "lambda_security_group_id" {
  description = "Lambda security group ID"
  value       = aws_security_group.lambda_sg.id
}

# ============================================
# Explanation
# ============================================

# Security Group Architecture:
#
# ┌─────────────────────────────────────────┐
# │ Glue Security Group                     │
# │ - Allows internal communication         │
# │ - Allows all outbound (S3, Kinesis)    │
# │         │                                │
# │         ↓ Can connect to                │
# └─────────────────────────────────────────┘
#           │
#           ↓
# ┌─────────────────────────────────────────┐
# │ RDS Security Group                      │
# │ - Accepts PostgreSQL (5432) from:       │
# │   • Glue security group                 │
# │   • VPC CIDR                            │
# │   • Lambda security group               │
# │ - Allows all outbound                   │
# └─────────────────────────────────────────┘
#
# Security Best Practices:
# 1. Least Privilege: Only allow necessary ports
# 2. Source Restrictions: Use security groups, not 0.0.0.0/0
# 3. VPC Isolation: Keep databases in private subnets (production)
# 4. Monitoring: Enable VPC Flow Logs
# 5. Regular Audits: Review security group rules quarterly

# Port Reference:
# - PostgreSQL: 5432
# - MySQL: 3306
# - Redis: 6379
# - HTTP: 80
# - HTTPS: 443
# - SSH: 22 (avoid opening to 0.0.0.0/0!)

# Production Recommendations:
# 1. Move RDS to private subnet
# 2. Use bastion host for RDS access
# 3. Restrict outbound rules (whitelist specific IPs)
# 4. Enable AWS WAF for web-facing resources
# 5. Use AWS Shield for DDoS protection