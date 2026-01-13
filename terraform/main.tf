/*
terraform/main.tf - Infrastructure as Code

CONCEPTO FUNDAMENTAL: Infrastructure as Code (IaC)
==================================================
En lugar de crear infraestructura manualmente (clickear en AWS console),
la definimos como código.

Beneficios:
1. Versionado (Git)
2. Reproducible (mismo código = misma infra)
3. Auditable (quién cambió qué)
4. Testeable (terraform plan antes de aplicar)
5. Automatizable (CI/CD)

Para Databricks:
- Databricks Provider de Terraform
- Provisiona workspaces, clusters, jobs, notebooks
- Integra con cloud providers (AWS/Azure/GCP)
*/

# CONCEPTO: Terraform Configuration
# Define versión de Terraform y providers requeridos
terraform {
  required_version = ">= 1.0"
  
  required_providers {
    # AWS provider (para storage, compute)
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    
    # Databricks provider (si fuera a deployar en Databricks)
    # databricks = {
    #   source  = "databricks/databricks"
    #   version = "~> 1.0"
    # }
  }
  
  # BEST PRACTICE: Remote State
  # En producción, el state de Terraform se guarda en S3/GCS
  # No en local (se puede perder, no es compartido entre equipo)
  /*
  backend "s3" {
    bucket = "my-terraform-state"
    key    = "github-pipeline/terraform.tfstate"
    region = "us-east-1"
  }
  */
}

# CONCEPTO: Variables
# Parametrizar la configuración
variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  description = "Project name (used for resource naming)"
  type        = string
  default     = "github-events-pipeline"
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

# CONCEPTO: Local Values
# Computed values reutilizables
locals {
  # BEST PRACTICE: Consistent naming
  # Todas las resources siguen el mismo patrón
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
  
  # Resource naming pattern
  resource_prefix = "${var.project_name}-${var.environment}"
}

# CONCEPTO: AWS Provider Configuration
provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = local.common_tags
  }
}

# ========================================
# STORAGE: S3 Buckets para el pipeline
# ========================================

# CONCEPTO: S3 como Data Lake
# Medallion Architecture en S3:
# - bronze/: raw data
# - silver/: curated data
# - gold/: aggregated metrics
resource "aws_s3_bucket" "data_lake" {
  bucket = "${local.resource_prefix}-data-lake"
  
  # BEST PRACTICE: Force destroy en dev, proteger en prod
  force_destroy = var.environment == "dev" ? true : false
  
  tags = {
    Name = "Data Lake - ${var.environment}"
  }
}

# CONCEPTO: Bucket Versioning
# Delta Lake necesita versioning para time travel
resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# CONCEPTO: Lifecycle Rules
# Optimización de costos: mover datos viejos a storage más barato
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  # Raw data: mover a Glacier después de 90 días
  rule {
    id     = "archive-raw-data"
    status = "Enabled"
    
    filter {
      prefix = "bronze/"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    # Eliminar después de 365 días
    expiration {
      days = 365
    }
  }
  
  # Curated data: mantener más tiempo
  rule {
    id     = "archive-curated-data"
    status = "Enabled"
    
    filter {
      prefix = "silver/"
    }
    
    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
}

# CONCEPTO: Bucket Policies (Security)
# Solo el pipeline puede escribir, analysts pueden leer
resource "aws_s3_bucket_policy" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowPipelineWrite"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.pipeline_role.arn
        }
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/*",
          aws_s3_bucket.data_lake.arn
        ]
      }
    ]
  })
}

# ========================================
# IAM: Roles y Permissions
# ========================================

# CONCEPTO: IAM Role para el pipeline
# En producción, el pipeline corre con esta identidad
# (Lambda, ECS, EC2, etc.)
resource "aws_iam_role" "pipeline_role" {
  name = "${local.resource_prefix}-pipeline-role"
  
  # CONCEPTO: Assume Role Policy
  # Define quién puede asumir este rol
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          # En este ejemplo: EC2 instances
          # Para Databricks: arn:aws:iam::414351767826:role/...
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# CONCEPTO: IAM Policy (Permissions)
# Define QUÉ puede hacer el role
resource "aws_iam_role_policy" "pipeline_policy" {
  name = "${local.resource_prefix}-pipeline-policy"
  role = aws_iam_role.pipeline_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Read/Write to data lake
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/*",
          aws_s3_bucket.data_lake.arn
        ]
      },
      # CloudWatch Logs (para logging)
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# ========================================
# OBSERVABILITY: CloudWatch
# ========================================

# CONCEPTO: Log Group para el pipeline
# Todos los logs del pipeline van aquí
resource "aws_cloudwatch_log_group" "pipeline_logs" {
  name              = "/aws/${local.resource_prefix}"
  retention_in_days = var.environment == "prod" ? 90 : 7
  
  tags = {
    Name = "Pipeline Logs - ${var.environment}"
  }
}

# CONCEPTO: CloudWatch Alarms
# Monitoreo y alertas
resource "aws_cloudwatch_metric_alarm" "pipeline_errors" {
  alarm_name          = "${local.resource_prefix}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = var.project_name
  period              = 300
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Alert when pipeline has more than 5 errors in 5 minutes"
  
  # En producción: SNS topic para notificaciones
  # alarm_actions = [aws_sns_topic.alerts.arn]
}

# ========================================
# DATABRICKS (Ejemplo - commented)
# ========================================

# Si estuvieras usando Databricks, provisiona:
/*
# Databricks Workspace
resource "databricks_workspace" "this" {
  workspace_name = "${local.resource_prefix}-workspace"
  aws_region     = var.aws_region
  
  # Conecta con AWS account
  credentials_id = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
}

# Cluster para el pipeline
resource "databricks_cluster" "pipeline_cluster" {
  cluster_name  = "${local.resource_prefix}-cluster"
  spark_version = "13.3.x-scala2.12"  # LTS
  node_type_id  = "i3.xlarge"
  
  autoscale {
    min_workers = 1
    max_workers = 5  # Escala según carga
  }
  
  # Delta Lake pre-installed
  spark_conf = {
    "spark.databricks.delta.preview.enabled" = "true"
  }
}

# Job para ejecutar el pipeline
resource "databricks_job" "pipeline_job" {
  name = "${local.resource_prefix}-job"
  
  task {
    task_key = "ingest_and_transform"
    
    existing_cluster_id = databricks_cluster.pipeline_cluster.id
    
    notebook_task {
      notebook_path = "/Repos/main/github-pipeline/main"
    }
  }
  
  # Schedule: cada hora
  schedule {
    quartz_cron_expression = "0 0 * * * ?"
    timezone_id            = "UTC"
  }
  
  email_notifications {
    on_failure = ["data-eng@company.com"]
  }
}
*/

# ========================================
# OUTPUTS
# ========================================

# CONCEPTO: Terraform Outputs
# Valores que otros módulos o humanos necesitan
output "data_lake_bucket" {
  description = "S3 bucket for data lake"
  value       = aws_s3_bucket.data_lake.bucket
}

output "data_lake_arn" {
  description = "ARN of data lake bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "pipeline_role_arn" {
  description = "IAM role ARN for pipeline"
  value       = aws_iam_role.pipeline_role.arn
}

output "log_group_name" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.pipeline_logs.name
}

# CONCEPTO: Sensitive Outputs
# Algunos valores no deben mostrarse en logs
# output "api_key" {
#   value     = var.api_key
#   sensitive = true
# }