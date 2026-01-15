# New Relic Configuration
variable "NEW_RELIC_LICENSE_KEY" {
  type = string
}

variable "NEW_RELIC_ACCOUNT_ID" {
  type = string
}

variable "NEW_RELIC_ENABLE_FUNCTION_LOGS" {
  type = string
}

variable "NEW_RELIC_ENABLE_DISTRIBUTED_TRACING" {
  type = string
}

# Firebase Configuration
variable "FIREBASE_AWS_SECRET_NAME" {
  type = string
}

variable "FIREBASE_REALTIME_DATABASE_URL" {
  type = string
}

variable "DEFAULT_ADMIN_EMAIL" {
  type = string
}

# Snowflake Configuration
variable "SNOWFLAKE_ACCOUNT" {
  type = string
}

variable "SNOWFLAKE_WAREHOUSE" {
  type = string
}

variable "SNOWFLAKE_ROLE" {
  type = string
}

variable "SNOWFLAKE_USER" {
  type = string
}

variable "SNOWFLAKE_PASSWORD" {
  type = string
}

variable "SNOWFLAKE_DATABASE" {
  type = string
}

variable "SNOWFLAKE_SCHEMA" {
  type = string
}

# Azure/MSAL Configuration
variable "AZURE_TENANT_ID" {
  type = string
}

variable "AZURE_AD_APP_ID" {
  type = string
}

variable "AZURE_AD_APP_SECRET" {
  type = string
}

variable "AZURE_AUTHORITY_URL" {
  type = string
}

variable "AZURE_SCOPE_BASE" {
  type = string
}

# Database Configuration
variable "POSTGRES_URL" {
  type = string
}

variable "POSTGRES_READONLY_URL" {
  type = string
}

variable "POSTGRES_URL_BUYLIST" {
  type = string
}

variable "POSTGRES_URL_BUYLIST_READONLY" {
  type = string
}

variable "POSTGRES_REALTIME_CATALOG" {
  type = string
}

variable "POSTGRES_URL_OD" {
  type = string
}

variable "POSTGRES_READONLY_URL_OD" {
  type = string
}

# API Keys
variable "AMS_API_KEY" {
  type = string
}

variable "CORPAY_CLIENT_ID" {
  type = string
}

variable "CORPAY_CLIENT_SECRET" {
  type      = string
  sensitive = true
}

variable "OPENAI_API_KEY" {
  description = "Key for OpenAI API"
  type        = string
}

variable "VIAGOGO_API_TOKEN" {
  description = "Token for Viagogo API access"
  type        = string
  sensitive   = true
}

variable "TICKETSUITE_API_KEY" {
  default = ""
}

variable "MLX_IT_KEY" {
  type = string
}

variable "GLOBAL_REWARDS_AUTH_KEY_TB_MAIN" {
  type = string
}

variable "GLOBAL_REWARDS_AUTH_KEY_SHADOWS_MAIN" {
  type = string
}

variable "GLOBAL_REWARDS_AUTH_KEY_TB_INTERNATIONAL" {
  type = string
}

variable "GLOBAL_REWARDS_AUTH_KEY_SHADOWS_INTERNATIONAL" {
  type = string
}

# SQS Configuration
variable "SQS_CSV_QUEUE_URL" {
  type = string
}

variable "SQS_UPDATE_CART_STATUS_QUEUE_URL" {
  type = string
}

variable "VIAGOGO_DELETE_SQS_QUEUE" {
  type = string
}
variable "SEATGEEK_DELETE_SQS_QUEUE" {
  type = string
}

variable "BROWSER_CAPTURE_API_URL" {
  type = string
}

# Redis Configuration
variable "SHADOWS_REDIS_HOST" {
  default = ""
}

variable "SHADOWS_REDIS_PORT" {
  default = ""
}

# Other Services
variable "OPENSEARCH_ENDPOINT" {
  default = ""
}

variable "VAULTWARDEN_URL" {
  default = ""
}

variable "CC_ENCRYPTION_KEY_FOR_STORAGE" {
  default = ""
}

variable "CC_MASTER_ENCRYPTION_KEY" {
  default = ""
}

variable "TRADE_DESK_BROKER_KEY" {
  default = ""
}

variable "FORWARDER_FROM_EMAIL" {
  type        = string
  default     = "forwarder@tb-portal.com"
  description = "Email address to use as the sender when forwarding emails"
}

variable "AWS_ACCESS_KEY_ID" {
  type        = string
  description = "AWS Access Key ID for SES"
}

variable "AWS_SECRET_ACCESS_KEY" {
  type        = string
  description = "AWS Secret Access Key for SES"
  sensitive   = true
}

variable "AWS_REGION" {
  type        = string
  default     = "us-east-1"
  description = "AWS region for SES (defaults to us-east-1)"
}
