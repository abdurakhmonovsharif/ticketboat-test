terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.49"
    }
  }
}

variable "aws_region" {
  type = string
}

provider "aws" {
  region  = var.aws_region
}

provider "aws" {
  alias  = "useast1"
  region = "us-east-1"
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# Remote state for marketplace-sync-manager to get SQS queue URL
data "terraform_remote_state" "marketplace_sync_manager" {
  backend = "s3"
  config = {
    bucket = "ticketboat-terraform-state"
    key    = "terraform-marketplace-sync-manager-${var.environment}.tfstate"
    region = var.aws_region
  }
}

# Remote state for cirque-listing-monitor to get SQS queue URL
data "terraform_remote_state" "cirque_listing_monitor" {
  backend = "s3"
  config = {
    bucket = "ticketboat-terraform-state"
    key    = "terraform-cirque-listing-monitor-${var.environment}.tfstate"
    region = var.aws_region
  }
}

# NOTE: You must choose a vpc that has a NAT gateway so that the lambda function can talk to the internet
data "aws_vpc" "selected" {
  filter {
    name   = "tag:Name"
    values = [var.VPC_NAME]
  }
}

variable "VPC_NAME" {
  type = string
}

variable "PRIVATE_SUBNET_IDS" {
  type = list(string)
}

variable "PUBLIC_SUBNET_IDS" {
  type = list(string)
}

variable "app_ident" {
  description = "Identifier of the application"
  type        = string
}

variable "environment" {
  type        = string
}

variable "current_timestamp" {
  type = string
}
