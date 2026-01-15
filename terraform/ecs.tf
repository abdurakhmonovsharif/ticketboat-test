# ECS Configuration
locals {
  # Service scaling configuration
  min_capacity          = var.environment == "prod" ? 4 : 1    # Minimum number of running tasks
  max_capacity          = var.environment == "prod" ? 8 : 1    # Maximum number of running tasks
  desired_count         = var.environment == "prod" ? 4 : 1    # Initial desired number of tasks
  
  # Task configuration
  task_cpu              = var.environment == "prod" ? 4096 : 1024 # CPU units (1024 = 1 vCPU)
  task_memory           = var.environment == "prod" ? 8192 : 2048 # Memory in MB
  container_port        = 8080 # Application port
  
  # Auto scaling thresholds
  cpu_target_value      = 50   # Target CPU utilization percentage
  memory_target_value   = 50   # Target memory utilization percentage
}

# ECS Cluster
resource "aws_ecs_cluster" "main" {
  name = "${var.app_ident}-cluster"

  setting {
    name  = "containerInsights"
    value = "enhanced"
  }

  tags = {
    Name        = "${var.app_ident}-cluster"
    Environment = var.environment
  }
}

# ECS Task Definition
resource "aws_ecs_task_definition" "app" {
  family                   = "${var.app_ident}-task"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = local.task_cpu
  memory                   = local.task_memory
  execution_role_arn       = aws_iam_role.ecs_execution_role.arn
  task_role_arn           = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([
    {
      name  = "${var.app_ident}-container"
      image = "${aws_ecr_repository.ecr_repository.repository_url}:${null_resource.push_image.triggers.code_hash}"
      
      portMappings = [
        {
          containerPort = local.container_port
          protocol      = "tcp"
        }
      ]

      environment = [
        {
          name  = "ENVIRONMENT"
          value = var.environment
        },
        {
          name  = "FIREBASE_AWS_SECRET_NAME"
          value = var.FIREBASE_AWS_SECRET_NAME
        },
        {
          name  = "FIREBASE_REALTIME_DATABASE_URL"
          value = var.FIREBASE_REALTIME_DATABASE_URL
        },
        {
          name  = "DEFAULT_ADMIN_EMAIL"
          value = var.DEFAULT_ADMIN_EMAIL
        },
        {
          name  = "REDIS_ADDRESS"
          value = aws_elasticache_serverless_cache.redis.endpoint.0.address
        },
        {
          name  = "REDIS_PORT"
          value = tostring(aws_elasticache_serverless_cache.redis.endpoint.0.port)
        },
        {
          name  = "SHADOWS_REDIS_ADDRESS"
          value = var.SHADOWS_REDIS_HOST
        },
        {
          name  = "SHADOWS_REDIS_PORT"
          value = var.SHADOWS_REDIS_PORT
        },
        {
          name  = "SNOWFLAKE_ACCOUNT"
          value = var.SNOWFLAKE_ACCOUNT
        },
        {
          name  = "SNOWFLAKE_WAREHOUSE"
          value = var.SNOWFLAKE_WAREHOUSE
        },
        {
          name  = "SNOWFLAKE_ROLE"
          value = var.SNOWFLAKE_ROLE
        },
        {
          name  = "SNOWFLAKE_USER"
          value = var.SNOWFLAKE_USER
        },
        {
          name  = "SNOWFLAKE_PASSWORD"
          value = var.SNOWFLAKE_PASSWORD
        },
        {
          name  = "SNOWFLAKE_DATABASE"
          value = var.SNOWFLAKE_DATABASE
        },
        {
          name  = "SNOWFLAKE_SCHEMA"
          value = var.SNOWFLAKE_SCHEMA
        },
        {
          name  = "AZURE_TENANT_ID"
          value = var.AZURE_TENANT_ID
        },
        {
          name  = "AZURE_AD_APP_ID"
          value = var.AZURE_AD_APP_ID
        },
        {
          name  = "AZURE_AD_APP_SECRET"
          value = var.AZURE_AD_APP_SECRET
        },
        {
          name  = "AZURE_AUTHORITY_URL"
          value = var.AZURE_AUTHORITY_URL
        },
        {
          name  = "AZURE_SCOPE_BASE"
          value = var.AZURE_SCOPE_BASE
        },
        {
          name  = "POSTGRES_URL"
          value = var.POSTGRES_URL
        },
        {
          name  = "AMS_API_KEY"
          value = var.AMS_API_KEY
        },
        {
          name  = "POSTGRES_READONLY_URL"
          value = var.POSTGRES_READONLY_URL
        },
        {
          name  = "POSTGRES_URL_BUYLIST"
          value = var.POSTGRES_URL_BUYLIST
        },
        {
          name  = "POSTGRES_URL_BUYLIST_READONLY"
          value = var.POSTGRES_URL_BUYLIST_READONLY
        },
        {
          name  = "POSTGRES_URL_OD"
          value = var.POSTGRES_URL_OD
        },
        {
          name  = "POSTGRES_READONLY_URL_OD"
          value = var.POSTGRES_READONLY_URL_OD
        },
        {
          name  = "SQS_CSV_QUEUE_URL"
          value = var.SQS_CSV_QUEUE_URL
        },
        {
          name  = "SQS_UPDATE_CART_STATUS_QUEUE_URL"
          value = var.SQS_UPDATE_CART_STATUS_QUEUE_URL
        },
        {
          name  = "VIAGOGO_DELETE_SQS_QUEUE"
          value = var.VIAGOGO_DELETE_SQS_QUEUE
        },
        {
          name  = "VIAGOGO_API_TOKEN"
          value = var.VIAGOGO_API_TOKEN
        },
        {
          name  = "SEATGEEK_DELETE_SQS_QUEUE"
          value = var.SEATGEEK_DELETE_SQS_QUEUE
        },
        {
          name  = "BROWSER_CAPTURE_API_URL"
          value = var.BROWSER_CAPTURE_API_URL
        },
        {
          name  = "NEW_RELIC_APP_NAME"
          value = var.api_domain
        },
        {
          name  = "NEW_RELIC_LICENSE_KEY"
          value = var.NEW_RELIC_LICENSE_KEY
        },
        {
          name  = "NEW_RELIC_ACCOUNT_ID"
          value = var.NEW_RELIC_ACCOUNT_ID
        },
        {
          name  = "NEW_RELIC_ENABLE_FUNCTION_LOGS"
          value = var.NEW_RELIC_ENABLE_FUNCTION_LOGS
        },
        {
          name  = "NEW_RELIC_ENABLE_DISTRIBUTED_TRACING"
          value = var.NEW_RELIC_ENABLE_DISTRIBUTED_TRACING
        },
        {
          name  = "OPENAI_API_KEY"
          value = var.OPENAI_API_KEY
        },
        {
          name  = "POSTGRES_REALTIME_CATALOG"
          value = var.POSTGRES_REALTIME_CATALOG
        },
        {
          name  = "OPENSEARCH_ENDPOINT"
          value = var.OPENSEARCH_ENDPOINT
        },
        {
          name  = "VAULTWARDEN_URL"
          value = var.VAULTWARDEN_URL
        },
        {
          name  = "CC_ENCRYPTION_KEY_FOR_STORAGE"
          value = var.CC_ENCRYPTION_KEY_FOR_STORAGE
        },
        {
          name  = "CC_MASTER_ENCRYPTION_KEY"
          value = var.CC_MASTER_ENCRYPTION_KEY
        },
        {
          name  = "TRADE_DESK_BROKER_KEY"
          value = var.TRADE_DESK_BROKER_KEY
        },
        {
          name  = "MARKETPLACE_SYNC_MANAGER_QUEUE_URL"
          value = data.terraform_remote_state.marketplace_sync_manager.outputs.init_sync_event_queue_url
        },
        {
          name  = "CIRQUE_LISTING_MONITOR_QUEUE_URL"
          value = data.terraform_remote_state.cirque_listing_monitor.outputs.sqs_queue_url
        },
        {
          name  = "TICKETSUITE_API_KEY"
          value = var.TICKETSUITE_API_KEY
        },
        {
          name  = "MLX_IT_KEY"
          value = var.MLX_IT_KEY
        },
        {
          name  = "GLOBAL_REWARDS_AUTH_KEY_TB_MAIN"
          value = var.GLOBAL_REWARDS_AUTH_KEY_TB_MAIN
        },
        {
          name  = "GLOBAL_REWARDS_AUTH_KEY_SHADOWS_MAIN"
          value = var.GLOBAL_REWARDS_AUTH_KEY_SHADOWS_MAIN
        },
        {
          name  = "GLOBAL_REWARDS_AUTH_KEY_TB_INTERNATIONAL"
          value = var.GLOBAL_REWARDS_AUTH_KEY_TB_INTERNATIONAL
        },
        {
          name  = "GLOBAL_REWARDS_AUTH_KEY_SHADOWS_INTERNATIONAL"
          value = var.GLOBAL_REWARDS_AUTH_KEY_SHADOWS_INTERNATIONAL
        },
        {
          name  = "CORPAY_CLIENT_ID"
          value = var.CORPAY_CLIENT_ID
        },
        {
          name  = "CORPAY_CLIENT_SECRET"
          value = var.CORPAY_CLIENT_SECRET
        },
        {
          name  = "FORWARDER_FROM_EMAIL"
          value = var.FORWARDER_FROM_EMAIL
        },
        {
          name  = "AWS_ACCESS_KEY_ID"
          value = var.AWS_ACCESS_KEY_ID
        },
        {
          name  = "AWS_SECRET_ACCESS_KEY"
          value = var.AWS_SECRET_ACCESS_KEY
        },
        {
          name  = "AWS_REGION"
          value = var.AWS_REGION
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ecs_logs.name
          awslogs-region        = data.aws_region.current.name
          awslogs-stream-prefix = "ecs"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "curl -f http://localhost:${local.container_port}/healthcheck || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }

      essential = true
    }
  ])

  tags = {
    Name        = "${var.app_ident}-task"
    Environment = var.environment
  }
}

# ECS Service
resource "aws_ecs_service" "app" {
  name            = "${var.app_ident}-service"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.app.arn
  desired_count   = local.desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.PRIVATE_SUBNET_IDS
    security_groups  = [aws_security_group.ecs_sg.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.app.arn
    container_name   = "${var.app_ident}-container"
    container_port   = local.container_port
  }

  depends_on = [aws_lb_listener.app]

  tags = {
    Name        = "${var.app_ident}-service"
    Environment = var.environment
  }
}

# CloudWatch Log Group for ECS
resource "aws_cloudwatch_log_group" "ecs_logs" {
  name              = "/ecs/${var.app_ident}"
  retention_in_days = 7

  tags = {
    Name        = "${var.app_ident}-ecs-logs"
    Environment = var.environment
  }
}

# Auto Scaling Target
resource "aws_appautoscaling_target" "ecs_target" {
  max_capacity       = local.max_capacity
  min_capacity       = local.min_capacity
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.app.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

# Auto Scaling Policy - CPU
resource "aws_appautoscaling_policy" "ecs_cpu_policy" {
  name               = "${var.app_ident}-cpu-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.ecs_target.resource_id
  scalable_dimension = aws_appautoscaling_target.ecs_target.scalable_dimension
  service_namespace  = aws_appautoscaling_target.ecs_target.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value = local.cpu_target_value
  }
}

# Auto Scaling Policy - Memory
resource "aws_appautoscaling_policy" "ecs_memory_policy" {
  name               = "${var.app_ident}-memory-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.ecs_target.resource_id
  scalable_dimension = aws_appautoscaling_target.ecs_target.scalable_dimension
  service_namespace  = aws_appautoscaling_target.ecs_target.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageMemoryUtilization"
    }
    target_value = local.memory_target_value
  }
}
