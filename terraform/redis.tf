resource "aws_elasticache_serverless_cache" "redis" {
  engine = "redis"
  major_engine_version = "7"
  name   = "${var.app_ident}-v2"
  cache_usage_limits {
    data_storage {
      maximum = 10
      unit    = "GB"
    }
    ecpu_per_second {
      maximum = 5000
    }
  }

  security_group_ids = [aws_security_group.redis_sg.id]
  subnet_ids         = var.PRIVATE_SUBNET_IDS
}

resource "aws_security_group" "redis_sg" {
  name   = "${var.app_ident}_redis_sg3"
  vpc_id = data.aws_vpc.selected.id

  ingress {
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "allow_lambda_to_redis" {
  type                     = "ingress"
  from_port                = 6379
  to_port                  = 6379
  protocol                 = "tcp"
  security_group_id        = aws_security_group.redis_sg.id
  source_security_group_id = aws_security_group.lambda_sg.id
}

resource "aws_security_group" "lambda_sg" {
  name   = "${var.app_ident}_lambda_sg3"
  vpc_id = data.aws_vpc.selected.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}
