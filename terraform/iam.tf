resource "aws_iam_role" "lambda_exec" {
  name = "${var.app_ident}_lambda_exec_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "lambda.amazonaws.com",
        },
        Effect = "Allow",
      },
    ],
  })
}

resource "aws_iam_role_policy_attachment" "lambda_exec_policy" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_policy" "cloudwatch_policy" {
  name   = "${var.app_ident}_policy"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "secretsmanager:GetSecretValue",
        "sqs:*"
      ],
      "Effect": "Allow",
      "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "cloudwatch_policy_attachment" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.cloudwatch_policy.arn
}

# Add the EC2 permissions to the IAM role policy
resource "aws_iam_policy" "lambda_vpc_permissions" {
  name   = "${var.app_ident}_lambda_vpc_permissions"
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:CreateNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterface",
        "ec2:AssignPrivateIpAddresses",
        "ec2:UnassignPrivateIpAddresses",
        "s3:*"
      ],
      "Resource": "*"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "lambda_vpc_permissions_attachment" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_vpc_permissions.arn
}


resource "aws_iam_policy" "dynamodb_access_policy" {
  name        = "${var.app_ident}_DynamoDBDomainTableAccessPolicy"
  description = "Allows access to the DynamoDB tables"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "dynamodb:*"
        ],
        Resource = "arn:aws:dynamodb:*:*:table/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_dynamodb_domain_policy" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.dynamodb_access_policy.arn
}


resource "aws_iam_policy" "opensearch_access_policy" {
  name        = "${var.app_ident}_OpenSearchAccessPolicy"
  description = "Allows access to the OpenSearch"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "es:ESHttpPost",
          "es:ESHttpPut",
          "es:ESHttpGet",
          "es:ESHttpDelete",
          "es:ESHttpHead"
        ],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_opensearch_access_policy" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.opensearch_access_policy.arn
}