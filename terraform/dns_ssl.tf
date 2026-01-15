variable "api_domain" {
  type = string
}

variable "api_root_domain" {
  type = string
}

# Route53 zone for the domain
data "aws_route53_zone" "api_domain" {
  name = "${var.api_root_domain}."
}

# Route53 record for ALB
resource "aws_route53_record" "app_alb" {
  zone_id = data.aws_route53_zone.api_domain.zone_id
  name    = var.api_domain
  type    = "A"

  alias {
    name                   = aws_lb.app.dns_name
    zone_id                = aws_lb.app.zone_id
    evaluate_target_health = true
  }
}

# ACM certificate for the API domain
resource "aws_acm_certificate" "api_cert" {
  domain_name       = var.api_domain
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }

  tags = {
    Name        = "${var.app_ident}-api-cert"
    Environment = var.environment
  }
}

# Certificate validation
resource "aws_acm_certificate_validation" "api_cert_validation" {
  certificate_arn         = aws_acm_certificate.api_cert.arn
  validation_record_fqdns = [for record in aws_route53_record.cert_validation : record.fqdn]
}

# DNS validation records
resource "aws_route53_record" "cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.api_cert.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      record = dvo.resource_record_value
      type   = dvo.resource_record_type
    }
  }

  allow_overwrite = true
  name            = each.value.name
  records         = [each.value.record]
  ttl             = 60
  type            = each.value.type
  zone_id         = data.aws_route53_zone.api_domain.zone_id
}

# Output the ALB endpoint
output "app_endpoint" {
  value = "https://${var.api_domain}"
  description = "Application endpoint URL"
}

# Output the ALB DNS name
output "alb_dns_name" {
  value = aws_lb.app.dns_name
  description = "Application Load Balancer DNS name"
}
