# Changelog

## 2024-12-19

### Major Infrastructure Migration
- **BREAKING CHANGE**: Migrated from AWS Lambda + API Gateway to ECS Fargate with Application Load Balancer
- **WebSocket Support**: Full WebSocket support now available through direct HTTP/HTTPS connections
- **Improved Performance**: Eliminated Lambda cold starts with persistent ECS containers
- **Auto Scaling**: Added CPU and memory-based auto scaling for ECS service (2-10 instances)
- **Load Balancing**: Application Load Balancer with HTTPS termination and HTTP redirect
- **Health Checks**: Comprehensive health monitoring at both ALB and ECS levels
- **Security**: Updated security groups for ALB and ECS with least-privilege access
- **Monitoring**: Maintained New Relic monitoring compatibility for ECS environment
- **Environment Variables**: All existing environment variables preserved in migration
- **SSL/TLS**: Maintained SSL certificate management with Route53 DNS validation

### Technical Details
- Docker image optimized for ECS with Python 3.11 slim base
- Added health checks and proper security user context
- ECS service running on Fargate with 1024 CPU and 2048 MB memory per task
- Application Load Balancer handling traffic distribution and SSL termination
- Maintained all existing integrations (Firebase, Postgres, Redis, Snowflake, etc.)

### Deployment Notes
- Legacy Lambda and API Gateway resources commented out for rollback capability
- New Route53 records point to ALB instead of API Gateway
- Same domain structure maintained for seamless client transition
