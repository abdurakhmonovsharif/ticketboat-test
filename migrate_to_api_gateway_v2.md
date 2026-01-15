# Migration Guide: REST API to HTTP API (API Gateway v2)

## Overview
This guide helps you migrate from API Gateway REST API to HTTP API (API Gateway v2) to enable WebSocket support.

## Changes Made

### 1. Terraform Configuration
- **Removed**: `api_gateway.tf` (renamed to `api_gateway.tf.deprecated`)
- **Added**: `api_gateway_v2.tf` (new HTTP API configuration)
- **Removed**: `websocket_api_gateway.tf` (no longer needed)
- **Removed**: `websocket_handler.py` (no longer needed)

### 2. Key Benefits of HTTP API
- **WebSocket Support**: Native WebSocket support in the same API
- **Better Performance**: Lower latency and higher throughput
- **Simplified Configuration**: Less complex than REST API
- **Cost Effective**: Lower costs for API Gateway usage

## Deployment Steps

### Step 1: Deploy the New Configuration
```bash
# Apply the new API Gateway v2 configuration
terraform apply -target=aws_apigatewayv2_api.http_api
terraform apply -target=aws_apigatewayv2_stage.http_stage
terraform apply -target=aws_apigatewayv2_integration.http_integration
terraform apply -target=aws_apigatewayv2_route.http_route
terraform apply -target=aws_apigatewayv2_route.http_root_route
terraform apply -target=aws_lambda_permission.http_apigw
```

### Step 2: Update Domain Configuration
```bash
# Apply domain configuration
terraform apply -target=aws_apigatewayv2_domain_name.http_domain
terraform apply -target=aws_apigatewayv2_api_mapping.http_mapping
terraform apply -target=aws_route53_record.http_api_gateway
```

### Step 3: Test the New API
```bash
# Test HTTP endpoints
curl https://api-staging.ticketboat-admin.com/healthcheck

# Test WebSocket connection
# Use wss://api-staging.ticketboat-admin.com/onsale-chat/ws/{analysis_id}
```

### Step 4: Update Frontend Configuration
Update your frontend to use the new WebSocket URL:
```javascript
// Old (REST API - doesn't support WebSocket)
// const wsUrl = `wss://api-staging.ticketboat-admin.com/onsale-chat/ws/${analysisId}`;

// New (HTTP API - supports WebSocket)
const wsUrl = `wss://api-staging.ticketboat-admin.com/onsale-chat/ws/${analysisId}`;
```

### Step 5: Clean Up Old Resources
```bash
# After confirming everything works, remove old REST API
terraform destroy -target=aws_api_gateway_rest_api.api
terraform destroy -target=aws_api_gateway_deployment.deployment
terraform destroy -target=aws_api_gateway_stage.api_stage
```

## Important Notes

### WebSocket URLs
- **HTTP API**: Uses the same domain for both HTTP and WebSocket
- **WebSocket URL**: `wss://api-staging.ticketboat-admin.com/onsale-chat/ws/{analysis_id}`
- **HTTP URL**: `https://api-staging.ticketboat-admin.com/`

### CORS Configuration
The HTTP API includes CORS configuration that allows:
- All origins (`*`)
- All methods (`*`)
- All headers (`*`)
- Credentials: `false` (for WebSocket compatibility)

### Lambda Handler
Your existing Mangum handler will work with HTTP API:
```python
lambda_handler = Mangum(app, lifespan="off")
```

## Troubleshooting

### Common Issues

1. **WebSocket Connection Fails**
   - Check that the domain is properly configured
   - Verify the certificate is valid
   - Ensure the Lambda function has proper permissions

2. **CORS Errors**
   - The HTTP API includes CORS configuration
   - If issues persist, check your frontend CORS settings

3. **Domain Not Resolving**
   - Check Route53 records
   - Verify the domain mapping is correct
   - Ensure the certificate is issued and valid

### Rollback Plan
If issues occur, you can rollback by:
1. Reverting the Terraform changes
2. Restoring the old `api_gateway.tf` file
3. Running `terraform apply` to restore the REST API

## Verification

After deployment, verify:
1. ✅ HTTP endpoints work: `https://api-staging.ticketboat-admin.com/healthcheck`
2. ✅ WebSocket connections work: `wss://api-staging.ticketboat-admin.com/onsale-chat/ws/{analysis_id}`
3. ✅ Frontend can connect to WebSocket
4. ✅ Chat functionality works as expected
