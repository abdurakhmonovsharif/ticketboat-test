# OnSale Email Analysis Backend Implementation

## Overview

This document describes the backend implementation for the OnSale Email Analysis tool, which provides AI-powered analysis of ticket resale opportunities from email data stored in PostgreSQL.

## Architecture

The backend follows a clean, layered architecture:

```
┌─────────────────┐
│   FastAPI API   │  ← API endpoints with authentication
├─────────────────┤
│   Pydantic      │  ← Data validation and serialization
│   Models        │
├─────────────────┤
│   Database      │  ← PostgreSQL queries and data access
│   Layer         │
├─────────────────┤
│   PostgreSQL    │  ← onsale_email_analysis table
│   Database      │
└─────────────────┘
```

## Components

### 1. Data Models (`src/app/model/onsale_email_analysis.py`)

**Pydantic models for type safety and validation:**

- `OnsaleEmailAnalysisItem`: Individual analysis record
- `OnsaleEmailAnalysisResponse`: Paginated response wrapper
- `OnsaleEmailAnalysisSummary`: Summary statistics
- `FilterOptionsResponse`: Filter dropdown options

### 2. Database Layer (`src/app/db/onsale_email_analysis_db.py`)

**Efficient PostgreSQL queries with the following functions:**

#### Core Functions:
- `get_onsale_email_analyses()`: Main paginated query with comprehensive filtering
- `get_onsale_email_analysis_summary()`: Aggregated statistics and insights
- `get_onsale_email_analysis_venues()`: Unique venues for filters
- `get_onsale_email_analysis_performers()`: Unique performers for filters
- `get_onsale_email_analysis_event_types()`: Unique event types for filters

#### Key Features:
- **Parameterized Queries**: SQL injection prevention
- **Dynamic Filtering**: Build WHERE clauses based on provided parameters
- **Efficient Pagination**: LIMIT/OFFSET with proper count queries
- **Array Handling**: Proper handling of PostgreSQL arrays (risk_factors, opportunities)
- **Null Safety**: Robust handling of NULL values and empty results

### 3. API Layer (`src/app/api/onsale_email_analysis_api.py`)

**FastAPI endpoints with authentication:**

#### Endpoints:
- `GET /reports/emails/onsale-analysis` - Main data endpoint with pagination
- `GET /reports/emails/onsale-analysis/summary` - Summary statistics
- `GET /reports/emails/onsale-analysis/venues` - Venue filter options
- `GET /reports/emails/onsale-analysis/performers` - Performer filter options
- `GET /reports/emails/onsale-analysis/event-types` - Event type filter options

#### Features:
- **Authentication**: Firebase token-based authentication
- **Role-based Access**: Requires "user" role
- **Comprehensive Filtering**: All filter parameters supported
- **Error Handling**: Proper HTTP status codes and error messages
- **Input Validation**: Pydantic model validation
- **Documentation**: OpenAPI/Swagger documentation

## Database Schema

The backend works with the `onsale_email_analysis` table:

```sql
CREATE TABLE onsale_email_analysis (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    
    -- Email metadata
    email_id TEXT NOT NULL,
    email_subject TEXT NOT NULL,
    email_from TEXT NOT NULL,
    email_to TEXT NOT NULL,
    email_ts TIMESTAMP WITH TIME ZONE,
    analysis_generated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Event details
    event_name TEXT NOT NULL,
    venue_name TEXT,
    venue_location TEXT,
    performer TEXT,
    event_type TEXT,
    
    -- Analysis scores and metrics
    opportunity_score DECIMAL(5,2) NOT NULL,
    confidence_percentage DECIMAL(5,2),
    target_margin_percentage DECIMAL(5,2),
    
    -- Risk and opportunity factors
    risk_factors TEXT[],
    opportunities TEXT[],
    
    -- Detailed analysis content
    reasoning_summary TEXT,
    historical_context TEXT,
    buying_guidance TEXT,
    risk_management TEXT,
    next_steps TEXT,
    
    -- Market analysis
    market_volatility_level TEXT,
    demand_uncertainty_level TEXT,
    competition_level TEXT,
    
    -- Financial projections
    recommended_buy_amount_min INTEGER,
    recommended_buy_amount_max INTEGER,
    target_resale_markup_percentage DECIMAL(5,2),
    stop_loss_percentage DECIMAL(5,2),
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
```

## Query Optimization

### Indexes
The implementation assumes the following indexes exist for optimal performance:

```sql
CREATE INDEX idx_onsale_email_analysis_email_id ON onsale_email_analysis(email_id);
CREATE INDEX idx_onsale_email_analysis_event_name ON onsale_email_analysis(event_name);
CREATE INDEX idx_onsale_email_analysis_venue_name ON onsale_email_analysis(venue_name);
CREATE INDEX idx_onsale_email_analysis_performer ON onsale_email_analysis(performer);
CREATE INDEX idx_onsale_email_analysis_opportunity_score ON onsale_email_analysis(opportunity_score);
CREATE INDEX idx_onsale_email_analysis_analysis_generated_at ON onsale_email_analysis(analysis_generated_at);
```

### Query Patterns
- **Efficient Filtering**: WHERE clauses built dynamically based on provided parameters
- **Pagination**: Proper LIMIT/OFFSET with separate count queries
- **Aggregation**: Efficient GROUP BY queries for summary statistics
- **Array Operations**: Proper handling of PostgreSQL TEXT[] arrays

## Performance Features

### 1. Connection Pooling
- Uses the existing PostgreSQL connection pool from `app.database`
- Efficient connection reuse for multiple queries

### 2. Query Optimization
- **Parameterized Queries**: Prevents SQL injection and enables query plan caching
- **Selective Columns**: Only fetches required columns
- **Efficient Joins**: Minimal joins, mostly single-table queries
- **Proper Indexing**: Queries designed to use existing indexes

### 3. Caching Strategy
- **Application-level**: React Query caching on frontend
- **Database-level**: PostgreSQL query plan caching
- **Connection-level**: Connection pooling for reduced overhead

### 4. Pagination
- **Efficient Pagination**: LIMIT/OFFSET with proper count queries
- **Configurable Page Size**: Default 20, configurable up to reasonable limits
- **Total Count**: Always returns total count for UI pagination

## Error Handling

### Database Errors
- **Connection Errors**: Proper handling of database connection failures
- **Query Errors**: Graceful handling of SQL errors with meaningful messages
- **Data Type Errors**: Proper handling of NULL values and type conversions

### API Errors
- **Authentication Errors**: Proper 401 responses for invalid tokens
- **Validation Errors**: 400 responses for invalid input parameters
- **Server Errors**: 500 responses with detailed error messages for debugging

### Logging
- **Structured Logging**: Consistent logging format across all functions
- **Error Tracking**: Full stack traces for debugging
- **Performance Monitoring**: Query execution time tracking

## Security

### Authentication
- **Firebase Authentication**: Token-based authentication
- **Role-based Access**: Requires "user" role for all endpoints
- **Token Validation**: Proper JWT token validation

### Input Validation
- **Pydantic Models**: Automatic input validation and sanitization
- **SQL Injection Prevention**: Parameterized queries only
- **Type Safety**: Strong typing throughout the codebase

### Data Protection
- **No Sensitive Data**: Only analysis data, no personal information
- **Read-only Operations**: All endpoints are read-only
- **Audit Trail**: Created/updated timestamps for all records

## Testing

### Test Script
Run the test script to verify backend functionality:

```bash
cd ticketboat-admin-api-python
python test_onsale_email_analysis.py
```

### Test Coverage
- **Database Functions**: Direct testing of all database functions
- **Filter Functionality**: Testing with various filter combinations
- **Error Scenarios**: Testing error handling and edge cases
- **Performance**: Basic performance testing with sample data

## Integration

### Frontend Integration
The backend is designed to work seamlessly with the React frontend:

- **API Endpoints**: Match frontend API client expectations
- **Data Format**: Consistent JSON response format
- **Error Handling**: Compatible error response format
- **Authentication**: Same Firebase authentication system

### Database Integration
- **PostgreSQL**: Uses existing PostgreSQL connection pool
- **Migration**: Works with existing database migration system
- **Environment**: Compatible with existing environment configuration

## Deployment

### Environment Variables
Ensure these environment variables are set:

```bash
# Database
POSTGRES_URL=postgresql://user:password@host:port/database

# Authentication
FIREBASE_PROJECT_ID=your-project-id
FIREBASE_PRIVATE_KEY_ID=your-private-key-id
FIREBASE_PRIVATE_KEY=your-private-key
FIREBASE_CLIENT_EMAIL=your-client-email
FIREBASE_CLIENT_ID=your-client-id
```

### Dependencies
The implementation uses existing dependencies:
- `fastapi`: Web framework
- `asyncpg`: PostgreSQL async driver
- `pydantic`: Data validation
- `firebase-admin`: Authentication

## Monitoring and Maintenance

### Performance Monitoring
- **Query Performance**: Monitor slow queries and optimize indexes
- **Connection Pool**: Monitor connection pool usage
- **Response Times**: Track API response times

### Data Maintenance
- **Data Cleanup**: Regular cleanup of old analysis data
- **Index Maintenance**: Regular index rebuilding and statistics updates
- **Backup**: Regular database backups

### Logging and Debugging
- **Structured Logs**: Consistent logging format for easy parsing
- **Error Tracking**: Full error context for debugging
- **Performance Metrics**: Query execution time tracking

## Future Enhancements

### Potential Improvements
- **Caching Layer**: Redis caching for frequently accessed data
- **Real-time Updates**: WebSocket support for real-time data updates
- **Advanced Analytics**: More sophisticated statistical analysis
- **Export Functionality**: CSV/Excel export capabilities
- **Bulk Operations**: Batch processing for large datasets

### Scalability Considerations
- **Database Sharding**: Horizontal partitioning for large datasets
- **Read Replicas**: Separate read replicas for better performance
- **CDN Integration**: Static asset caching
- **Load Balancing**: Multiple API instances behind load balancer

## Conclusion

The OnSale Email Analysis backend provides a robust, scalable, and secure foundation for the frontend tool. It efficiently queries the PostgreSQL database, provides comprehensive filtering capabilities, and delivers rich analytics data to help users understand ticket resale opportunities.

The implementation follows best practices for:
- **Performance**: Efficient queries and proper indexing
- **Security**: Authentication, validation, and SQL injection prevention
- **Maintainability**: Clean code structure and comprehensive documentation
- **Scalability**: Connection pooling and efficient resource usage
