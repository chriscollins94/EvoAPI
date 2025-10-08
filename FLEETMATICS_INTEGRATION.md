# Fleetmatics Integration - Usage Guide

## Overview

The EvoAPI Fleetmatics integration provides automated synchronization of vehicle assignments from the Fleetmatics API to the EvoAPI user database. The integration includes both manual and automated sync capabilities.

## Authentication Flow

### Token-Based Authentication
1. **Initial Authentication**: Use Basic Authentication (username:password encoded in base64) with `GET /auth/v1/token`
2. **Response**: Receive JWT access token valid for 20 minutes
3. **API Calls**: Use Bearer token for all subsequent API requests
4. **Token Caching**: Tokens cached with 2-minute buffer before expiration
5. **Auto-Refresh**: Service automatically requests new tokens when needed

## Architecture

### Components
- **FleetmaticsService**: Core service for API interactions and token management
- **FleetmaticsSyncService**: Background service for daily automated synchronization
- **FleetmaticsController**: REST API endpoints for manual operations and monitoring

### Background Service
The `FleetmaticsSyncService` runs as a hosted background service and automatically synchronizes vehicle assignments daily at the configured time (default: 2:00 AM).

## Configuration

### Environment Variables (Azure)
Set these environment variables in your Azure App Service:

```
FLEETMATICS_BASE_URL=https://api.fleetmatics.com/v1
FLEETMATICS_USERNAME=your_fleetmatics_username
FLEETMATICS_PASSWORD=your_fleetmatics_password
```

### Local Development
Update `src/EvoAPI.Api/appsettings.secrets.json`:

```json
{
  "Fleetmatics": {
    "BaseUrl": "https://api.fleetmatics.com/v1",
    "Username": "your_fleetmatics_username",
    "Password": "your_fleetmatics_password",
    "SyncHour": 2
  }
}
```

### Configuration Options
- `SyncHour`: Hour of day for automatic sync (0-23, default: 2)
- `BaseUrl`: Fleetmatics API base URL
- `Username`: Fleetmatics API username
- `Password`: Fleetmatics API password

## API Endpoints

All endpoints require authentication. Admin-only endpoints require ADMIN access level.

### Manual Sync Operations

#### Trigger Full Sync (Admin Only)
```
POST /EvoApi/fleetmatics/sync-vehicle-assignments
```
Manually triggers a complete vehicle assignment sync for all eligible users.

#### Get Individual Driver Assignment
```
GET /EvoApi/fleetmatics/driver-assignment/{employeeNumber}
```
Retrieves vehicle assignment for a specific driver using their employee number from the user table.

#### Update Vehicle Number (Admin Only)
```
PUT /EvoApi/fleetmatics/update-vehicle-number
Content-Type: application/json

{
  "userId": 123,
  "vehicleNumber": "TRUCK001"
}
```

### Monitoring and Status

#### Test API Connectivity (Admin Only)
```
GET /EvoApi/fleetmatics/test-connection
```
Tests connectivity to the Fleetmatics API and token retrieval.

#### Get Sync Service Status (Admin Only)
```
GET /EvoApi/fleetmatics/sync-service-status
```
Returns information about the background sync service including next scheduled run time.

#### Get Sync Eligible Users (Admin Only)
```
GET /EvoApi/fleetmatics/sync-eligible-users
```
Returns list of users eligible for Fleetmatics synchronization.

## Background Service Features

### Daily Automated Sync
- Runs daily at configured hour (default: 2:00 AM)
- Processes all active users with valid employee numbers
- Updates `u_vehiclenumber` field in user table

### Error Handling & Retry Logic
- **Max Retry Attempts**: 3
- **Retry Delay**: 30 minutes between attempts
- **Comprehensive Logging**: All operations logged to audit system
- **Graceful Failure**: Continues to next day if all retries fail

### Token Management
- **Automatic Token Refresh**: Handles 20-minute token expiration
- **Thread-Safe Caching**: Multiple concurrent requests use same token
- **2-Minute Buffer**: Refreshes token before expiration

## Database Integration

### User Table Updates
The sync process updates the following fields in the `[user]` table:
- `u_vehiclenumber`: Vehicle number from Fleetmatics
- `u_lastmodified`: Timestamp of last update

### Eligibility Criteria
Users are included in sync if:
- `u_active = 1` (active users only)
- `u_employeenumber` is not null and not empty
- Employee number has content after trimming whitespace

## Monitoring and Troubleshooting

### Audit Logging
All operations are logged to the audit system with details including:
- Sync results (success/failure counts)
- Individual user updates
- API connectivity issues
- Token refresh operations
- Background service lifecycle events

### Log Levels
- **Information**: Normal sync operations, scheduling info
- **Warning**: Partial sync failures, individual user errors
- **Error**: Complete sync failures, API connectivity issues

### Common Issues

#### API Connection Failures
- Verify environment variables are set correctly
- Check Fleetmatics API credentials
- Test using `/test-connection` endpoint

#### Sync Failures
- Check audit logs for specific error messages
- Verify database connectivity
- Ensure users have valid usernames

#### Background Service Not Running
- Check application logs for service startup messages
- Verify hosting environment supports background services
- Monitor using `/sync-service-status` endpoint

## Security Considerations

### Credentials Storage
- **Local Development**: Stored in `appsettings.secrets.json` (not committed)
- **Azure Deployment**: Stored as encrypted environment variables
- **No Frontend Exposure**: API keys never exposed to client-side code

### Access Control
- **Admin-Only Operations**: Sync operations require ADMIN access level
- **Authenticated Endpoints**: All endpoints require valid JWT token
- **Audit Trail**: All operations logged with user identification

## Integration Testing

### Manual Testing Steps
1. **Test API Connectivity**: `GET /EvoApi/fleetmatics/test-connection`
2. **Get Eligible Users**: `GET /EvoApi/fleetmatics/sync-eligible-users`
3. **Test Individual Lookup**: `GET /EvoApi/fleetmatics/driver-assignment/username`
4. **Manual Sync**: `POST /EvoApi/fleetmatics/sync-vehicle-assignments`
5. **Check Service Status**: `GET /EvoApi/fleetmatics/sync-service-status`

### Validation
- Verify vehicle numbers are updated in user table
- Check audit logs for operation details
- Monitor background service scheduling
- Test error handling with invalid credentials

## Performance Considerations

### API Rate Limits
- Token requests limited to once per 18 minutes (with 2-minute buffer)
- Individual driver lookups cached in token duration
- Bulk sync processes users sequentially to avoid rate limiting

### Database Performance
- Uses Dapper for efficient database operations
- Single connection per operation
- Minimal database queries (select eligible users, update vehicle numbers)

### Background Service Impact
- Runs during off-hours (configurable)
- Processes users sequentially
- Graceful error handling prevents service crashes
