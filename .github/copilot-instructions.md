# AI Development Instructions for EvoAPI

## Architecture Overview

This is a **modern .NET 8 Web API** using clean architecture principles, designed to replace the legacy EvoWS (.NET Framework) API while maintaining compatibility with existing JWT authentication and frontend systems.

**Clean Architecture Layers:**
- `src/EvoAPI.Api` - Controllers, middleware, DI configuration
- `src/EvoAPI.Core` - Business logic, interfaces, domain models  
- `src/EvoAPI.Infrastructure` - Data access, external services
- `src/EvoAPI.Shared` - DTOs, attributes, cross-cutting models

## Critical Development Patterns

### BaseController Pattern
**Always** inherit from `BaseController` for automatic authentication:
```csharp
[ApiController]
[Route("api/[controller]")]
public class WorkOrderController : BaseController
{
    [HttpGet]
    public async Task<ActionResult<ApiResponse<List<WorkOrderDto>>>> GetWorkOrders()
    {
        // Authentication handled automatically
        var currentUserId = UserId; // Available from BaseController
        var isUserAdmin = IsAdmin;  // Available from BaseController
    }
}
```

### Authorization Attributes
```csharp
[EvoAuthorize]        // Default - requires valid JWT (inherited from BaseController)
[AdminOnly]           // Requires admin access level
[AllowAnonymous]      // Public endpoint - overrides authentication requirement
```

### ApiResponse Wrapper Pattern
**All** endpoints must return `ApiResponse<T>`:
```csharp
public async Task<ActionResult<ApiResponse<List<WorkOrderDto>>>> GetWorkOrders()
{
    var stopwatch = Stopwatch.StartNew();
    try
    {
        var workOrders = await _dataService.GetWorkOrdersAsync();
        stopwatch.Stop();
        
        return Ok(new ApiResponse<List<WorkOrderDto>>
        {
            Success = true,
            Message = "Work orders retrieved successfully",
            Data = workOrders,
            Count = workOrders.Count
        });
    }
    catch (Exception ex)
    {
        stopwatch.Stop();
        await LogAuditErrorAsync("GetWorkOrders", ex);
        
        return StatusCode(500, new ApiResponse<List<WorkOrderDto>>
        {
            Success = false,
            Message = "Failed to retrieve work orders"
        });
    }
}
```

### ApiResponse<T> Structure
The `ApiResponse<T>` class is located in `Shared/DTOs/WorkOrderDto.cs` and has these properties:
```csharp
public class ApiResponse<T>
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public T? Data { get; set; }
    public int Count { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}
```
**Important**: There is **no** `ResponseTime` property. Do not add this property to ApiResponse objects.

### Audit Logging Method Signatures
BaseController provides these audit logging methods:
```csharp
// Standard audit logging - responseTime parameter must be string
protected async Task LogAuditAsync(string description, object? detail = null, string? responseTime = null)

// Error audit logging
protected async Task LogAuditErrorAsync(string description, Exception ex, object? detail = null)
```
**Critical**: `LogAuditAsync` expects `string?` for responseTime parameter, not `TimeSpan`. Always convert:
```csharp
await LogAuditAsync("Operation", detail, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
```

## Authentication & Authorization

### JWT Token Compatibility
- **Compatible** with existing EvoWS token system
- Tokens contain: `id`, `username`, `accesslevel`, `firstname`, `lastname`, `XSRF-TOKEN`
- Cookie-based (`AccessToken`) and header-based authentication supported

### BaseController Properties
Available in all controllers inheriting from `BaseController`:
- `UserId` - Current user ID (int)
- `Username` - Current username (string)
- `IsAdmin` - Boolean admin check
- `UserFullName` - First + Last name
- `ClientIPAddress` - Client IP address
- `UserAgent` - User agent string

## Development Workflow

### Running EvoAPI
```bash
# Development (uses appsettings.Development.json)
dotnet run --project src/EvoAPI.Api

# Production build
dotnet publish src/EvoAPI.Api -c Release -o publish/evoapi
```

**Important Note**: The `dotnet run` command to start the EvoAPI will be run manually by Chris. AI systems should not attempt to run or automate this command.

## Local Development Configuration

### Secrets Management
For local development, EvoAPI uses `appsettings.secrets.json` to store sensitive configuration values like API keys and database passwords. This file is **NOT** committed to version control.

**Location**: `src/EvoAPI.Api/appsettings.secrets.json`

**Structure**:
```json
{
  "GoogleMaps": {
    "ApiKey": "AIzaSy..."
  },
  "DB_PASSWORD": "actual_password_here"
}
```

### Configuration Hierarchy
The configuration system loads values in this order (later sources override earlier ones):
1. `appsettings.json` (base settings)
2. `appsettings.{Environment}.json` (environment-specific settings)
3. `appsettings.secrets.json` (local secrets - **only for local development**)
4. Environment variables (for Test/Production Azure deployments)

### Environment-Specific Configuration
- **Local Development**: Uses `appsettings.secrets.json` for sensitive values
- **Test Environment (Azure)**: Uses environment variables (`GOOGLE_MAPS_API_KEY`, `DB_PASSWORD`)
- **Production Environment (Azure)**: Uses environment variables (`GOOGLE_MAPS_API_KEY`, `DB_PASSWORD`)

### Configuration Loading (Program.cs)
The `Program.cs` automatically loads the secrets file for local development:
```csharp
// Add secrets file if it exists (for local development in any environment)
var secretsPath = Path.Combine(builder.Environment.ContentRootPath, "appsettings.secrets.json");
if (File.Exists(secretsPath))
{
    builder.Configuration.AddJsonFile("appsettings.secrets.json", optional: true, reloadOnChange: true);
}
```

### Adding New Secrets
1. **For local development**: Add to `appsettings.secrets.json`
2. **For Test/Production**: Set as environment variables in Azure
3. **Configuration files**: Use placeholder format `"ApiKey": "${ENVIRONMENT_VARIABLE_NAME}"`

### Important Security Notes
- `appsettings.secrets.json` is in `.gitignore` and **never** committed to source control
- Environment-specific config files (`appsettings.Test.json`, etc.) use placeholders like `${GOOGLE_MAPS_API_KEY}`
- Real values are only in the secrets file (local) or environment variables (Azure)

### Adding New Endpoints
1. **Create controller** inheriting from `BaseController`
2. **Use appropriate authorization** attributes
3. **Return `ApiResponse<T>`** wrapper with timing
4. **Add audit logging** using BaseController methods
5. **Test authentication** with existing JWT tokens

### Audit Logging Pattern
```csharp
public async Task<ActionResult> UpdatePriority(int id, UpdatePriorityRequest request)
{
    return await ExecuteWithAuditAsync("UpdatePriority", request, async () =>
    {
        // Business logic here
        await _dataService.UpdatePriorityAsync(id, request);
        
        return Ok(new ApiResponse<object>
        {
            Success = true,
            Message = "Priority updated successfully"
        });
    });
}
```

## Testing & Integration

### Key Test Endpoints
- `GET /api/authtest/public/status` - Public (no auth)
- `GET /api/authtest/profile` - Authenticated user info
- `GET /api/authtest/admin/users` - Admin-only endpoint

### Frontend Integration
Frontend calls use proxy routing:
- `/api/evoapi/*` routes to `https://localhost:5001/EvoApi/*`
- Cookies automatically forwarded for authentication
- Response structure expected: `{success, message, data, count}`

## Migration from EvoWS

### Before (EvoWS Pattern)
```csharp
public HttpResponseMessage GetData()
{
    if (!LoggedIn())
        return new HttpResponseMessage(HttpStatusCode.Unauthorized);
    
    // Manual audit logging
    auditEntry.a_username = user.u_username;
    evoData.InsertAudit(auditEntry);
}
```

### After (EvoAPI Pattern)
```csharp
[HttpGet]
public async Task<ActionResult<ApiResponse<DataDto>>> GetData()
{
    // Authentication & audit handled by BaseController
    var data = await _dataService.GetDataAsync(UserId);
    
    return Ok(new ApiResponse<DataDto>
    {
        Success = true,
        Data = data
    });
}
```

## Key Files & Directories

- `Controllers/BaseController.cs` - **Authentication base class**
- `Controllers/EvoApiController.cs` - Main business endpoints
- `Controllers/MappingController.cs` - **Google Maps API proxy and caching**
- `Controllers/FleetmaticsController.cs` - **Fleetmatics API integration and vehicle assignment sync**
- `Shared/Attributes/` - `EvoAuthorize`, `AdminOnly` attributes
- `Shared/Models/ApiResponse.cs` - Standard response wrapper
- `Shared/DTOs/` - Data transfer objects for API responses

## Fleetmatics Integration

### Fleetmatics API Architecture
The Fleetmatics integration provides automated vehicle assignment synchronization with the following components:
- **FleetmaticsService** - Core API integration with token management and driver lookups
- **FleetmaticsSyncService** - Daily background service for automated synchronization
- **FleetmaticsController** - Admin-only REST endpoints for manual operations

### Authentication Flow
Fleetmatics uses a **two-step authentication process**:
1. **Token Endpoint**: `POST /token` with Basic Auth → Returns JWT token directly (not JSON wrapped)
2. **API Calls**: Use special "Atmosphere" authorization format with app ID + Bearer token

```csharp
// Step 1: Get JWT token (returns raw JWT string, not JSON)
var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
httpClient.DefaultRequestHeaders.Authorization = 
    new AuthenticationHeaderValue("Basic", credentials);
var tokenResponse = await httpClient.PostAsync("/token", content);
var jwtToken = await tokenResponse.Content.ReadAsStringAsync(); // Direct JWT string

// Step 2: Use Atmosphere authorization for API calls
httpClient.DefaultRequestHeaders.Authorization = 
    new AuthenticationHeaderValue("Atmosphere", 
        $"atmosphere_app_id={atmosphereAppId}, Bearer {jwtToken}");
```

### Configuration Requirements
```json
{
  "Fleetmatics": {
    "BaseUrl": "${FLEETMATICS__BASEURL}",           // https://fim.api.us.fleetmatics.com
    "Username": "${FLEETMATICS__USERNAME}",         // REST_EvoTrakkerAPI_XXXX@XXXXXXX.com
    "Password": "${FLEETMATICS__PASSWORD}",         // API password
    "AtmosphereAppId": "${FLEETMATICS__ATMOSPHEREAPPID}", // fleetmatics-p-us-XXXXXXXXXX
    "SyncHour": 2                                   // Daily sync time (2:00 AM)
  }
}
```

### Environment Variables (Azure)
- `Fleetmatics__BaseUrl` - Fleetmatics API base URL
- `Fleetmatics__Username` - API username
- `Fleetmatics__Password` - API password  
- `Fleetmatics__AtmosphereAppId` - Required for Atmosphere authorization format

### API Endpoints Structure
```csharp
[ApiController]
[Route("EvoApi/fleetmatics")]  // Note: Azure maps /api/EvoApi/fleetmatics to this route
public class FleetmaticsController : BaseController
{
    [HttpPost("sync-vehicle-assignments")]  // Manual sync all users
    [HttpGet("driver-assignment/{employeeNumber}")]  // Individual lookup
    [HttpGet("test-connection")]  // Connection test
    [HttpGet("sync-service-status")]  // Background service status
}
```

### Data Mapping Pattern
Fleetmatics API returns employee-based vehicle assignments:
```json
{
  "DriverNumber": "099",
  "VehicleNumber": "82651H2", 
  "StartDateUTC": "2022-01-20T21:34:00"
}
```

Maps to EvoAPI user table updates:
```sql
UPDATE [user] 
SET u_vehiclenumber = @VehicleNumber
WHERE u_id = @UserId
-- Note: u_lastmodified handled by database trigger
```

### Background Service Pattern
```csharp
public class FleetmaticsSyncService : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTime.Now;
            var nextRun = DateTime.Today.AddHours(_syncHour);
            
            if (now >= nextRun)
                nextRun = nextRun.AddDays(1);
                
            var delay = nextRun - now;
            await Task.Delay(delay, stoppingToken);
            
            // Daily sync logic
            await SyncVehicleAssignments();
        }
    }
}
```

### Integration Patterns
**Employee Number Lookup**: Uses `u_employeenumber` field from user table to match Fleetmatics driver assignments
**Vehicle Assignment**: Updates `u_vehiclenumber` field with current vehicle assignment
**Audit Logging**: Comprehensive logging of all API calls, errors, and database updates
**Error Handling**: Graceful handling of missing assignments, API errors, and retry logic

### Common Troubleshooting
- **"Required Header Parameter Missing: atmosphere_app_id"** → Check AtmosphereAppId configuration
- **"'e' is an invalid start of a value"** → JWT token parsing issue, ensure handling direct token response
- **"Invalid column name 'u_lastmodified'"** → Remove from SQL update, handled by database trigger
- **404 on endpoints** → Check route mapping, use `EvoApi/fleetmatics` not `api/EvoApi/fleetmatics`

## Critical Don'ts
- **Never** bypass `BaseController` for authenticated endpoints
- **Never** skip `ApiResponse<T>` wrapper pattern
- **Never** forget timing measurement with `Stopwatch`
- **Never** add `ResponseTime` property to `ApiResponse<T>` objects - this property doesn't exist
- **Never** pass `TimeSpan` to `LogAuditAsync` - convert to string first: `timeSpan.TotalSeconds.ToString("0.00")`
- **Never** use standard Bearer authorization for Fleetmatics API calls - use Atmosphere format
- **Never** expect JSON-wrapped token response from Fleetmatics - handle direct JWT string
- **Never** include `u_lastmodified` in SQL updates - handled by database triggers
- **Never** use employee numbers directly without URI encoding in API URLs
- **Always** use async/await patterns consistently
- **Always** include proper error handling and audit logging
- **Always** verify `ApiResponse<T>` property names match the actual class definition
- **Always** use `Fleetmatics__` prefix for environment variables (double underscore for nested config)
- **Always** validate Fleetmatics DTO property names match actual API response structure
