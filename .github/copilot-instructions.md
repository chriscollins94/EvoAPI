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
            Count = workOrders.Count,
            ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00")
        });
    }
    catch (Exception ex)
    {
        stopwatch.Stop();
        await LogAuditErrorAsync("GetWorkOrders", ex);
        
        return StatusCode(500, new ApiResponse<List<WorkOrderDto>>
        {
            Success = false,
            Message = "Failed to retrieve work orders",
            ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00")
        });
    }
}
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
- `Shared/Attributes/` - `EvoAuthorize`, `AdminOnly` attributes
- `Shared/Models/ApiResponse.cs` - Standard response wrapper
- `Shared/DTOs/` - Data transfer objects for API responses

## Critical Don'ts
- **Never** bypass `BaseController` for authenticated endpoints
- **Never** skip `ApiResponse<T>` wrapper pattern
- **Never** forget timing measurement with `Stopwatch`
- **Always** use async/await patterns consistently
- **Always** include proper error handling and audit logging
