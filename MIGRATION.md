# Migration Guide: EvoWS to EvoAPI

## Authentication Migration Examples

### Before (EvoWS - .NET Framework 4.6.2)
```csharp
[HttpPost]
[ActionName("GetBoardBadgeCount")]
public HttpResponseMessage GetBoardBadgeCount([FromBody] SimpleID simpleid)
{
    var resp = new HttpResponseMessage();

    try
    {
        if (!LoggedIn()) //return 401 if not logged in
        {
            return (new HttpResponseMessage(HttpStatusCode.Unauthorized) { Content = new StringContent("") });
        }

        // ... rest of implementation
        
        resp.StatusCode = HttpStatusCode.OK;
        resp.Content = new ObjectContent<object>(dt, Configuration.Formatters.JsonFormatter);
    }
    catch (Exception ex)
    {
        // Manual error handling and logging
    }
    
    return resp;
}
```

### After (EvoAPI - .NET 8)
```csharp
[HttpGet("board/{id}/badge-count")]
public async Task<ActionResult<BoardBadgeCountDto>> GetBoardBadgeCount(int id)
{
    // Authentication is automatically handled by [EvoAuthorize] on BaseController
    // User info is available via properties: UserId, Username, AccessLevel, etc.
    
    if (id <= 0)
    {
        return BadRequest("Invalid ID provided");
    }
    
    var result = await _boardService.GetBadgeCountAsync(id);
    return Ok(result);
    
    // Error handling and audit logging handled by middleware
}
```

## Key Differences

### Authentication
- **Old**: Manual `LoggedIn()` checks in every method
- **New**: Automatic via `[EvoAuthorize]` attribute on BaseController

### Authorization  
- **Old**: Manual `IsAdmin()` checks with exceptions
- **New**: Declarative `[AdminOnly]` attribute

### Error Handling
- **Old**: Try-catch in every method with manual audit logging
- **New**: Global exception handling and automatic audit logging via middleware

### Return Types
- **Old**: `HttpResponseMessage` with manual status codes
- **New**: Strongly-typed `ActionResult<T>` with automatic serialization

### User Context
- **Old**: Manual JWT parsing in every controller constructor
- **New**: Available as properties on `BaseController`

## Migration Steps

1. **Identify endpoint** in EvoWS controller
2. **Create new controller** inheriting from `BaseController`
3. **Remove manual auth checks** - handled automatically
4. **Use modern return types** - `ActionResult<T>`
5. **Apply specific attributes** if needed (`[AdminOnly]`, `[AllowAnonymous]`)
6. **Update client** to call new endpoint

## Example Migration

### 1. Old Endpoint
```csharp
// EvoWS/Controllers/EvoController.cs
[HttpPost]
[ActionName("GetActiveEmployees")]
public HttpResponseMessage GetActiveEmployees()
{
    if (!LoggedIn()) return Unauthorized();
    
    try 
    {
        DataTable dt = EvoData.GetActiveEmployees();
        return Ok(dt);
    }
    catch (Exception ex)
    {
        // manual logging
        throw new HttpResponseException(HttpStatusCode.InternalServerError);
    }
}
```

### 2. New Endpoint
```csharp
// EvoAPI/Controllers/EmployeeController.cs
[HttpGet("active")]
public async Task<ActionResult<IEnumerable<EmployeeDto>>> GetActiveEmployees()
{
    // Auth automatic, user context available via base properties
    var employees = await _employeeService.GetActiveEmployeesAsync();
    return Ok(employees);
    // Error handling and logging automatic via middleware
}
```

### 3. Client Update
```javascript
// Old
const response = await fetch('/evoWS/api/Evo/GetActiveEmployees', {
    method: 'POST',
    credentials: 'include'
});

// New  
const response = await fetch('/api/employee/active', {
    method: 'GET',
    credentials: 'include'
});
```
