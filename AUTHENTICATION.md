# EvoAPI Authentication Implementation

This document describes the authentication and authorization system implemented in the EvoAPI project.

## Overview

The EvoAPI uses JWT (JSON Web Token) based authentication with support for both Authorization header and cookie-based tokens for backward compatibility with the existing EvoWS system.

## Architecture

### Authentication Flow
1. User authenticates via existing system (EvoWS)
2. JWT token is issued and stored in `AccessToken` cookie
3. EvoAPI validates the JWT token on each request
4. Claims from the token are available throughout the application

### Authorization Levels
- **Anonymous**: No authentication required
- **Logged In**: Requires valid JWT token
- **Admin**: Requires JWT token with `accesslevel` claim set to "ADMIN"

## Implementation Details

### JWT Claims Expected
The JWT token should contain the following claims:
- `id`: User ID (integer)
- `username`: Username (string)
- `accesslevel`: Access level ("ADMIN" or other values)
- `firstname`: User's first name (optional)
- `lastname`: User's last name (optional)
- `XSRF-TOKEN`: Cross-site request forgery protection token

### Configuration

Update `appsettings.json` with your JWT settings:

```json
{
  "Jwt": {
    "Key": "YourSecretKeyHereMustBe32CharactersLong!",
    "Issuer": "EvoAPI",
    "Audience": "EvoClient",
    "ExpiryInMinutes": 60
  }
}
```

### Usage Examples

#### Controller with Authentication
```csharp
[ApiController]
[Route("api/[controller]")]
public class WorkOrderController : BaseController
{
    // Inherits authentication requirement from BaseController
    
    [HttpGet]
    public IActionResult GetWorkOrders()
    {
        // User is automatically authenticated
        var currentUserId = UserId; // Available from BaseController
        var isUserAdmin = IsAdmin;  // Available from BaseController
        
        // Implementation...
    }
    
    [HttpPost("admin/reset")]
    [AdminOnly] // Requires admin access
    public IActionResult ResetWorkOrders()
    {
        // Only admins can access this endpoint
        // Implementation...
    }
    
    [HttpGet("public/count")]
    [AllowAnonymous] // Override authentication requirement
    public IActionResult GetWorkOrderCount()
    {
        // Public endpoint, no authentication required
        // Implementation...
    }
}
```

#### Available Properties in BaseController
- `UserId`: Current user's ID
- `Username`: Current user's username  
- `AccessLevel`: Current user's access level
- `IsAdmin`: Boolean indicating if user is admin
- `UserFullName`: User's full name
- `IsLoggedIn`: Boolean indicating if user is authenticated
- `ClientIPAddress`: Client's IP address
- `UserAgent`: Client's user agent string

### Attributes

#### `[EvoAuthorize]`
Requires user to be authenticated (logged in). Applied by default to all controllers inheriting from `BaseController`.

#### `[AdminOnly]` 
Requires user to be authenticated AND have admin access level.

#### `[AllowAnonymous]`
Overrides authentication requirements for specific endpoints.

### Middleware

#### AuditMiddleware
Automatically logs all API requests including:
- Username and full name
- Request path and method
- Response time
- IP address and user agent
- Error details (if any)

### Testing Endpoints

The API includes test endpoints to verify authentication:

- `GET /api/authtest/public/status` - Public endpoint (no auth required)
- `GET /api/authtest/profile` - Returns current user profile (auth required)
- `GET /api/authtest/admin/users` - Admin-only endpoint
- `GET /api/authtest/test-auth` - Detailed authentication information

### Error Handling

- **401 Unauthorized**: User is not authenticated or token is invalid
- **403 Forbidden**: User is authenticated but lacks required permissions

### Security Features

1. **JWT Validation**: Full validation of token signature, issuer, audience, and expiration
2. **Cookie Support**: Backward compatibility with existing cookie-based authentication
3. **CORS Protection**: Configured allowed origins for cross-origin requests
4. **Audit Logging**: Comprehensive logging of all API access
5. **Claims-based Authorization**: Fine-grained access control based on user claims

## Migration from EvoWS

To migrate existing endpoints from EvoWS to EvoAPI:

1. Replace manual `LoggedIn()` checks with `[EvoAuthorize]` attribute
2. Replace manual `IsAdmin()` checks with `[AdminOnly]` attribute  
3. Use `BaseController` properties instead of parsing JWT manually
4. Remove manual audit logging - handled automatically by middleware

### Before (EvoWS)
```csharp
public HttpResponseMessage GetData()
{
    if (!LoggedIn())
    {
        return new HttpResponseMessage(HttpStatusCode.Unauthorized);
    }
    
    // Manual audit logging
    auditEntry.a_username = user.u_username;
    EvoData.InsertAudit(auditEntry);
    
    // Implementation...
}
```

### After (EvoAPI)
```csharp
[HttpGet]
public IActionResult GetData()
{
    // Authentication and audit logging handled automatically
    var currentUser = Username; // Available from BaseController
    
    // Implementation...
}
```

This provides a much cleaner, more secure, and maintainable authentication system while maintaining compatibility with your existing JWT tokens.
