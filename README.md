# EvoAPI

A modern .NET 8 Web API project using clean architecture principles. This API serves as the next-generation replacement for the legacy EvoWS (.NET Framework 4.6.2) API.

## Features

- **JWT Authentication**: Compatible with existing EvoWS token system
- **Clean Architecture**: Separation of concerns with distinct layers
- **Automatic Auditing**: Request logging and performance monitoring
- **Swagger Documentation**: Built-in API documentation
- **CORS Support**: Configured for existing client applications
- **Role-based Authorization**: Admin and user-level access control

## Structure

- `src/EvoAPI.Api` - API layer (controllers, middleware, configuration)
- `src/EvoAPI.Core` - Core business logic (services, interfaces)
- `src/EvoAPI.Infrastructure` - Data access and infrastructure concerns
- `src/EvoAPI.Shared` - Shared models, DTOs, and attributes
- `tests/` - Test projects (to be added)

## Authentication

The API uses JWT token authentication compatible with the existing EvoWS system. See [AUTHENTICATION.md](./AUTHENTICATION.md) for detailed documentation.

### Quick Start - Authentication

1. **Public endpoints** - Use `[AllowAnonymous]`
2. **Authenticated endpoints** - Inherit from `BaseController` (default)
3. **Admin-only endpoints** - Use `[AdminOnly]` attribute

## Getting Started

### Prerequisites
- .NET 8 SDK
- Visual Studio 2022 or VS Code

### Setup

1. Clone the repository
2. Open the solution in Visual Studio or VS Code
3. Update `appsettings.json` with your JWT configuration:
   ```json
   {
     "Jwt": {
       "Key": "YourSecretKeyHereMustBe32CharactersLong!",
       "Issuer": "EvoAPI",
       "Audience": "EvoClient"
     }
   }
   ```
4. Build and run:
   ```bash
   dotnet build
   dotnet run --project src/EvoAPI.Api
   ```

### Testing Authentication

Visit these endpoints to test authentication:
- `GET /api/authtest/public/status` - Public (no auth)
- `GET /api/authtest/profile` - Requires authentication
- `GET /api/authtest/admin/users` - Requires admin access

## Migration from EvoWS

This API is designed to gradually replace endpoints from the legacy EvoWS system:

1. **Maintain compatibility** - Uses same JWT token format
2. **Improved security** - Built-in authentication middleware
3. **Better performance** - .NET 8 performance improvements
4. **Cleaner code** - Attribute-based authorization vs manual checks

### Migration Example

**Before (EvoWS):**
```csharp
if (!LoggedIn()) {
    return new HttpResponseMessage(HttpStatusCode.Unauthorized);
}
```

**After (EvoAPI):**
```csharp
[HttpGet]
public IActionResult GetData()
{
    // Authentication handled automatically
    var userId = UserId; // From BaseController
}
```

## Development

### Adding New Controllers

1. Inherit from `BaseController` for automatic authentication
2. Use appropriate authorization attributes (`[AdminOnly]`, `[AllowAnonymous]`)
3. Access user information via BaseController properties

### Available in BaseController

- `UserId` - Current user ID
- `Username` - Current username
- `AccessLevel` - User's access level
- `IsAdmin` - Boolean admin check
- `UserFullName` - User's full name
- `ClientIPAddress` - Client IP
- `UserAgent` - User agent string

## Documentation

- [Authentication Guide](./AUTHENTICATION.md) - Detailed authentication documentation
- [Swagger UI](https://localhost:5001/swagger) - Interactive API documentation (when running)

---

This modern API provides a secure, maintainable foundation for migrating from EvoWS while maintaining compatibility with existing client applications.
