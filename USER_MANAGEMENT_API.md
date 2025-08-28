# User Management API Documentation

## Overview
Complete user management system with CRUD operations for the User table. All endpoints inherit authentication from BaseController and follow EvoAPI patterns.

## Endpoints

### GET /EvoApi/users/management
**Admin Access Required**
- Returns all users with complete field data for management purposes
- Includes sensitive fields like passwords (hashed), SSN, etc.
- Response: `ApiResponse<List<UserDto>>`

### GET /EvoApi/users/{id}
**Admin Access Required** 
- Returns a single user by ID with all fields
- Response: `ApiResponse<UserDto>`
- Returns 404 if user not found

### GET /EvoApi/users
**Standard Access**
- Returns active users with basic fields (existing endpoint)
- Used for dropdowns and general user lists
- Response: `ApiResponse<List<UserDto>>`

### POST /EvoApi/users
**Admin Access Required**
- Creates a new user
- Request: `CreateUserRequest`
- Response: `ApiResponse<UserDto>` (password field cleared in response)
- Validates username (3+ chars), password (6+ chars), and OId

### PUT /EvoApi/users/{id}
**Admin Access Required**
- Updates existing user
- Request: `UpdateUserRequest`
- Response: `ApiResponse<UserDto>` (password field cleared in response)
- Password is optional - only updated if provided
- Validates username (3+ chars), password (6+ chars if provided), and OId

## Data Models

### UserDto
Complete user model with all fields from the User table:
```csharp
public class UserDto
{
    public int Id { get; set; }
    public int OId { get; set; }
    public int? AId { get; set; }
    public int? VId { get; set; }
    public int? SupervisorId { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public string Username { get; set; }
    public string Password { get; set; } // Cleared in API responses
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? EmployeeNumber { get; set; }
    public string? Email { get; set; }
    public string? PhoneHome { get; set; }
    public string? PhoneMobile { get; set; }
    public bool Active { get; set; }
    public string? Picture { get; set; }
    public string? SSN { get; set; }
    public DateTime? DateOfHire { get; set; }
    public DateTime? DateEligiblePTO { get; set; }
    public DateTime? DateEligibleVacation { get; set; }
    public decimal? DaysAvailablePTO { get; set; }
    public decimal? DaysAvailableVacation { get; set; }
    public string? ClothingShirt { get; set; }
    public string? ClothingJacket { get; set; }
    public string? ClothingPants { get; set; }
    public string? WirelessProvider { get; set; }
    public string? PreferredNotification { get; set; }
    public string? QuickBooksName { get; set; }
    public DateTime? PasswordChanged { get; set; }
    public bool U_2FA { get; set; }
    public int? ZoneId { get; set; }
    public DateTime? CovidVaccineDate { get; set; }
    public string? Note { get; set; }
    public string? NoteDashboard { get; set; }
    
    // Computed properties
    public string FullName => $"{FirstName} {LastName}".Trim();
    public string DisplayName => !string.IsNullOrEmpty(FullName) ? FullName : Username;
}
```

### CreateUserRequest
All fields except Id, audit fields, and computed properties

### UpdateUserRequest  
All fields except audit fields and computed properties. Password is optional.

## Authentication & Authorization
- All new endpoints use BaseController for automatic JWT authentication
- Admin-only endpoints will require `[AdminOnly]` attribute (to be added)
- Audit logging included for all operations
- Password change automatically sets u_passwordchanged timestamp

## Database Integration
- Full SQL INSERT and UPDATE operations
- Proper handling of nullable fields with DBNull.Value
- Comprehensive error handling and logging
- Follows established DataService patterns

## Frontend Integration
Ready for frontend implementation with:
- Complete CRUD operations
- Form validation support
- Toast notification integration
- Admin guard protection
- Consistent ApiResponse format

## Security Considerations
- Passwords are never returned in API responses (cleared before sending)
- Sensitive fields like SSN are included but should be admin-only
- Password updates are optional and properly handled
- All operations are audited
