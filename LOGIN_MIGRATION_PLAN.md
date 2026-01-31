# Login Migration Plan: EvoWS → EvoAPI

## Executive Summary

This plan outlines the migration of the legacy login authentication system from EvoWS (.NET Framework 4.6.2) to EvoAPI (.NET Core 8.0), maintaining full backward compatibility with existing functionality while leveraging modern ASP.NET Core patterns.

### Key Decisions

- ✅ **Password Security:** Maintain plain text passwords for backward compatibility
- ✅ **Token Compatibility:** Full compatibility - same HMAC key and JWT claim structure
- ✅ **TimeTracking:** Continue creating TimeTracking records with geolocation on login
- ✅ **2FA:** Maintain legacy weekly rotating secure code implementation

---

## 1. Architecture Overview

### Current State (EvoWS)
```
Client (evotech)
    ↓ POST /token/Login {username, password, lat, lon}
EvoWS TokenController
    ↓ Validates credentials (plain text)
Database [User] table
    ↓ Creates JWT token (HMAC-SHA256)
TokenManager
    ↓ Sets cookies (AccessToken, XSRF-TOKEN, LoginTracking)
Response with token + TimeTrackingStatus
```

### Target State (EvoAPI)
```
Client (evotech)
    ↓ POST /api/authentication/login {username, password, lat, lon}
EvoAPI AuthenticationController
    ↓ Validates credentials via AuthenticationService
AuthenticationService → DataService → Database
    ↓ Generates JWT token (same key as EvoWS)
JwtTokenService
    ↓ Creates TimeTracking record (ttt_id=1)
TimeTrackingService
    ↓ Sets cookies & returns response
Response with token + TimeTrackingStatus
```

### Transition Strategy
- EvoAPI and EvoWS will coexist during migration
- Both systems share the same JWT signing key for token compatibility
- Clients can authenticate via either system
- Frontend will be updated to call EvoAPI login endpoint
- EvoWS login remains available as fallback during transition

---

## 2. Leveraging Existing EvoAPI Infrastructure

### 2.0 Existing Services to Reuse ✅

**The following functionality already exists in EvoAPI and will be reused:**

1. **DataService** - Already provides:
   - `GetConfigSettingAsync(string identifier)` - Retrieve config from database (eliminates need for ConfigurationService)
   - `ExecuteQueryAsync(sql, parameters)` - Generic SQL SELECT queries
   - `ExecuteNonQueryAsync(sql, parameters)` - Generic SQL INSERT/UPDATE/DELETE
   - Can be used for all user authentication and TimeTracking database operations

2. **AuditService** - Already exists:
   - Automatically logs all requests via AuditMiddleware
   - `LogAsync()` and `LogErrorAsync()` methods available
   - No need to create custom audit logging

3. **JWT Authentication Setup** - Already configured in Program.cs:
   - `GetEvoWSSigningKey()` method with correct HMAC key (lines 266-273)
   - JWT validation configured (lines 180-208)
   - Cookie-based token extraction implemented
   - Authorization policies (AdminOnly, UserAdminOnly) defined

4. **CORS & Middleware** - Already configured:
   - CORS policy includes all necessary origins
   - AuditMiddleware automatically logs all requests

**IMPACT:** Reduces implementation from 14 files to ~10 files by eliminating ConfigurationService and leveraging existing infrastructure.

---

## 2. Implementation Components

### 2.1 New Files to Create

#### A. Authentication Controller
**Location:** `src/EvoAPI.Api/Controllers/AuthenticationController.cs`

**Responsibilities:**
- Handle POST `/api/authentication/login` endpoint
- Accept `LoginRequest` (username, password, latitude, longitude)
- Call `IAuthenticationService` for credential validation
- Generate JWT token via `IJwtTokenService`
- Create TimeTracking record via `ITimeTrackingService`
- Set HTTP cookies (AccessToken, XSRF-TOKEN, LoginTracking)
- Return `ApiResponse<LoginResponse>` with token and user info
- Audit all login attempts (success and failure)

**Key Methods:**
- `Login(LoginRequest request)` - Main login endpoint
- `Logout()` - Clear cookies and create logout TimeTracking record
- `RefreshToken()` - Optional: refresh expired tokens

**Attributes:**
- `[AllowAnonymous]` on login endpoint
- Inherits from `BaseController` for audit logging

---

#### B. Authentication Service (Core Layer)
**Location:** `src/EvoAPI.Core/Services/AuthenticationService.cs`
**Interface:** `src/EvoAPI.Core/Interfaces/IAuthenticationService.cs`

**Responsibilities:**
- Validate user credentials against database
- Handle 2FA secure code detection and validation
- Retrieve user details and permissions
- Calculate secure code using legacy algorithm
- Return authenticated user object or throw authentication exception

**Key Methods:**
```csharp
Task<AuthenticatedUser> ValidateCredentialsAsync(string username, string password, bool require2fa = false)
Task<string> CalculateSecureCodeAsync() // Weekly rotating code
Task<UserPermissions> GetUserPermissionsAsync(string username)
```

**Business Logic:**
- Query `[User]` table: `WHERE u_username = @username AND u_password = @password AND u_active = 1`
- 2FA Detection: If password ends with 3 digits, validate against `CalculateSecureCode()`
- Extract user properties: `u_id`, `u_firstname`, `u_lastname`, `u_picture`, `u_passwordchanged`
- Query user functions via joins to `xrefUserRole`, `xrefRoleFunction`, `Function` tables
- Determine access level (ADMIN or TECH based on functions)

---

#### C. JWT Token Service (Core Layer)
**Location:** `src/EvoAPI.Core/Services/JwtTokenService.cs`
**Interface:** `src/EvoAPI.Core/Interfaces/IJwtTokenService.cs`

**Responsibilities:**
- Generate JWT tokens compatible with EvoWS format
- Use same HMAC-SHA256 signing key from configuration
- Include all required claims for backward compatibility
- Generate XSRF tokens (GUIDs)
- Set token expiration based on config timeout

**Key Methods:**
```csharp
Task<JwtTokenResult> CreateJwtTokenAsync(AuthenticatedUser user, string xsrfToken, int timeoutInMinutes)
ClaimsPrincipal ValidateJwtToken(string jwtToken) // For token refresh
```

**JWT Claims (Must Match EvoWS):**
- `ClaimTypes.Name` → username
- `"username"` → username
- `"firstname"` → user.u_firstname
- `"lastname"` → user.u_lastname
- `"passwordchanged"` → user.u_passwordchanged (DateTime or empty string)
- `"picture"` → user.u_picture (URL path)
- `"id"` → user.u_id (numeric string)
- `"XSRF-TOKEN"` → xsrfToken (GUID)
- `"function"` → Multiple claims, one per assigned function (e.g., "TECH", "ADMIN", "Admin - User Admin")
- `"accesslevel"` → Multiple claims based on function (only "TECH" and "ADMIN" supported)

**Token Configuration:**
- **Algorithm:** `SecurityAlgorithms.HmacSha256`
- **Signing Key:** From `appsettings.json` → `Jwt:Key` (must match EvoWS `KeyForHmacSha256`)
- **Issuer:** From config (backward compatible with EvoWS)
- **Audience:** From config (backward compatible with EvoWS)
- **Expiration:** Default from config `Jwt:ExpiryInMinutes` (match EvoWS `iTokenWindowsAuthJWT`)
- **Max Timeout:** 1440 minutes (24 hours) enforced

---

#### D. TimeTracking Service (Core Layer)
**Location:** `src/EvoAPI.Core/Services/TimeTrackingService.cs`
**Interface:** `src/EvoAPI.Core/Interfaces/ITimeTrackingService.cs`

**Responsibilities:**
- Create TimeTracking records for login/logout events
- Store geolocation data (latitude/longitude)
- Retrieve TimeTrackingStatus (aggregate view of today's activity)
- Query time tracking records for reporting

**Key Methods:**
```csharp
Task<int> CreateLoginTrackingAsync(int userId, decimal latitude, decimal longitude)
Task<int> CreateLogoutTrackingAsync(int userId, decimal latitude, decimal longitude)
Task<TimeTrackingStatus> GetTimeTrackingStatusAsync(int userId)
```

**Database Operations:**
```sql
-- Login Tracking
INSERT INTO TimeTracking (ttt_id, u_id, tt_begin, tt_begin_lat, tt_begin_lon)
VALUES (1, @u_id, GETDATE(), @latitude, @longitude)

-- Logout Tracking
UPDATE TimeTracking
SET tt_end = GETDATE(), tt_end_lat = @latitude, tt_end_lon = @longitude
WHERE u_id = @u_id AND ttt_id = 1 AND tt_end IS NULL

-- Get Today's Status (aggregate query)
SELECT
    MIN(CASE WHEN ttt_id = 1 THEN tt_begin END) as LoginTime,
    MIN(CASE WHEN ttt_id = 2 THEN tt_begin END) as ClockInTime,
    SUM(DATEDIFF(MINUTE, tt_begin, ISNULL(tt_end, GETDATE()))) as MinutesWorked
FROM TimeTracking
WHERE u_id = @u_id
  AND CAST(tt_begin as DATE) = CAST(GETDATE() as DATE)
```

---

#### E. ~~Configuration Service~~ - NOT NEEDED ✅

**USES EXISTING:** `IDataService.GetConfigSettingAsync(string identifier)` already exists

The existing DataService provides configuration retrieval from the database. Simply inject IDataService and use:
```csharp
var configValue = await _dataService.GetConfigSettingAsync("iTokenWindowsAuthJWT");
// Parse and use the value
```

For seed values (seedLogin, seedWeek), these will be stored in `appsettings.json` since they're static values, not database-driven.

---

### 2.2 New DTOs/Models (Shared Layer)

**Location:** `src/EvoAPI.Shared/DTOs/Authentication/`

#### LoginRequest.cs
```csharp
public class LoginRequest
{
    [Required]
    public string Username { get; set; } = string.Empty;

    [Required]
    public string Password { get; set; } = string.Empty;

    [Required]
    public decimal Latitude { get; set; }

    [Required]
    public decimal Longitude { get; set; }
}
```

#### LoginResponse.cs
```csharp
public class LoginResponse
{
    public string AccessToken { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public int UserId { get; set; }
    public DateTime? PasswordChanged { get; set; }
    public TimeTrackingStatus? TimeTrackingStatus { get; set; }
    public List<string> Functions { get; set; } = new();
    public string AccessLevel { get; set; } = string.Empty;
}
```

#### AuthenticatedUser.cs
```csharp
public class AuthenticatedUser
{
    public int UserId { get; set; }
    public string Username { get; set; } = string.Empty;
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string Picture { get; set; } = string.Empty;
    public DateTime? PasswordChanged { get; set; }
    public List<string> Functions { get; set; } = new();
    public string AccessLevel { get; set; } = string.Empty; // "ADMIN" or "TECH"
}
```

#### JwtTokenResult.cs
```csharp
public class JwtTokenResult
{
    public string Token { get; set; } = string.Empty;
    public string XsrfToken { get; set; } = string.Empty;
    public DateTime Expiration { get; set; }
    public int ExpiresInSeconds { get; set; }
}
```

#### TimeTrackingStatus.cs
```csharp
public class TimeTrackingStatus
{
    public DateTime? LoginTime { get; set; }
    public DateTime? ClockInTime { get; set; }
    public DateTime? CheckInTime { get; set; }
    public DateTime? BreakTime { get; set; }
    public int MinutesWorkedToday { get; set; }
    public decimal HoursWorkedToday => MinutesWorkedToday / 60.0m;
}
```

---

### 2.3 Database Queries

All queries will use parameterized SQL via `DataService` to prevent SQL injection.

#### User Authentication Query
```sql
SELECT
    u.u_id,
    u.u_username,
    u.u_password,
    u.u_firstname,
    u.u_lastname,
    u.u_picture,
    u.u_passwordchanged,
    u.u_active,
    u.u_2fa
FROM [User] u
WHERE u.u_username = @username
  AND u.u_password = @password
  AND u.u_active = 1
  AND (@require2fa = 0 OR u.u_2fa = 1)
```

#### User Functions/Permissions Query
```sql
SELECT DISTINCT
    f.f_function,
    f.f_functionidentifier
FROM [User] u
INNER JOIN xrefUserRole xur ON u.u_id = xur.u_id
INNER JOIN Role r ON r.r_id = xur.r_id
INNER JOIN xrefRoleFunction xrf ON r.r_id = xrf.r_id
INNER JOIN [Function] f ON xrf.f_id = f.f_id
WHERE u.u_username = @username
ORDER BY f.f_function
```

#### Access Level Determination Logic
```csharp
// From functions, determine access level
if (functions.Any(f => f.Contains("ADMIN", StringComparison.OrdinalIgnoreCase)))
    accessLevel = "ADMIN";
else if (functions.Any(f => f.Equals("TECH", StringComparison.OrdinalIgnoreCase)))
    accessLevel = "TECH";
else
    accessLevel = "USER";
```

---

### 2.4 Configuration Updates

#### appsettings.json (EvoAPI)
Add/update the following sections:

```json
{
  "Jwt": {
    "Key": "[COPY FROM EvoWS Web.config KeyForHmacSha256]",
    "Issuer": "EvoWS",
    "Audience": "EvoClient",
    "ExpiryInMinutes": 60,
    "MaxExpiryInMinutes": 1440,
    "ValidateIssuer": false,
    "ValidateAudience": false,
    "ValidateLifetime": true,
    "ClockSkewMinutes": 5
  },
  "Authentication": {
    "SeedLogin": 123456,
    "SeedWeek": 3,
    "CookieSecure": true,
    "CookieSameSite": "None",
    "CookieHttpOnly": false,
    "LoginTrackingExpiryDays": 30,
    "XsrfExpirySeconds": 3600
  },
  "TimeTracking": {
    "LoginTypeId": 1,
    "ClockInTypeId": 2,
    "CheckInTypeId": 3,
    "BreakTypeId": 4
  }
}
```

**CRITICAL:** The `Jwt:Key` value must be exactly the same as EvoWS's `KeyForHmacSha256` for token compatibility.

#### appsettings.secrets.json (Local development)
Add actual key value from EvoWS:
```json
{
  "Jwt": {
    "Key": "[BASE64 OR BYTE ARRAY FROM EVOWS]"
  }
}
```

---

### 2.5 Dependency Injection Registration

**Location:** `src/EvoAPI.Api/Program.cs`

**ONLY ADD THESE THREE SERVICES:**
```csharp
// Add new authentication services (add after existing service registrations)
builder.Services.AddScoped<IAuthenticationService, AuthenticationService>();
builder.Services.AddScoped<IJwtTokenService, JwtTokenService>();
builder.Services.AddScoped<ITimeTrackingService, TimeTrackingService>();
```

**JWT Configuration - ALREADY EXISTS, NO CHANGES NEEDED:**

The JWT authentication is already configured in Program.cs (lines 180-208) with:
- Correct HMAC-SHA256 signing key via `GetEvoWSSigningKey()`
- Cookie-based token extraction from `AccessToken` cookie
- Disabled issuer/audience validation for EvoWS compatibility
- 5-minute clock skew tolerance

**GetEvoWSSigningKey() - ALREADY EXISTS, NO CHANGES NEEDED:**

The signing key method is already implemented (lines 266-273) with the exact key string used by EvoWS. JwtTokenService will reference this same method.

---

## 3. Implementation Flow

### 3.1 Login Request Flow

```
1. Client POST /api/authentication/login
   Body: { username, password, latitude, longitude }

2. AuthenticationController.Login()
   - Validate request model
   - Log audit entry (login attempt)

3. AuthenticationService.ValidateCredentialsAsync()
   - Detect 2FA: Check if password ends with 3 digits
   - If 2FA detected:
     * Calculate secure code via CalculateSecureCodeAsync()
     * Validate last 3 digits match secure code
     * Strip 3 digits from password
     * Set require2fa = true
   - Query database for user with credentials
   - If no match: throw UnauthorizedException
   - If match: extract user details

4. AuthenticationService.GetUserPermissionsAsync()
   - Query user's functions via role joins
   - Determine access level (ADMIN/TECH/USER)
   - Build AuthenticatedUser object

5. JwtTokenService.CreateJwtTokenAsync()
   - Generate XSRF token (Guid.NewGuid().ToString())
   - Build claims list (username, id, firstname, lastname, etc.)
   - Create JWT with HMAC-SHA256 signing
   - Set expiration based on config timeout (max 1440 min)
   - Return JwtTokenResult with token and expiration

6. TimeTrackingService.CreateLoginTrackingAsync()
   - Insert TimeTracking record (ttt_id = 1)
   - Store latitude/longitude in tt_begin_lat/tt_begin_lon
   - Set tt_begin = current server time

7. TimeTrackingService.GetTimeTrackingStatusAsync()
   - Query aggregate today's time tracking
   - Return login time, clock-in time, hours worked

8. AuthenticationController sets HTTP cookies
   - AccessToken: JWT token, expires per config
   - XSRF-TOKEN: CSRF protection token
   - LoginTracking: username|timestamp history (30 days)

9. AuthenticationController returns response
   - Status 200 OK
   - Body: ApiResponse<LoginResponse> with token and user info

10. Log audit entry (login success)
```

### 3.2 Secure Code Calculation (2FA)

**Algorithm (from EvoWS TokenController):**
```csharp
private string CalculateSecureCode()
{
    // Get seed values from appsettings.json
    var seedLogin = _configuration.GetValue<int>("Authentication:SeedLogin");
    var seedWeek = _configuration.GetValue<int>("Authentication:SeedWeek");
    var weekOfYear = GetWeekOfYear(DateTime.Now);

    var calculation = Math.Ceiling((double)seedLogin / (seedWeek * weekOfYear));
    return calculation.ToString().Substring(calculation.ToString().Length - 3);
}

private int GetWeekOfYear(DateTime date)
{
    var culture = System.Globalization.CultureInfo.CurrentCulture;
    var calendar = culture.Calendar;
    var calendarWeekRule = culture.DateTimeFormat.CalendarWeekRule;
    var firstDayOfWeek = culture.DateTimeFormat.FirstDayOfWeek;

    return calendar.GetWeekOfYear(date, calendarWeekRule, firstDayOfWeek);
}
```

**Validation Flow:**
```csharp
if (password.Length >= 3 && int.TryParse(password.Substring(password.Length - 3), out int providedCode))
{
    var expectedCode = await CalculateSecureCodeAsync();
    if (providedCode.ToString() == expectedCode)
    {
        password = password.Substring(0, password.Length - 3);
        require2fa = true;
    }
}
```

### 3.3 Cookie Management

**AccessToken Cookie:**
```csharp
response.Cookies.Append("AccessToken", jwtToken, new CookieOptions
{
    HttpOnly = false,  // JavaScript needs to read it
    Secure = true,     // HTTPS only
    SameSite = SameSiteMode.None,
    Expires = DateTimeOffset.UtcNow.AddSeconds(expiresInSeconds),
    Path = "/"
});
```

**XSRF-TOKEN Cookie:**
```csharp
response.Cookies.Append("XSRF-TOKEN", xsrfToken, new CookieOptions
{
    HttpOnly = false,  // JavaScript needs to read it for header
    Secure = true,
    SameSite = SameSiteMode.None,
    Expires = DateTimeOffset.UtcNow.AddHours(1),
    Path = "/"
});
```

**LoginTracking Cookie:**
```csharp
var loginHistory = GetLoginHistory(username); // Parse existing cookie
loginHistory.Add($"{username}|{DateTime.UtcNow:yyyyMMddHHmmss}");
var trackingValue = string.Join(",", loginHistory.TakeLast(10)); // Keep last 10 logins
if (trackingValue.Length > 500) trackingValue = trackingValue.Substring(0, 500);

response.Cookies.Append("LoginTracking", Uri.EscapeDataString(trackingValue), new CookieOptions
{
    HttpOnly = true,   // Server-side only
    Secure = true,
    SameSite = SameSiteMode.None,
    Expires = DateTimeOffset.UtcNow.AddDays(30),
    Path = "/"
});
```

---

## 4. Security Considerations

### 4.1 Password Security
- **Current State:** Plain text passwords stored in database
- **Decision:** Maintain plain text for backward compatibility
- **Mitigation:**
  - Add password hashing in future phase
  - Consider dual authentication during transition
  - Enforce HTTPS for all authentication requests
  - Implement rate limiting on login endpoint

### 4.2 JWT Token Security
- **Signing Algorithm:** HMAC-SHA256 (secure symmetric encryption)
- **Key Management:**
  - Store key in environment variables/Azure Key Vault (production)
  - Never commit keys to source control
  - Use same key as EvoWS for compatibility
- **Token Expiration:** Configurable timeout (default 60 minutes, max 1440 minutes)
- **Claims Validation:** Validate signature, expiration, and required claims

### 4.3 CSRF Protection
- **XSRF-TOKEN Cookie:** Sent with each request
- **X-XSRF-TOKEN Header:** Client must include in request headers
- **Validation:** Middleware validates cookie matches header (except for login endpoint)
- **Token Rotation:** New XSRF token generated on each login

### 4.4 Cookie Security
- **Secure Flag:** HTTPS only transmission
- **SameSite:** Set to None for cross-site requests (requires Secure flag)
- **HttpOnly:** LoginTracking only (AccessToken and XSRF need JS access)
- **Path:** Root path (/) for application-wide access

### 4.5 Audit Logging
- **All Login Attempts:** Success and failure logged to Audit table
- **Logged Information:**
  - Username, timestamp, IP address, user agent
  - Success/failure status
  - Error details (if failed)
  - Response time
  - Geolocation (latitude/longitude)
- **Audit Review:** Regular review of failed login attempts for security monitoring

### 4.6 Input Validation
- **SQL Injection:** Parameterized queries only
- **XSS:** Sanitize user inputs (username should be alphanumeric)
- **Request Validation:** Data annotations on DTOs
- **Rate Limiting:** Implement throttling on login endpoint to prevent brute force

---

## 5. Testing Strategy

### 5.1 Unit Tests

**AuthenticationService Tests:**
- ✓ Valid credentials return authenticated user
- ✓ Invalid credentials throw UnauthorizedException
- ✓ Inactive users cannot login
- ✓ 2FA detection works correctly
- ✓ Secure code calculation matches expected value
- ✓ User permissions retrieved correctly

**JwtTokenService Tests:**
- ✓ Token generation includes all required claims
- ✓ Token signature validation works
- ✓ Token expiration respected
- ✓ Claims extraction works correctly
- ✓ XSRF token generated as valid GUID

**TimeTrackingService Tests:**
- ✓ Login tracking record created with correct ttt_id
- ✓ Geolocation stored correctly
- ✓ Timestamp captured accurately
- ✓ TimeTrackingStatus aggregation correct

### 5.2 Integration Tests

**Login Endpoint Tests:**
- ✓ POST /api/authentication/login with valid credentials returns 200 OK
- ✓ Response includes AccessToken in cookie and body
- ✓ Response includes XSRF-TOKEN cookie
- ✓ Response includes LoginTracking cookie
- ✓ Invalid credentials return 401 Unauthorized
- ✓ Missing geolocation returns 400 Bad Request
- ✓ Inactive user returns 401 Unauthorized
- ✓ 2FA with correct code succeeds
- ✓ 2FA with incorrect code fails
- ✓ TimeTracking record created in database
- ✓ Audit log entry created

**Token Validation Tests:**
- ✓ EvoAPI-generated token validates in EvoAPI
- ✓ EvoWS-generated token validates in EvoAPI (backward compatibility)
- ✓ EvoAPI-generated token validates in EvoWS (if needed)
- ✓ Expired tokens rejected
- ✓ Tampered tokens rejected
- ✓ Missing XSRF token on protected endpoint returns 403

### 5.3 End-to-End Tests

**User Journey Tests:**
1. User opens login page in evotech frontend
2. User enters valid credentials
3. User allows geolocation access
4. Login request sent to EvoAPI
5. JWT token received and stored
6. User redirected to /board
7. Protected API calls work with token
8. Token validated on each request
9. User logs out successfully
10. Cookies cleared

**Cross-System Tests:**
- ✓ User logs in via EvoAPI, token works for EvoWS endpoints
- ✓ User logs in via EvoWS, token works for EvoAPI endpoints
- ✓ Token claims match between systems

---

## 6. Frontend Integration

### 6.1 Update API Configuration

**File:** `evotech/src/components/api.js`

**Change:** Update login call to use evoApi instead of api

**Before:**
```javascript
const response = await api.post('/token/Login', payload);
```

**After:**
```javascript
const response = await evoApi.post('/authentication/login', payload);
```

### 6.2 Update Login Component

**File:** `evotech/src/pages/login.js`

**Changes:**
- Update endpoint URL from `/token/Login` to `/authentication/login`
- Handle updated response structure (ApiResponse<LoginResponse>)
- Extract token from response.data.data.accessToken
- Store user data from response (firstName, lastName, userId)

**Updated Code:**
```javascript
try {
    const response = await evoApi.post('/authentication/login', payload);
    if (response.status === 200 && response.data.success) {
        const loginData = response.data.data;

        // Store user data
        localStorage.setItem('userData', JSON.stringify({
            u_firstName: loginData.firstName,
            u_lastName: loginData.lastName,
            u_id: loginData.userId
        }));

        login(); // Update UserContext
        router.push('/board');
    } else {
        setErrorMessage(response.data.message || 'Login failed');
    }
} catch (error) {
    // Error handling...
}
```

### 6.3 Environment Variables

**File:** `evotech/.env.local`

Ensure `NEXT_PUBLIC_EVOAPI_URL` points to EvoAPI:
```
NEXT_PUBLIC_API_URL=https://localhost:44393
NEXT_PUBLIC_EVOAPI_URL=https://localhost:44307
```

---

## 7. Deployment Strategy

### Phase 1: Development & Testing (Week 1-2)
1. Implement all services and controllers in EvoAPI
2. Write unit tests for all components
3. Configure local environment with correct JWT key
4. Test login endpoint locally
5. Verify token compatibility between EvoWS and EvoAPI
6. Integration testing

### Phase 2: QA Environment Deployment (Week 3)
1. Deploy EvoAPI to QA environment (evoqa.azurewebsites.net)
2. Update QA appsettings with correct JWT key from EvoWS
3. Configure QA database connection
4. Run integration tests in QA
5. Test frontend integration (evotech calling EvoAPI login)
6. Verify both EvoWS and EvoAPI login work side-by-side

### Phase 3: Canary Deployment (Week 4)
1. Deploy to production but don't switch frontend yet
2. Add feature flag in frontend to switch between EvoWS and EvoAPI login
3. Enable EvoAPI login for internal users only
4. Monitor performance, errors, audit logs
5. Verify TimeTracking records created correctly
6. Test token compatibility in production

### Phase 4: Full Production Rollout (Week 5)
1. Update frontend to use EvoAPI login as default
2. Keep EvoWS login as fallback
3. Monitor error rates, response times, user reports
4. Gradually increase traffic to EvoAPI
5. Once stable, deprecate EvoWS login endpoint

### Phase 5: Cleanup (Week 6+)
1. Remove EvoWS login endpoint (optional - can keep for legacy clients)
2. Update documentation
3. Review security improvements (password hashing roadmap)
4. Performance optimization based on production metrics

---

## 8. Rollback Plan

If critical issues arise during deployment:

### Immediate Rollback (< 5 minutes)
1. Frontend: Update api.js to use `api.post('/token/Login')` instead of evoApi
2. Deploy frontend change immediately
3. All users revert to EvoWS login

### Partial Rollback (Feature Flag)
1. Set feature flag `USE_EVOAPI_LOGIN=false`
2. Gradual switch back to EvoWS
3. Investigate and fix issues in EvoAPI
4. Re-enable when ready

### No Database Changes Required
- No schema migrations needed
- TimeTracking records compatible with existing system
- User table unchanged

---

## 9. Success Metrics

### Performance Metrics
- **Response Time:** < 500ms for 95th percentile
- **Availability:** 99.9% uptime
- **Error Rate:** < 0.1% failed login attempts (excluding bad credentials)

### Functional Metrics
- **Token Compatibility:** 100% of EvoAPI tokens work in EvoWS and vice versa
- **TimeTracking Accuracy:** 100% of logins create TimeTracking records
- **Audit Logging:** 100% of login attempts logged
- **Cookie Management:** 100% of successful logins set all 3 cookies correctly

### Business Metrics
- **User Adoption:** 100% of users can login via EvoAPI without issues
- **Support Tickets:** No increase in login-related support tickets
- **Security Incidents:** No authentication-related security breaches

---

## 10. Future Enhancements

### Short-term (Next 3-6 months)
1. **Password Hashing Migration:**
   - Implement bcrypt or Argon2 password hashing
   - Dual authentication during transition
   - Gradual migration on successful logins

2. **Modern 2FA:**
   - TOTP-based authentication (Google Authenticator, Authy)
   - SMS-based 2FA option
   - Backup codes for account recovery

3. **Rate Limiting:**
   - Implement login attempt throttling
   - IP-based blocking after X failed attempts
   - CAPTCHA after multiple failures

### Medium-term (6-12 months)
1. **OAuth/SSO Integration:**
   - Azure AD integration for enterprise
   - Google/Microsoft SSO options
   - SAML support for enterprise clients

2. **Session Management:**
   - Active session tracking
   - Force logout from all devices
   - Session expiration policies

3. **Security Hardening:**
   - Move JWT keys to Azure Key Vault
   - Implement token rotation
   - Refresh token flow

### Long-term (12+ months)
1. **Passwordless Authentication:**
   - WebAuthn/FIDO2 support
   - Biometric authentication
   - Magic link email authentication

2. **Advanced Audit:**
   - Real-time security monitoring
   - Anomaly detection (unusual login times/locations)
   - Automated alerts for suspicious activity

---

## 11. Key Files Reference

### Files to Create (EvoAPI) - Total: 10 files

**Controllers (1 file):**
| File Path | Purpose |
|-----------|---------|
| `src/EvoAPI.Api/Controllers/AuthenticationController.cs` | Login/logout endpoint handler |

**Services - Interfaces (3 files):**
| File Path | Purpose |
|-----------|---------|
| `src/EvoAPI.Core/Interfaces/IAuthenticationService.cs` | Auth service interface |
| `src/EvoAPI.Core/Interfaces/IJwtTokenService.cs` | JWT service interface |
| `src/EvoAPI.Core/Interfaces/ITimeTrackingService.cs` | TimeTracking interface |

**Services - Implementations (3 files):**
| File Path | Purpose |
|-----------|---------|
| `src/EvoAPI.Core/Services/AuthenticationService.cs` | Credential validation logic |
| `src/EvoAPI.Core/Services/JwtTokenService.cs` | JWT token generation |
| `src/EvoAPI.Core/Services/TimeTrackingService.cs` | TimeTracking record creation |

**DTOs (5 files in Authentication folder):**
| File Path | Purpose |
|-----------|---------|
| `src/EvoAPI.Shared/DTOs/Authentication/LoginRequest.cs` | Login request DTO |
| `src/EvoAPI.Shared/DTOs/Authentication/LoginResponse.cs` | Login response DTO |
| `src/EvoAPI.Shared/DTOs/Authentication/AuthenticatedUser.cs` | Authenticated user model |
| `src/EvoAPI.Shared/DTOs/Authentication/JwtTokenResult.cs` | JWT result model |
| `src/EvoAPI.Shared/DTOs/Authentication/TimeTrackingStatus.cs` | Time tracking status DTO |

**NOTE:** ConfigurationService eliminated - using existing `IDataService.GetConfigSettingAsync()` instead

### Files to Modify (EvoAPI) - Total: 2 files

| File Path | Changes |
|-----------|---------|
| `src/EvoAPI.Api/Program.cs` | Register 3 new services (Authentication, JwtToken, TimeTracking) |
| `src/EvoAPI.Api/appsettings.json` | Add Authentication section with SeedLogin/SeedWeek |

**NOTE:** No changes needed to existing JWT configuration in Program.cs - it already has the correct setup

### Files to Modify (Frontend)

| File Path | Changes |
|-----------|---------|
| `evotech/src/pages/login.js` | Update login endpoint call |
| `evotech/src/components/api.js` | Update API URL (already has evoApi instance) |
| `evotech/.env.local` | Ensure NEXT_PUBLIC_EVOAPI_URL is set |

---

## 12. Dependencies & Prerequisites

### Development Prerequisites
- ✓ Access to EvoWS Web.config to retrieve `KeyForHmacSha256`
- ✓ Access to EVO database for testing
- ✓ .NET 8 SDK installed
- ✓ Visual Studio 2022 or VS Code with C# extension
- ✓ SQL Server Management Studio or Azure Data Studio

### Database Prerequisites
- ✓ Existing tables: `[User]`, `TimeTracking`, `Audit`, `xrefUserRole`, `xrefRoleFunction`, `Function`
- ✓ No schema changes required
- ✓ Database connection from EvoAPI to Azure SQL

### Configuration Prerequisites
- ✓ JWT signing key from EvoWS (must match exactly)
- ✓ Database connection string
- ✓ CORS origins configured
- ✓ Environment variables for secrets

### Testing Prerequisites
- ✓ Test user accounts in database
- ✓ QA environment for integration testing
- ✓ Postman or similar for API testing
- ✓ Browser DevTools for frontend testing

---

## 13. Risk Assessment

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| JWT key mismatch breaks token compatibility | HIGH | LOW | Verify key before deployment, integration tests |
| Password plain text security concern | HIGH | N/A | Document as technical debt, plan future hashing migration |
| TimeTracking record creation fails | MEDIUM | LOW | Wrap in try-catch, log errors but don't block login |
| Cookie settings incompatible with browser | MEDIUM | LOW | Test across browsers, verify SameSite=None + Secure |
| Database connection timeout on login | HIGH | LOW | Implement connection pooling, retry logic |
| Geolocation permission denied | LOW | MEDIUM | Clear error message, allow retry |
| Audit logging failure impacts login | MEDIUM | LOW | Make audit non-blocking, log failures separately |
| EvoWS and EvoAPI tokens not interoperable | HIGH | LOW | Thorough compatibility testing before rollout |
| 2FA secure code calculation differs | MEDIUM | LOW | Unit test against known values, verify algorithm match |
| XSRF validation breaks existing API calls | MEDIUM | LOW | Ensure frontend sends X-XSRF-TOKEN header |

---

## 14. Support & Documentation

### Documentation to Create
1. **API Documentation:** Update Swagger/OpenAPI with new login endpoint
2. **Developer Guide:** How to test login locally
3. **Deployment Guide:** Step-by-step production deployment
4. **Troubleshooting Guide:** Common issues and resolutions
5. **Security Guide:** Authentication best practices

### Training Required
1. Development team: EvoAPI architecture and patterns
2. QA team: New test scenarios for login endpoint
3. Support team: Login troubleshooting (cookie issues, 2FA, etc.)
4. DevOps team: Deployment process and rollback procedures

### Monitoring & Alerts
1. **Application Insights:** Track login success/failure rates
2. **Azure Monitor:** Database connection health
3. **Custom Alerts:**
   - Login error rate > 5%
   - Response time > 1 second
   - Database connection failures
   - Audit logging failures

---

## 15. Conclusion

This migration plan provides a comprehensive roadmap for moving the login authentication system from the legacy EvoWS to the modern EvoAPI while maintaining full backward compatibility. The approach prioritizes stability and gradual rollout, with clear rollback procedures and success metrics.

**Key Benefits:**
- Modern ASP.NET Core architecture
- Clean separation of concerns (Controllers → Services → Data)
- Maintained backward compatibility with existing tokens
- Comprehensive audit logging and monitoring
- Foundation for future security enhancements (password hashing, modern 2FA, OAuth)

**Next Steps:**
1. Review and approve this plan
2. Set up development environment with correct JWT key
3. Begin implementation starting with DTOs and interfaces
4. Develop services layer (Authentication, JWT, TimeTracking)
5. Implement controller and test thoroughly
6. Frontend integration and testing
7. QA deployment and validation
8. Production rollout with monitoring

**Implementation Scope:**
- **New Files:** 10 (down from original 14)
- **Modified Files:** 2
- **Reused Services:** DataService, AuditService, existing JWT config

**Timeline:** Estimated 4-5 weeks from development to full production rollout (reduced due to leveraging existing infrastructure).

**Effort:** ~60-80 hours of development, testing, and deployment work (reduced from original 80-100 hours).
