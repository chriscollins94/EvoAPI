using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.DTOs.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("EvoApi/authentication")]
public class AuthenticationController : BaseController
{
    private readonly IAuthenticationService _authenticationService;
    private readonly IJwtTokenService _jwtTokenService;
    private readonly ITimeTrackingService _timeTrackingService;
    private readonly IConfiguration _configuration;
    private readonly ILogger<AuthenticationController> _logger;

    public AuthenticationController(
        IAuditService auditService,
        IAuthenticationService authenticationService,
        IJwtTokenService jwtTokenService,
        ITimeTrackingService timeTrackingService,
        IConfiguration configuration,
        ILogger<AuthenticationController> logger)
    {
        InitializeAuditService(auditService);
        _authenticationService = authenticationService;
        _jwtTokenService = jwtTokenService;
        _timeTrackingService = timeTrackingService;
        _configuration = configuration;
        _logger = logger;
    }

    [HttpPost("login")]
    [AllowAnonymous]
    public async Task<ActionResult<ApiResponse<LoginResponse>>> Login([FromBody] LoginRequest request)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Fire-and-forget audit logging (don't block login response)
            _ = LogAuditAsync("Login Attempt", $"Username: {request.Username}");

            // First, check if user requires 2FA
            var userCheckSql = @"SELECT u_2fa FROM [User] WHERE u_username = @username";
            var userCheckParams = new Dictionary<string, object> { { "@username", request.Username } };
            var dataService = HttpContext.RequestServices.GetRequiredService<IDataService>();
            var userCheckResult = await dataService.ExecuteQueryAsync(userCheckSql, userCheckParams);
            
            var userRequires2fa = false;
            if (userCheckResult.Rows.Count > 0)
            {
                userRequires2fa = Convert.ToInt32(userCheckResult.Rows[0]["u_2fa"]) == 1;
            }

            // Weekly2faCode feature flag (ConfigSetting cs_type='featureflag'): when off,
            // skip the 3-digit code handling entirely — the password is used as typed.
            if (userRequires2fa && !await _authenticationService.IsWeekly2faCodeEnabledAsync())
            {
                _logger.LogInformation("Weekly2faCode feature flag is off - skipping 2FA code for user: {Username}", request.Username);
                userRequires2fa = false;
            }

            // Handle 2FA: Only process 3-digit code if user requires 2FA
            var password = request.Password;
            var require2fa = false;

            // Compare the trailing 3 characters as a string - the expected code is zero-padded
            // (e.g. "060"), so parsing to an int here would drop the leading zero and never match.
            var providedCode = password.Length >= 3 ? password.Substring(password.Length - 3) : null;

            if (userRequires2fa && providedCode != null && providedCode.All(char.IsDigit))
            {
                // User requires 2FA and password ends with 3 digits
                var expectedCode = _authenticationService.CalculateSecureCode();
                if (providedCode == expectedCode)
                {
                    // Valid 2FA code - strip it from password
                    password = password.Substring(0, password.Length - 3);
                    require2fa = true;
                    _logger.LogInformation("2FA code validated for user: {Username}", request.Username);
                }
                else
                {
                    // Invalid 2FA code - reject immediately
                    _logger.LogWarning("Invalid 2FA code provided for user: {Username}. Provided: {ProvidedCode}, Expected: {ExpectedCode}", 
                        request.Username, providedCode, expectedCode);
                    stopwatch.Stop();
                    _ = LogAuditErrorAsync("Login Failed - Invalid 2FA Code", 
                        new Exception($"Username: {request.Username}, Provided Code: {providedCode}, Expected Code: {expectedCode}"));
                    
                    return Unauthorized(new ApiResponse<LoginResponse>
                    {
                        Success = false,
                        Message = "Invalid security code",
                        Data = null,
                        Count = 0,
                        Timestamp = DateTime.UtcNow
                    });
                }
            }

            // Validate credentials
            var user = await _authenticationService.ValidateCredentialsAsync(request.Username, password, require2fa);

            // Generate JWT token
            var timeout = _configuration.GetValue<int>("Jwt:ExpiryInMinutes", 60);
            var xsrfToken = Guid.NewGuid().ToString();
            var tokenResult = _jwtTokenService.CreateJwtToken(user, xsrfToken, timeout);

            // Create TimeTracking record
            await _timeTrackingService.CreateLoginTrackingAsync(user.UserId, request.Latitude, request.Longitude);

            // Get TimeTracking status
            var timeTrackingStatus = await _timeTrackingService.GetTimeTrackingStatusAsync(user.UserId);

            // Set cookies
            SetAuthenticationCookies(tokenResult.Token, xsrfToken, request.Username, tokenResult.ExpiresInSeconds);

            stopwatch.Stop();
            _ = LogAuditAsync("Login Success", $"User: {request.Username}, UserId: {user.UserId}");

            return Ok(new ApiResponse<LoginResponse>
            {
                Success = true,
                Message = "Login successful",
                Data = new LoginResponse
                {
                    AccessToken = tokenResult.Token,
                    Username = user.Username,
                    FirstName = user.FirstName,
                    LastName = user.LastName,
                    UserId = user.UserId,
                    PasswordChanged = user.PasswordChanged,
                    TimeTrackingStatus = timeTrackingStatus,
                    Functions = user.Functions,
                    AccessLevel = user.AccessLevel
                },
                Count = 1,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (UnauthorizedAccessException ex)
        {
            stopwatch.Stop();
            _ = LogAuditErrorAsync("Login Failed", new Exception($"Username: {request.Username}, Error: {ex.Message}"));

            return Unauthorized(new ApiResponse<LoginResponse>
            {
                Success = false,
                Message = "Invalid username or password",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Login error for user: {Username}", request.Username);
            _ = LogAuditErrorAsync("Login Error", ex);

            return StatusCode(500, new ApiResponse<LoginResponse>
            {
                Success = false,
                Message = "An error occurred during login",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpGet("getusertoken")]
    [AllowAnonymous]
    public ActionResult<object> GetUserToken()
    {
        try
        {
            _logger.LogInformation("GetUserToken called - checking for AccessToken cookie");
            
            // Read AccessToken from cookie
            if (!Request.Cookies.TryGetValue("AccessToken", out var token))
            {
                _logger.LogWarning("AccessToken cookie not found");
                return Unauthorized(new { message = "AccessToken cookie not found" });
            }

            _logger.LogInformation("AccessToken cookie found, validating token");
            
            // Validate and decode the token
            var principal = _jwtTokenService.ValidateJwtToken(token);
            if (principal == null)
            {
                _logger.LogWarning("Token validation failed");
                return Unauthorized(new { message = "Invalid token" });
            }
            
            _logger.LogInformation("Token validated successfully");

            // Helper function to get first claim value or empty string
            string GetClaimValue(string claimType)
            {
                var claim = principal.Claims.FirstOrDefault(c => c.Type == claimType);
                return claim?.Value ?? "";
            }

            // Extract multi-value claims
            var functionClaims = principal.Claims.Where(c => c.Type == "function").Select(c => c.Value).ToList();
            var accessLevelClaims = principal.Claims.Where(c => c.Type == "accesslevel").Select(c => c.Value).ToList();

            var tokenData = new
            {
                nbf = GetClaimValue("nbf"),
                exp = GetClaimValue("exp"),
                unique_name = GetClaimValue(System.Security.Claims.ClaimTypes.Name),
                username = GetClaimValue("username"),
                firstname = GetClaimValue("firstname"),
                lastname = GetClaimValue("lastname"),
                passwordchanged = GetClaimValue("passwordchanged"),
                picture = GetClaimValue("picture"),
                id = GetClaimValue("id"),
                xsrfToken = GetClaimValue("XSRF-TOKEN"),
                function = functionClaims,
                accesslevel = accessLevelClaims
            };

            return Ok(tokenData);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error decoding token");
            return StatusCode(500, new { message = $"Error decoding token: {ex.Message}" });
        }
    }

    /// <summary>
    /// Reissues a fresh JWT (full standard lifetime) for the current, still-valid session,
    /// letting the user extend their session without re-entering credentials or reloading the page.
    /// Requires a valid token, so an already-expired session must log in again.
    /// </summary>
    [HttpPost("refresh")]
    [Authorize]
    public ActionResult<ApiResponse<object>> Refresh()
    {
        try
        {
            // Rebuild the AuthenticatedUser from the current token's claims. Reusing the claims
            // keeps roles consistent with the active session and avoids a DB round-trip; it carries
            // exactly the same authorization the user already holds until their next real login.
            DateTime? passwordChanged = null;
            var passwordChangedClaim = GetClaimValue<string>("passwordchanged");
            if (!string.IsNullOrEmpty(passwordChangedClaim) &&
                DateTime.TryParse(passwordChangedClaim, null,
                    System.Globalization.DateTimeStyles.RoundtripKind, out var parsedPwdChanged))
            {
                passwordChanged = parsedPwdChanged;
            }

            var user = new AuthenticatedUser
            {
                UserId = UserId,
                Username = Username,
                FirstName = GetClaimValue<string>("firstname") ?? string.Empty,
                LastName = GetClaimValue<string>("lastname") ?? string.Empty,
                Picture = GetClaimValue<string>("picture") ?? string.Empty,
                PasswordChanged = passwordChanged,
                Functions = User.FindAll("function").Select(c => c.Value).ToList()
            };

            if (user.UserId <= 0 || string.IsNullOrEmpty(user.Username))
            {
                return Unauthorized(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Invalid session",
                    Data = null,
                    Count = 0,
                    Timestamp = DateTime.UtcNow
                });
            }

            // Reuse the existing XSRF token so any client-cached value remains valid.
            var xsrfToken = string.IsNullOrEmpty(XsrfToken) ? Guid.NewGuid().ToString() : XsrfToken;

            var timeout = _configuration.GetValue<int>("Jwt:ExpiryInMinutes", 60);
            var tokenResult = _jwtTokenService.CreateJwtToken(user, xsrfToken, timeout);

            SetAuthenticationCookies(tokenResult.Token, xsrfToken, user.Username, tokenResult.ExpiresInSeconds);

            _ = LogAuditAsync("Token Refresh", $"User: {user.Username}, UserId: {user.UserId}");

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Session extended",
                Data = new
                {
                    accessToken = tokenResult.Token,
                    expiresInSeconds = tokenResult.ExpiresInSeconds
                },
                Count = 1,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Token refresh error");
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while extending the session",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpPost("changepassword")]
    [Authorize]
    public async Task<ActionResult<ApiResponse<object>>> ChangePassword([FromBody] ChangePasswordRequest request)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            _ = LogAuditAsync("Password Change Attempt", $"UserId: {UserId}");

            await _authenticationService.ChangePasswordAsync(UserId, request.CurrentPassword, request.NewPassword);

            // Clear auth cookies so the client is forced to log in again with the new password
            // and receive a fresh JWT containing the updated passwordchanged claim.
            ClearAuthenticationCookies();

            stopwatch.Stop();
            _ = LogAuditAsync("Password Change Success", $"UserId: {UserId}");

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Password changed successfully",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (UnauthorizedAccessException ex)
        {
            stopwatch.Stop();
            _ = LogAuditErrorAsync("Password Change Failed", new Exception($"UserId: {UserId}, Error: {ex.Message}"));

            return Unauthorized(new ApiResponse<object>
            {
                Success = false,
                Message = ex.Message,
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (ArgumentException ex)
        {
            stopwatch.Stop();
            return BadRequest(new ApiResponse<object>
            {
                Success = false,
                Message = ex.Message,
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Password change error for user id: {UserId}", UserId);
            _ = LogAuditErrorAsync("Password Change Error", ex);

            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while changing the password",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpPost("logout")]
    [Authorize]
    public async Task<ActionResult<ApiResponse<object>>> Logout([FromBody] LoginRequest? request)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            _ = LogAuditAsync("Logout Attempt", $"User: {Username}");

            // Update TimeTracking record with logout time
            if (request != null)
            {
                await _timeTrackingService.CreateLogoutTrackingAsync(UserId, request.Latitude, request.Longitude);
            }

            // Clear cookies
            ClearAuthenticationCookies();

            stopwatch.Stop();
            _ = LogAuditAsync("Logout Success", $"User: {Username}");

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Logout successful",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Logout error for user: {Username}", Username);
            _ = LogAuditErrorAsync("Logout Error", ex);

            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred during logout",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    private void SetAuthenticationCookies(string jwtToken, string xsrfToken, string username, int expiresInSeconds)
    {
        var cookieSecure = _configuration.GetValue<bool>("Authentication:CookieSecure", true);
        var cookieSameSite = _configuration.GetValue<string>("Authentication:CookieSameSite", "None");
        var xsrfExpiry = _configuration.GetValue<int>("Authentication:XsrfExpirySeconds", 3600);
        var loginTrackingExpiryDays = _configuration.GetValue<int>("Authentication:LoginTrackingExpiryDays", 30);

        // AccessToken cookie
        Response.Cookies.Append("AccessToken", jwtToken, new CookieOptions
        {
            HttpOnly = false,  // JavaScript needs to read it
            Secure = cookieSecure,
            SameSite = ParseSameSiteMode(cookieSameSite),
            Expires = DateTimeOffset.UtcNow.AddSeconds(expiresInSeconds),
            Path = "/"
        });

        // XSRF-TOKEN cookie
        Response.Cookies.Append("XSRF-TOKEN", xsrfToken, new CookieOptions
        {
            HttpOnly = false,  // JavaScript needs to read it for header
            Secure = cookieSecure,
            SameSite = ParseSameSiteMode(cookieSameSite),
            Expires = DateTimeOffset.UtcNow.AddSeconds(xsrfExpiry),
            Path = "/"
        });

        // LoginTracking cookie (simplified version)
        var trackingValue = $"{username}|{DateTime.UtcNow:yyyyMMddHHmmss}";
        Response.Cookies.Append("LoginTracking", Uri.EscapeDataString(trackingValue), new CookieOptions
        {
            HttpOnly = true,   // Server-side only
            Secure = cookieSecure,
            SameSite = ParseSameSiteMode(cookieSameSite),
            Expires = DateTimeOffset.UtcNow.AddDays(loginTrackingExpiryDays),
            Path = "/"
        });

        _logger.LogInformation("Authentication cookies set for user: {Username}", username);
    }

    private void ClearAuthenticationCookies()
    {
        Response.Cookies.Delete("AccessToken");
        Response.Cookies.Delete("XSRF-TOKEN");
        Response.Cookies.Delete("LoginTracking");

        _logger.LogInformation("Authentication cookies cleared");
    }

    private SameSiteMode ParseSameSiteMode(string sameSite)
    {
        return sameSite.ToLower() switch
        {
            "none" => SameSiteMode.None,
            "lax" => SameSiteMode.Lax,
            "strict" => SameSiteMode.Strict,
            _ => SameSiteMode.None
        };
    }
}
