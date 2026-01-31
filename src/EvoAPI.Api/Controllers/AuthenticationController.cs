using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.DTOs.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
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

            // Handle 2FA: Check if password ends with 3 digits
            var password = request.Password;
            var require2fa = false;

            if (password.Length >= 3 && int.TryParse(password.Substring(password.Length - 3), out int providedCode))
            {
                var expectedCode = _authenticationService.CalculateSecureCode();
                if (providedCode.ToString() == expectedCode)
                {
                    password = password.Substring(0, password.Length - 3);
                    require2fa = true;
                    _logger.LogInformation("2FA code validated for user: {Username}", request.Username);
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
            // Read AccessToken from cookie
            if (!Request.Cookies.TryGetValue("AccessToken", out var token))
            {
                return Unauthorized(new { message = "AccessToken cookie not found" });
            }

            // Validate and decode the token
            var principal = _jwtTokenService.ValidateJwtToken(token);
            if (principal == null)
            {
                return Unauthorized(new { message = "Invalid token" });
            }

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
