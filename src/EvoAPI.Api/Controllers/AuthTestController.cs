using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using EvoAPI.Shared.Attributes;
using EvoAPI.Core.Interfaces;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("[controller]")]
public class AuthTestController : BaseController
{
    public AuthTestController(IAuditService auditService)
    {
        InitializeAuditService(auditService);
    }
    [HttpGet("public/status")]
    [AllowAnonymous]
    public IActionResult GetStatus()
    {
        return Ok(new { 
            Status = "API is running", 
            Timestamp = DateTime.UtcNow,
            Environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production"
        });
    }

    [HttpGet("public/diagnostics")]
    [AllowAnonymous]
    public IActionResult GetDiagnostics()
    {
        try
        {
            var config = HttpContext.RequestServices.GetRequiredService<IConfiguration>();
            var connectionString = config.GetConnectionString("DefaultConnection");
            
            return Ok(new
            {
                Status = "Diagnostics running",
                Environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production",
                HasConnectionString = !string.IsNullOrEmpty(connectionString),
                ConnectionStringLength = connectionString?.Length ?? 0,
                HasPassword = connectionString?.Contains("Password=") ?? false,
                HasPlaceholder = connectionString?.Contains("{DB_PASSWORD}") ?? false,
                Timestamp = DateTime.UtcNow,
                MachineName = Environment.MachineName,
                ProcessId = Environment.ProcessId
            });
        }
        catch (Exception ex)
        {
            return Ok(new
            {
                Status = "Error in diagnostics",
                Error = ex.Message,
                StackTrace = ex.StackTrace,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpGet("profile")]
    public IActionResult GetProfile()
    {
        return Ok(new
        {
            UserId,
            Username,
            AccessLevel,
            IsAdmin,
            UserFullName,
            IsLoggedIn
        });
    }

    [HttpGet("admin/users")]
    [AdminOnly]
    public async Task<IActionResult> GetAllUsers()
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            #region Initialize & Audit Entry
            await LogAuditAsync("GetAllUsers", new { RequestedBy = Username });
            #endregion
            
            #region Admin Permission Check
            if (!IsAdmin)
            {
                await LogAuditErrorAsync("GetAllUsers", 
                    new UnauthorizedAccessException("Admin access required"), 
                    new { Username, AccessLevel });
                return Forbid("Admin access required");
            }
            #endregion
            
            #region Business Logic - Retrieve Users
            // Simulate user retrieval logic
            await Task.Delay(50); // Simulate database call
            
            var users = new[]
            {
                new { Id = 1, Username = "admin", AccessLevel = "ADMIN", Name = "System Administrator" },
                new { Id = 2, Username = "user1", AccessLevel = "USER", Name = "Regular User" },
                new { Id = 3, Username = "manager", AccessLevel = "MANAGER", Name = "Department Manager" }
            };
            
            stopwatch.Stop();
            await LogAuditAsync("GetAllUsers - Data Retrieved", 
                new { UserCount = users.Length }, 
                stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
            #endregion
            
            #region Return Response
            var response = new
            {
                Message = "Users retrieved successfully",
                RequestedBy = new
                {
                    UserId,
                    Username,
                    AccessLevel
                },
                Users = users,
                TotalCount = users.Length,
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00") + "s"
            };
            
            await LogAuditAsync("GetAllUsers - Response Sent", new { TotalUsers = users.Length, ResponseTimeMs = stopwatch.ElapsedMilliseconds });
            
            return Ok(response);
            #endregion
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetAllUsers", ex, new { Username, RequestTime = DateTime.UtcNow });
            return StatusCode(500, new { Error = "Internal server error", RequestId = Guid.NewGuid() });
        }
    }

    [HttpGet("test-auth")]
    public IActionResult TestAuth()
    {
        return Ok(new
        {
            Message = "Authentication successful",
            Claims = User.Claims.Select(c => new { c.Type, c.Value }).ToList(),
            ServerTime = DateTime.UtcNow,
            ClientIP = ClientIPAddress,
            UserAgent
        });
    }

    [HttpGet("test-database")]
    [AllowAnonymous]
    public async Task<IActionResult> TestDatabase()
    {
        try
        {
            var config = HttpContext.RequestServices.GetRequiredService<IConfiguration>();
            var connectionString = config.GetConnectionString("DefaultConnection");
            
            using var connection = new System.Data.SqlClient.SqlConnection(connectionString);
            await connection.OpenAsync();
            
            using var command = new System.Data.SqlClient.SqlCommand("SELECT COUNT(*) FROM servicerequest", connection);
            var count = await command.ExecuteScalarAsync();
            
            return Ok(new
            {
                Message = "Database connection successful",
                ServiceRequestCount = count,
                ConnectionString = connectionString?.Replace("Password=", "Password=***"),
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            return Ok(new
            {
                Message = "Database connection failed",
                Error = ex.Message,
                StackTrace = ex.StackTrace,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpGet("recent-audit-logs")]
    [AdminOnly]
    public async Task<IActionResult> GetRecentAuditLogs()
    {
        try
        {
            // Simple query to check recent audit entries - this will help debug the work orders issue
            var sql = @"
                SELECT TOP 10 
                    a_timestamp, 
                    a_name, 
                    a_description, 
                    a_detail,
                    a_responsetime
                FROM audit 
                ORDER BY a_timestamp DESC";
            
            var connectionString = HttpContext.RequestServices
                .GetRequiredService<IConfiguration>()
                .GetConnectionString("DefaultConnection");
            
            using var connection = new System.Data.SqlClient.SqlConnection(connectionString);
            using var command = new System.Data.SqlClient.SqlCommand(sql, connection);
            
            await connection.OpenAsync();
            using var reader = await command.ExecuteReaderAsync();
            
            var logs = new List<object>();
            while (await reader.ReadAsync())
            {
                logs.Add(new
                {
                    Timestamp = reader["a_timestamp"],
                    Name = reader["a_name"]?.ToString(),
                    Description = reader["a_description"]?.ToString(),
                    Detail = reader["a_detail"]?.ToString(),
                    ResponseTime = reader["a_responsetime"]?.ToString()
                });
            }
            
            return Ok(new { Message = "Recent audit logs", Logs = logs });
        }
        catch (Exception ex)
        {
            return Ok(new { Message = "Error reading audit logs", Error = ex.Message });
        }
    }

    [HttpGet("debug-token")]
    [AllowAnonymous]
    public IActionResult DebugToken()
    {
        var token = Request.Cookies["AccessToken"];
        
        if (string.IsNullOrEmpty(token))
        {
            return Ok(new { Message = "No AccessToken cookie found" });
        }

        try
        {
            // Decode JWT without validation to see its structure
            var parts = token.Split('.');
            if (parts.Length != 3)
            {
                return Ok(new { Message = "Invalid JWT format", Token = token });
            }

            var header = DecodeBase64(parts[0]);
            var payload = DecodeBase64(parts[1]);

            return Ok(new 
            { 
                Message = "Token found and decoded",
                Header = header,
                Payload = payload,
                TokenLength = token.Length
            });
        }
        catch (Exception ex)
        {
            return Ok(new { Message = "Error decoding token", Error = ex.Message, Token = token });
        }
    }

    private string DecodeBase64(string base64)
    {
        try
        {
            // Add padding if needed
            var padded = base64.Length % 4 == 0 ? base64 : base64 + new string('=', 4 - base64.Length % 4);
            var bytes = Convert.FromBase64String(padded);
            return System.Text.Encoding.UTF8.GetString(bytes);
        }
        catch
        {
            return "Unable to decode";
        }
    }
}
