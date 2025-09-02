using Microsoft.AspNetCore.Mvc;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.Models;
using EvoAPI.Core.Interfaces;
using System.Security.Claims;
using System.Text.Json;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    [EvoAuthorize] // Apply authentication to all controllers by default
    public abstract class BaseController : ControllerBase
    {
        protected IAuditService? _auditService;
        
        // Initialize audit service through DI
        protected void InitializeAuditService(IAuditService auditService)
        {
            _auditService = auditService;
        }


        /// Gets the current user's ID from JWT claims

        protected int UserId => GetClaimValue<int>("id");
        

        /// Gets the current user's username from JWT claims (try both 'username' and 'unique_name')

        protected string Username => GetClaimValue<string>("username") ?? GetClaimValue<string>("unique_name") ?? string.Empty;
        

        /// Gets the current user's access level from JWT claims (checks if ADMIN is in accesslevel array)

        protected string AccessLevel => IsAdmin ? "ADMIN" : "USER";
        

        /// Checks if the current user is an admin (looks for ADMIN in accesslevel claims)

        protected bool IsAdmin => User.FindAll("accesslevel").Any(c => c.Value == "ADMIN");
        

        /// Gets the current user's full name from JWT claims

        protected string UserFullName => $"{GetClaimValue<string>("firstname") ?? ""} {GetClaimValue<string>("lastname") ?? ""}".Trim();
        

        /// Gets the XSRF token from JWT claims

        protected string XsrfToken => GetClaimValue<string>("XSRF-TOKEN") ?? string.Empty;
        

        /// Helper method to extract claim values from JWT token

        /// <typeparam name="T">Type to convert the claim value to</typeparam>
        /// <param name="claimType">The claim type to retrieve</param>
        /// <returns>The claim value converted to type T, or default(T) if not found</returns>
        protected T GetClaimValue<T>(string claimType)
        {
            var claim = User.FindFirst(claimType)?.Value;
            if (claim == null) return default(T)!;
            
            try
            {
                return (T)Convert.ChangeType(claim, typeof(T));
            }
            catch
            {
                return default(T)!;
            }
        }
        

        /// Checks if user is logged in (authenticated)

        protected bool IsLoggedIn => User.Identity?.IsAuthenticated == true && UserId > 0;
        

        /// Gets the client IP address

        protected string ClientIPAddress => 
            HttpContext.Connection.RemoteIpAddress?.ToString() ?? "Unknown";
        

        /// Gets the user agent string

        protected string UserAgent => 
            HttpContext.Request.Headers["User-Agent"].ToString();

        #region Manual Audit Logging Methods (like EvoWS)
        

        /// Log a custom audit entry (like EvoWS pattern)

        protected async Task LogAuditAsync(string description, object? detail = null, string? responseTime = null)
        {
            if (_auditService == null) return;
            
            var auditEntry = CreateAuditEntry(description, detail, responseTime);
            await _auditService.LogAsync(auditEntry);
        }
        

        /// Log an error audit entry (like EvoWS pattern)

        protected async Task LogAuditErrorAsync(string description, Exception ex, object? detail = null)
        {
            if (_auditService == null) return;
            
            var auditEntry = CreateAuditEntry(description, detail);
            auditEntry.Detail = ex.ToString();
            auditEntry.IsError = true;
            
            await _auditService.LogErrorAsync(auditEntry);
        }
        

        /// Create a standard audit entry with current user context

        private AuditEntry CreateAuditEntry(string description, object? detail = null, string? responseTime = null)
        {
            var detailString = "";
            if (detail != null)
            {
                try
                {
                    detailString = JsonSerializer.Serialize(detail);
                }
                catch
                {
                    detailString = detail.ToString() ?? "";
                }
            }
            
            return new AuditEntry
            {
                Username = Username,
                Name = UserFullName,
                Description = $"Web Service - EvoAPI - {description}",
                Detail = detailString,
                ResponseTime = responseTime ?? "0.00",
                IPAddress = ClientIPAddress,
                UserAgent = UserAgent,
                MachineName = Environment.MachineName,
                IsError = false
            };
        }
        

        /// Execute an action with automatic audit logging (like EvoWS pattern)

        protected async Task<T> ExecuteWithAuditAsync<T>(string methodName, object? requestData, Func<Task<T>> action)
        {
            var stopwatch = Stopwatch.StartNew();
            
            try
            {
                await LogAuditAsync(methodName, requestData);
                
                var result = await action();
                
                stopwatch.Stop();
                await LogAuditAsync($"{methodName} - Completed", null, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
                
                return result;
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync(methodName, ex, requestData);
                throw;
            }
        }
        
        #endregion
    }
}
