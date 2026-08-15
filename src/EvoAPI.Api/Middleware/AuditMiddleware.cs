using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Models;
using System.Diagnostics;
using System.Text;

namespace EvoAPI.Api.Middleware;

public class AuditMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<AuditMiddleware> _logger;

    public AuditMiddleware(RequestDelegate next, ILogger<AuditMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context, IAuditService auditService)
    {
        // Skip audit logging for non-API requests for performance
        var path = context.Request.Path.Value?.ToLower();
        if (path != null && ShouldSkipAudit(path))
        {
            await _next(context);
            return;
        }

        var stopwatch = Stopwatch.StartNew();

        Exception? exception = null;
        string requestContent = "";
        string responseContent = "";

        // Buffer the response body so it can be captured for error details
        var originalResponseBody = context.Response.Body;
        using var responseBuffer = new MemoryStream();
        context.Response.Body = responseBuffer;

        try
        {
            // Only capture request content for API endpoints (not for static files)
            if ((context.Request.Method == "POST" || context.Request.Method == "PUT") &&
                path != null && path.StartsWith("/api/"))
            {
                requestContent = await CaptureRequestContent(context);
            }

            await _next(context);
        }
        catch (Exception ex)
        {
            exception = ex;
            throw;
        }
        finally
        {
            stopwatch.Stop();

            // Restore the original stream and flush the buffered response to the client
            context.Response.Body = originalResponseBody;
            try
            {
                if (context.Response.StatusCode >= 400)
                {
                    responseContent = ReadResponseContent(responseBuffer);
                }

                responseBuffer.Position = 0;
                await responseBuffer.CopyToAsync(originalResponseBody);
            }
            catch (Exception copyEx)
            {
                _logger.LogError(copyEx, "Failed to write buffered response to client");
            }

            try
            {
                await LogAuditEntry(context, auditService, stopwatch.Elapsed, exception, requestContent, responseContent);
            }
            catch (Exception logEx)
            {
                _logger.LogError(logEx, "Failed to log audit entry");
            }
        }
    }

    private string ReadResponseContent(MemoryStream responseBuffer)
    {
        try
        {
            responseBuffer.Position = 0;
            using var reader = new StreamReader(responseBuffer, Encoding.UTF8, leaveOpen: true);
            var content = reader.ReadToEnd();

            if (content.Length > 5000)
            {
                content = content.Substring(0, 5000) + "... [truncated]";
            }

            return content;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to capture response content");
            return "";
        }
    }

    private static bool ShouldSkipAudit(string path)
    {
        return path.Contains("/health") || 
               path.Contains("/swagger") || 
               path.Contains("/_framework") ||
               path.Contains("/favicon.ico") ||
               path.Contains("/css/") ||
               path.Contains("/js/") ||
               path.Contains("/images/") ||
               path.EndsWith(".map") ||
               path.EndsWith(".css") ||
               path.EndsWith(".js") ||
               path.EndsWith(".png") ||
               path.EndsWith(".jpg") ||
               path.EndsWith(".gif") ||
               path.EndsWith(".ico");
    }

    private async Task<string> CaptureRequestContent(HttpContext context)
    {
        try
        {
            context.Request.EnableBuffering();
            
            var body = context.Request.Body;
            body.Position = 0;
            
            using var reader = new StreamReader(body, Encoding.UTF8, leaveOpen: true);
            var content = await reader.ReadToEndAsync();
            
            body.Position = 0;
            
            // Limit content length for audit log
            if (content.Length > 5000)
            {
                content = content.Substring(0, 5000) + "... [truncated]";
            }
            
            return content;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to capture request content");
            return "";
        }
    }

    private async Task LogAuditEntry(HttpContext context, IAuditService auditService, TimeSpan elapsed, Exception? exception, string requestContent, string responseContent)
    {
        var methodName = GetActionName(context);
        var description = $"Web Service - EvoAPI - {methodName}";

        var isError = exception != null || context.Response.StatusCode >= 400;

        // On errors, capture everything available: status, exception, error response body, and request body
        string detail;
        if (isError)
        {
            var parts = new List<string> { $"HTTP {context.Response.StatusCode} {context.Request.Method} {context.Request.Path}{context.Request.QueryString}" };
            if (exception != null)
            {
                parts.Add($"Exception: {exception}");
            }
            if (!string.IsNullOrEmpty(responseContent))
            {
                parts.Add($"Response: {responseContent}");
            }
            if (!string.IsNullOrEmpty(requestContent))
            {
                parts.Add($"Request: {requestContent}");
            }
            detail = string.Join(" | ", parts);
        }
        else
        {
            detail = requestContent;
        }

        var auditEntry = new AuditEntry
        {
            Username = context.User.FindFirst("username")?.Value ??
                      context.User.FindFirst("unique_name")?.Value ?? "Anonymous",
            Name = GetUserFullName(context),
            Description = description,
            Detail = detail,
            ResponseTime = elapsed.TotalSeconds.ToString("0.00"),
            IPAddress = GetClientIPAddress(context),
            UserAgent = context.Request.Headers["User-Agent"].ToString(),
            MachineName = Environment.MachineName,
            IsError = isError
        };

        if (auditEntry.IsError)
        {
            await auditService.LogErrorAsync(auditEntry);
        }
        else
        {
            await auditService.LogAsync(auditEntry);
        }
    }

    private static string GetActionName(HttpContext context)
    {
        // Try to get the action name from route data
        var action = context.GetRouteValue("action")?.ToString();
        var controller = context.GetRouteValue("controller")?.ToString();
        
        if (!string.IsNullOrEmpty(action) && !string.IsNullOrEmpty(controller))
        {
            return $"{controller}.{action}";
        }
        
        // Fallback to method and path
        return $"{context.Request.Method} {context.Request.Path}";
    }

    private static string GetUserFullName(HttpContext context)
    {
        var firstName = context.User.FindFirst("firstname")?.Value ?? "";
        var lastName = context.User.FindFirst("lastname")?.Value ?? "";
        return $"{firstName} {lastName}".Trim();
    }

    private static string GetClientIPAddress(HttpContext context)
    {
        var ipAddress = context.Request.Headers["X-Forwarded-For"].FirstOrDefault();
        if (string.IsNullOrEmpty(ipAddress))
        {
            ipAddress = context.Request.Headers["X-Real-IP"].FirstOrDefault();
        }
        if (string.IsNullOrEmpty(ipAddress))
        {
            ipAddress = context.Connection.RemoteIpAddress?.ToString();
        }
        return ipAddress ?? "Unknown";
    }
}
