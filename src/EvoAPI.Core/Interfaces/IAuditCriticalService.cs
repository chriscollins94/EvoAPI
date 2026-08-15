using System.Text.Json.Serialization;

namespace EvoAPI.Core.Interfaces;

public interface IAuditCriticalService
{
    // User context properties - set by controller before calling log methods
    string? Username { get; set; }
    string? UserFullName { get; set; }
    string? IPAddress { get; set; }
    string? UserAgent { get; set; }

    /// <summary>
    /// Log a critical business process event with optional change details
    /// </summary>
    Task LogAsync(string description, object? detail = null, string? responseTime = null);
    
    /// <summary>
    /// Log a critical operation with JSON-formatted change details
    /// </summary>
    Task LogChangeAsync(string description, Dictionary<string, object?>? oldValues = null, Dictionary<string, object?>? newValues = null, string? responseTime = null);
    
    /// <summary>
    /// Log an error during a critical business process
    /// </summary>
    Task LogErrorAsync(string description, Exception ex, object? detail = null);
}
