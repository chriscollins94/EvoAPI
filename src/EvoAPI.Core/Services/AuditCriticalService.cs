using EvoAPI.Core.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;
using System.Text.Json;

namespace EvoAPI.Core.Services;

public class AuditCriticalService : IAuditCriticalService
{
    private readonly ILogger<AuditCriticalService> _logger;
    private readonly IConfiguration _configuration;
    
    // User context that can be set by the caller (controller)
    public string? Username { get; set; }
    public string? UserFullName { get; set; }
    public string? IPAddress { get; set; }
    public string? UserAgent { get; set; }
    
    public AuditCriticalService(ILogger<AuditCriticalService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }
    
    public async Task LogAsync(string description, object? detail = null, string? responseTime = null)
    {
        try
        {
            var detailJson = detail != null ? JsonSerializer.Serialize(detail) : null;
            await WriteToDatabase(description, detailJson, responseTime, false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to log critical audit entry");
        }
    }
    
    public async Task LogChangeAsync(string description, Dictionary<string, object?>? oldValues = null, Dictionary<string, object?>? newValues = null, string? responseTime = null)
    {
        try
        {
            var changeDetail = new
            {
                OldValues = oldValues,
                NewValues = newValues,
                ChangedFields = GetChangedFields(oldValues, newValues)
            };
            
            var detailJson = JsonSerializer.Serialize(changeDetail);
            await WriteToDatabase(description, detailJson, responseTime, false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to log critical audit change entry");
        }
    }
    
    public async Task LogErrorAsync(string description, Exception ex, object? detail = null)
    {
        try
        {
            var detailJson = detail != null 
                ? JsonSerializer.Serialize(new { Error = ex.ToString(), Detail = detail })
                : ex.ToString();
            
            await WriteToDatabase(description, detailJson, null, true);
        }
        catch (Exception auditEx)
        {
            _logger.LogError(auditEx, "Failed to log critical audit error entry");
        }
    }
    
    private async Task WriteToDatabase(string description, string? detail, string? responseTime, bool isError)
    {
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                _logger.LogWarning("No connection string found for critical audit logging");
                return;
            }

            const string sql = @"
                INSERT INTO AuditCritical (
                    ac_insertdatetime,
                    ac_description,
                    ac_detail,
                    ac_responsetime,
                    ac_error,
                    ac_browser,
                    ac_ipaddress,
                    ac_username,
                    ac_name,
                    ac_servername
                )
                VALUES (
                    GETDATE(),
                    @Description,
                    @Detail,
                    @ResponseTime,
                    @IsError,
                    @Browser,
                    @IPAddress,
                    @Username,
                    @Name,
                    @ServerName
                )";

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=5;";
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 5;
            
            // Truncate values to match database column sizes
            var descriptionValue = Truncate(description ?? string.Empty, 8000);
            var detailValue = detail ?? (object)DBNull.Value;
            var responseTimeValue = Truncate(responseTime ?? string.Empty, 10);
            var browserValue = Truncate(UserAgent ?? string.Empty, 800);
            var ipAddressValue = Truncate(IPAddress ?? string.Empty, 15);
            var usernameValue = Truncate(Username ?? string.Empty, 25);
            var nameValue = Truncate(UserFullName ?? string.Empty, 100);
            var serverNameValue = Truncate(GetServerFromConnectionString(connectionString) ?? string.Empty, 25);
            
            command.Parameters.AddWithValue("@Description", descriptionValue);
            command.Parameters.AddWithValue("@Detail", detailValue);
            command.Parameters.AddWithValue("@ResponseTime", string.IsNullOrEmpty(responseTimeValue) ? (object)DBNull.Value : responseTimeValue);
            command.Parameters.AddWithValue("@IsError", isError ? 1 : 0);
            command.Parameters.AddWithValue("@Browser", string.IsNullOrEmpty(browserValue) ? (object)DBNull.Value : browserValue);
            command.Parameters.AddWithValue("@IPAddress", string.IsNullOrEmpty(ipAddressValue) ? (object)DBNull.Value : ipAddressValue);
            command.Parameters.AddWithValue("@Username", string.IsNullOrEmpty(usernameValue) ? (object)DBNull.Value : usernameValue);
            command.Parameters.AddWithValue("@Name", string.IsNullOrEmpty(nameValue) ? (object)DBNull.Value : nameValue);
            command.Parameters.AddWithValue("@ServerName", string.IsNullOrEmpty(serverNameValue) ? (object)DBNull.Value : serverNameValue);
            
            await connection.OpenAsync();
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error writing to AuditCritical database. Username: {Username}, IPAddress: {IPAddress}, UserAgent length: {UserAgentLength}", 
                Username, IPAddress, UserAgent?.Length ?? 0);
        }
    }

    /// <summary>
    /// Truncate a string to a maximum length, returning empty string if null
    /// </summary>
    private static string Truncate(string? value, int maxLength)
    {
        if (string.IsNullOrEmpty(value))
            return string.Empty;
        
        return value.Length > maxLength ? value.Substring(0, maxLength) : value;
    }
    
    private static string? GetServerFromConnectionString(string connectionString)
    {
        try
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            return builder.DataSource;
        }
        catch
        {
            return null;
        }
    }
    
    /// <summary>
    /// Get list of field names that changed between old and new values
    /// </summary>
    private static List<string> GetChangedFields(Dictionary<string, object?>? oldValues, Dictionary<string, object?>? newValues)
    {
        var changedFields = new List<string>();
        
        if (newValues == null)
            return changedFields;
        
        foreach (var kvp in newValues)
        {
            var oldValue = oldValues?.ContainsKey(kvp.Key) == true ? oldValues[kvp.Key] : null;
            var newValue = kvp.Value;
            
            if (!ValuesEqual(oldValue, newValue))
            {
                changedFields.Add(kvp.Key);
            }
        }
        
        return changedFields;
    }
    
    private static bool ValuesEqual(object? oldValue, object? newValue)
    {
        if (oldValue == null && newValue == null)
            return true;
        
        if (oldValue == null || newValue == null)
            return false;
        
        return oldValue.Equals(newValue);
    }
}
