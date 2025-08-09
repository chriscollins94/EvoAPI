using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;

namespace EvoAPI.Core.Services;

public class AuditService : IAuditService
{
    private readonly ILogger<AuditService> _logger;
    private readonly IConfiguration _configuration;
    
    public AuditService(ILogger<AuditService> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }
    
    public async Task LogAsync(AuditEntry auditEntry)
    {
        try
        {
            _logger.LogInformation("=== AUDIT LOG ATTEMPT ===");
            _logger.LogInformation("Attempting to write audit entry to database...");
            
            await WriteToDatabase(auditEntry);
            
            _logger.LogInformation("Audit database write completed successfully");
            _logger.LogInformation("Audit: {Username} - {Description} - {ResponseTime}s - IP: {IPAddress}", 
                auditEntry.Username, auditEntry.Description, auditEntry.ResponseTime, auditEntry.IPAddress);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to log audit entry to database");
        }
    }
    
    public async Task LogErrorAsync(AuditEntry auditEntry)
    {
        try
        {
            _logger.LogInformation("=== AUDIT ERROR LOG ATTEMPT ===");
            _logger.LogInformation("Attempting to write audit error entry to database...");
            
            auditEntry.IsError = true;
            await WriteToDatabase(auditEntry);
            
            _logger.LogInformation("Audit error database write completed successfully");
            _logger.LogError("Audit Error: {Username} - {Description} - {Detail}", 
                auditEntry.Username, auditEntry.Description, auditEntry.Detail);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to log audit error entry to database");
        }
    }
    
    private async Task WriteToDatabase(AuditEntry auditEntry)
    {
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                _logger.LogWarning("No connection string found for audit logging");
                return;
            }

            _logger.LogInformation("Found connection string for audit logging");

            const string sql = @"
                INSERT INTO Audit (a_username, a_name, a_description, a_detail, a_responsetime, 
                                   a_ipaddress, a_browser, a_servername, a_error)
                VALUES (@Username, @Name, @Description, @Detail, @ResponseTime, 
                        @IPAddress, @UserAgent, @ServerName, @IsError)";

            _logger.LogInformation("Audit SQL: {Sql}", sql);

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=5;"; // 5 second connection timeout
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 5; // 5 second command timeout
            
            command.Parameters.AddWithValue("@Username", auditEntry.Username ?? string.Empty);
            command.Parameters.AddWithValue("@Name", auditEntry.Name ?? string.Empty);
            command.Parameters.AddWithValue("@Description", auditEntry.Description ?? string.Empty);
            command.Parameters.AddWithValue("@Detail", auditEntry.Detail ?? string.Empty);
            command.Parameters.AddWithValue("@ResponseTime", auditEntry.ResponseTime ?? string.Empty);
            command.Parameters.AddWithValue("@IPAddress", auditEntry.IPAddress ?? string.Empty);
            command.Parameters.AddWithValue("@UserAgent", auditEntry.UserAgent ?? string.Empty);
            command.Parameters.AddWithValue("@ServerName", auditEntry.MachineName ?? Environment.MachineName);
            command.Parameters.AddWithValue("@IsError", auditEntry.IsError);

            _logger.LogInformation("Audit parameters: Username={Username}, Name={Name}, Description={Description}", 
                auditEntry.Username, auditEntry.Name, auditEntry.Description);

            _logger.LogInformation("Opening database connection to: {Server}", GetServerFromConnectionString(connectionString));
            await connection.OpenAsync();
            
            _logger.LogInformation("Executing audit SQL command");
            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            _logger.LogInformation("Audit entry written to database successfully. Rows affected: {RowsAffected}", rowsAffected);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write audit entry to database. Connection string: {ConnectionString}", 
                _configuration.GetConnectionString("DefaultConnection")?.Replace("Password=", "Password=***"));
            // Don't rethrow - we don't want audit failures to break the application
        }
    }
    
    private static string GetServerFromConnectionString(string connectionString)
    {
        try
        {
            var parts = connectionString.Split(';');
            var serverPart = parts.FirstOrDefault(p => p.Trim().StartsWith("Server=", StringComparison.OrdinalIgnoreCase));
            return serverPart?.Split('=')[1] ?? "Unknown";
        }
        catch
        {
            return "Unknown";
        }
    }
}
