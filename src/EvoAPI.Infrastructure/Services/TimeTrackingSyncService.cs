using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace EvoAPI.Infrastructure.Services;

public class TimeTrackingSyncService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<TimeTrackingSyncService> _logger;
    private readonly IConfiguration _configuration;
    private readonly int _syncIntervalMinutes;
    private readonly TimeSpan _retryDelay = TimeSpan.FromMinutes(5);

    public TimeTrackingSyncService(
        IServiceProvider serviceProvider,
        ILogger<TimeTrackingSyncService> logger,
        IConfiguration configuration)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _configuration = configuration;
        
        // Get sync interval from configuration (default to 15 minutes)
        _syncIntervalMinutes = _configuration.GetValue<int>("TimeTracking:SyncIntervalMinutes", 15);
        
        // Validate sync interval
        if (_syncIntervalMinutes < 1 || _syncIntervalMinutes > 60)
        {
            _logger.LogWarning("Invalid sync interval {SyncInterval}, defaulting to 15 minutes", _syncIntervalMinutes);
            _syncIntervalMinutes = 15;
        }
        
        _logger.LogInformation("Time tracking sync service initialized with {SyncInterval} minute interval", _syncIntervalMinutes);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Time tracking sync service started");
        
        // Wait 2 minutes before first run to allow API to fully start
        await Task.Delay(TimeSpan.FromMinutes(2), stoppingToken);
        
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await PerformSync();
                
                // Wait for the configured interval before next sync
                _logger.LogInformation("Next time tracking sync in {SyncInterval} minutes", _syncIntervalMinutes);
                await Task.Delay(TimeSpan.FromMinutes(_syncIntervalMinutes), stoppingToken);
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested
                _logger.LogInformation("Time tracking sync service cancellation requested");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in time tracking sync service main loop");
                
                // Log the error to audit service
                await LogErrorToAuditAsync("TimeTrackingSyncService - Main Loop Error", ex);
                
                // Wait retry delay before continuing
                await Task.Delay(_retryDelay, stoppingToken);
            }
        }
        
        _logger.LogInformation("Time tracking sync service stopped");
    }

    private async Task PerformSync()
    {
        var stopwatch = Stopwatch.StartNew();
        
        using var scope = _serviceProvider.CreateScope();
        var dataService = scope.ServiceProvider.GetRequiredService<IDataService>();
        var auditService = scope.ServiceProvider.GetRequiredService<IAuditService>();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        
        try
        {
            _logger.LogInformation("Executing periodic time tracking sync for clocked-in/checked-in users");
            
            var usersToProcess = await GetUsersNeedingPeriodicCheckin(connectionString);
            
            if (usersToProcess.Count == 0)
            {
                _logger.LogInformation("No users found requiring periodic check-in");
                stopwatch.Stop();
                return;
            }
            
            _logger.LogInformation("Found {UserCount} users requiring periodic check-in", usersToProcess.Count);
            
            int successCount = 0;
            int errorCount = 0;
            var errorMessages = new List<string>();
            
            foreach (var user in usersToProcess)
            {
                try
                {
                    // Determine ttd_type based on ttt_id
                    string ttdType = user.TttId == 3 ? "CheckedInPeriodic" : "ClockedInPeriodic";
                    
                    _logger.LogDebug("Processing periodic check-in for User {UserId}, TTT_ID {TttId}, WO_ID {WoId}, Type {Type}", 
                        user.UserId, user.TttId, user.WoId, ttdType);
                    
                    // Call InsertTimeTrackingDetailAsync
                    var success = await dataService.InsertTimeTrackingDetailAsync(
                        userId: user.UserId,
                        tttId: user.TttId,
                        woId: user.WoId,
                        latBrowser: null,      // No GPS for periodic sync
                        lonBrowser: null,      // No GPS for periodic sync
                        ttdType: ttdType
                    );
                    
                    if (success)
                    {
                        successCount++;
                        _logger.LogDebug("Successfully created periodic check-in for User {UserId}", user.UserId);
                    }
                    else
                    {
                        errorCount++;
                        var errorMsg = $"User {user.UserId}: Failed to insert time tracking detail";
                        errorMessages.Add(errorMsg);
                        _logger.LogWarning(errorMsg);
                    }
                }
                catch (Exception ex)
                {
                    errorCount++;
                    var errorMsg = $"User {user.UserId}: {ex.Message}";
                    errorMessages.Add(errorMsg);
                    _logger.LogError(ex, "Error processing periodic check-in for User {UserId}", user.UserId);
                }
            }
            
            stopwatch.Stop();
            
            _logger.LogInformation(
                "Periodic time tracking sync completed. " +
                "Total: {TotalUsers}, Success: {SuccessCount}, Errors: {ErrorCount}, Duration: {Duration:F2}s",
                usersToProcess.Count, successCount, errorCount, stopwatch.Elapsed.TotalSeconds
            );
            
            // Log successful sync to audit
            await auditService.LogAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "TimeTrackingSyncService",
                Description = "Periodic Time Tracking Sync - Success",
                Detail = $"Processed: {usersToProcess.Count}, Success: {successCount}, Errors: {errorCount}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI TimeTrackingSyncService",
                MachineName = Environment.MachineName
            });
            
            // Log errors if any occurred during sync
            if (errorCount > 0)
            {
                _logger.LogWarning("Time tracking sync completed with {ErrorCount} errors: {ErrorMessages}", 
                    errorCount, string.Join("; ", errorMessages));
                    
                await auditService.LogAsync(new AuditEntry
                {
                    Username = "SYSTEM",
                    Name = "TimeTrackingSyncService",
                    Description = "Periodic Time Tracking Sync - Partial Errors",
                    Detail = $"Errors: {errorCount}, Messages: {string.Join("; ", errorMessages)}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                    IPAddress = "System",
                    UserAgent = "EvoAPI TimeTrackingSyncService",
                    MachineName = Environment.MachineName
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error during periodic time tracking sync");
            
            using var scope2 = _serviceProvider.CreateScope();
            var auditService2 = scope2.ServiceProvider.GetRequiredService<IAuditService>();
            
            await auditService2.LogErrorAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "TimeTrackingSyncService",
                Description = "Periodic Time Tracking Sync - Failed",
                Detail = $"Error: {ex.Message}, Duration: {stopwatch.Elapsed.TotalSeconds:F2}s",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI TimeTrackingSyncService",
                MachineName = Environment.MachineName,
                IsError = true
            });
            
            throw;
        }
    }

    private async Task<List<TimeTrackingUserDto>> GetUsersNeedingPeriodicCheckin(string connectionString)
    {
        var users = new List<TimeTrackingUserDto>();
        
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        
        // Query to get users clocked in or checked in
        // Prioritize checked-in (ttt_id = 3) over clocked-in (ttt_id = 2)
        const string sql = @"
            WITH RankedTimeTracking AS (
                SELECT 
                    u_id, 
                    ttt_id, 
                    tt.wo_id,
                    tt_insertdatetime,
                    ROW_NUMBER() OVER (PARTITION BY u_id ORDER BY ttt_id DESC, tt_insertdatetime DESC) as rn
                FROM timetracking tt, workorder wo, servicerequest sr, xrefCompanyCallCenter xccc, company c
                WHERE tt.wo_id = wo.wo_id AND wo.sr_id = sr.sr_id AND sr.xccc_id = xccc.xccc_id and xccc.c_id = c.c_id
                    AND tt_end IS NULL
                    AND ttt_id IN (2, 3)
                    AND tt_insertdatetime >= DATEADD(HOUR, -240, GETDATE())
                    AND c.c_id not in (select cs_value from configsetting where cs_identifier = 'HighVolumeCompanyID')

            )
            SELECT u_id, ttt_id, wo_id
            FROM RankedTimeTracking
            WHERE rn = 1
            ORDER BY u_id DESC";
        
        using var command = new SqlCommand(sql, connection);
        
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            users.Add(new TimeTrackingUserDto
            {
                UserId = reader.GetInt32(0),
                TttId = reader.GetInt32(1),
                WoId = reader.IsDBNull(2) ? null : reader.GetInt32(2)
            });
        }
        
        return users;
    }

    private async Task LogErrorToAuditAsync(string description, Exception ex, object? additionalData = null)
    {
        try
        {
            using var scope = _serviceProvider.CreateScope();
            var auditService = scope.ServiceProvider.GetRequiredService<IAuditService>();
            
            var detail = $"Error: {ex.Message}";
            if (additionalData != null)
            {
                detail += $", Additional Data: {System.Text.Json.JsonSerializer.Serialize(additionalData)}";
            }
            
            await auditService.LogErrorAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "TimeTrackingSyncService",
                Description = description,
                Detail = detail,
                IPAddress = "System",
                UserAgent = "EvoAPI TimeTrackingSyncService",
                MachineName = Environment.MachineName,
                IsError = true
            });
        }
        catch (Exception auditEx)
        {
            // If audit logging fails, just log to the regular logger
            _logger.LogError(auditEx, "Failed to log error to audit service: {OriginalError}", ex.Message);
        }
    }

    public override async Task StopAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Time tracking sync service stop requested");
        
        try
        {
            await LogErrorToAuditAsync("TimeTrackingSyncService - Service Stopped", 
                new Exception("Service stopped"), 
                new { StopTime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss") });
        }
        catch
        {
            // Ignore audit logging errors during shutdown
        }
        
        await base.StopAsync(stoppingToken);
        _logger.LogInformation("Time tracking sync service stopped");
    }

    // DTO for query results
    private class TimeTrackingUserDto
    {
        public int UserId { get; set; }
        public int TttId { get; set; }
        public int? WoId { get; set; }
    }
}
