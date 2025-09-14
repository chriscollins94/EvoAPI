using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace EvoAPI.Infrastructure.Services;

public class FleetmaticsSyncService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<FleetmaticsSyncService> _logger;
    private readonly IConfiguration _configuration;
    private readonly int _syncHour;
    private readonly TimeSpan _retryDelay = TimeSpan.FromMinutes(30);
    private readonly int _maxRetries = 3;

    public FleetmaticsSyncService(
        IServiceProvider serviceProvider,
        ILogger<FleetmaticsSyncService> logger,
        IConfiguration configuration)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _configuration = configuration;
        
        // Get sync time from configuration (default to 2:00 AM)
        _syncHour = _configuration.GetValue<int>("Fleetmatics:SyncHour", 2);
        
        // Validate sync hour
        if (_syncHour < 0 || _syncHour > 23)
        {
            _logger.LogWarning("Invalid sync hour {SyncHour}, defaulting to 2 AM", _syncHour);
            _syncHour = 2;
        }
        
        _logger.LogInformation("Fleetmatics sync service initialized with daily sync at {SyncHour}:00", _syncHour);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Fleetmatics sync service started");
        
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var nextRunTime = CalculateNextRunTime();
                var delay = nextRunTime - DateTime.Now;
                
                _logger.LogInformation("Next Fleetmatics sync scheduled for {NextRun} (in {DelayHours:F1} hours)", 
                    nextRunTime, delay.TotalHours);

                // Wait until the scheduled time
                await Task.Delay(delay, stoppingToken);
                
                if (!stoppingToken.IsCancellationRequested)
                {
                    await PerformSyncWithRetry(stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when cancellation is requested
                _logger.LogInformation("Fleetmatics sync service cancellation requested");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in Fleetmatics sync service main loop");
                
                // Log the error to audit service
                await LogErrorToAuditAsync("FleetmaticsSyncService - Main Loop Error", ex);
                
                // Wait before retrying to prevent tight error loops
                await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
            }
        }
        
        _logger.LogInformation("Fleetmatics sync service stopped");
    }

    private DateTime CalculateNextRunTime()
    {
        var now = DateTime.Now;
        var nextRun = DateTime.Today.AddHours(_syncHour);
        
        // If we've already passed today's sync time, schedule for tomorrow
        if (now >= nextRun)
        {
            nextRun = nextRun.AddDays(1);
        }
        
        return nextRun;
    }

    private async Task PerformSyncWithRetry(CancellationToken stoppingToken)
    {
        var attempt = 1;
        var success = false;
        
        while (attempt <= _maxRetries && !success && !stoppingToken.IsCancellationRequested)
        {
            try
            {
                _logger.LogInformation("Starting Fleetmatics sync attempt {Attempt} of {MaxRetries}", attempt, _maxRetries);
                
                await PerformSync();
                success = true;
                
                _logger.LogInformation("Fleetmatics sync completed successfully on attempt {Attempt}", attempt);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fleetmatics sync attempt {Attempt} failed: {ErrorMessage}", attempt, ex.Message);
                
                await LogErrorToAuditAsync($"FleetmaticsSyncService - Attempt {attempt} Failed", ex);
                
                if (attempt < _maxRetries)
                {
                    _logger.LogInformation("Retrying Fleetmatics sync in {RetryDelay} minutes", _retryDelay.TotalMinutes);
                    await Task.Delay(_retryDelay, stoppingToken);
                }
                else
                {
                    _logger.LogError("All Fleetmatics sync attempts failed. Will retry tomorrow at scheduled time.");
                    
                    await LogErrorToAuditAsync("FleetmaticsSyncService - All Attempts Failed", ex, new { 
                        Attempts = attempt,
                        NextRetry = CalculateNextRunTime().ToString("yyyy-MM-dd HH:mm:ss")
                    });
                }
                
                attempt++;
            }
        }
    }

    private async Task PerformSync()
    {
        var stopwatch = Stopwatch.StartNew();
        
        using var scope = _serviceProvider.CreateScope();
        var fleetmaticsService = scope.ServiceProvider.GetRequiredService<IFleetmaticsService>();
        var auditService = scope.ServiceProvider.GetRequiredService<IAuditService>();
        
        try
        {
            _logger.LogInformation("Executing scheduled Fleetmatics vehicle assignment sync");
            
            var syncResult = await fleetmaticsService.SyncAllVehicleAssignmentsAsync();
            
            stopwatch.Stop();
            
            _logger.LogInformation(
                "Scheduled Fleetmatics sync completed successfully. " +
                "Processed: {TotalUsers}, Updated: {SuccessfulUpdates}, Errors: {Errors}, Duration: {Duration:F2}s",
                syncResult.TotalUsersProcessed, 
                syncResult.SuccessfulUpdates, 
                syncResult.Errors, 
                syncResult.Duration.TotalSeconds
            );
            
            // Log successful sync to audit
            await auditService.LogAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "FleetmaticsSyncService",
                Description = "Scheduled Vehicle Assignment Sync - Success",
                Detail = $"Processed: {syncResult.TotalUsersProcessed}, Updated: {syncResult.SuccessfulUpdates}, Errors: {syncResult.Errors}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI FleetmaticsSyncService",
                MachineName = Environment.MachineName
            });
            
            // Log errors if any occurred during sync
            if (syncResult.Errors > 0)
            {
                _logger.LogWarning("Fleetmatics sync completed with {ErrorCount} errors: {ErrorMessages}", 
                    syncResult.Errors, string.Join("; ", syncResult.ErrorMessages));
                    
                await auditService.LogAsync(new AuditEntry
                {
                    Username = "SYSTEM",
                    Name = "FleetmaticsSyncService",
                    Description = "Scheduled Vehicle Assignment Sync - Partial Errors",
                    Detail = $"Errors: {syncResult.Errors}, Messages: {string.Join("; ", syncResult.ErrorMessages)}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                    IPAddress = "System",
                    UserAgent = "EvoAPI FleetmaticsSyncService",
                    MachineName = Environment.MachineName
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error during scheduled Fleetmatics sync");
            
            await auditService.LogErrorAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "FleetmaticsSyncService",
                Description = "Scheduled Vehicle Assignment Sync - Failed",
                Detail = $"Error: {ex.Message}, Duration: {stopwatch.Elapsed.TotalSeconds:F2}s",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI FleetmaticsSyncService",
                MachineName = Environment.MachineName,
                IsError = true
            });
            
            throw; // Re-throw to trigger retry logic
        }
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
                Name = "FleetmaticsSyncService",
                Description = description,
                Detail = detail,
                IPAddress = "System",
                UserAgent = "EvoAPI FleetmaticsSyncService",
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
        _logger.LogInformation("Fleetmatics sync service stop requested");
        
        try
        {
            await LogErrorToAuditAsync("FleetmaticsSyncService - Service Stopped", 
                new Exception("Service stopped"), 
                new { StopTime = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss") });
        }
        catch
        {
            // Ignore audit logging errors during shutdown
        }
        
        await base.StopAsync(stoppingToken);
        _logger.LogInformation("Fleetmatics sync service stopped");
    }
}
