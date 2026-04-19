using EvoAPI.Core.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EvoAPI.Infrastructure.Services;

public class NteNotificationBackgroundService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<NteNotificationBackgroundService> _logger;

    private static readonly TimeSpan DefaultInterval = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan ErrorBackoff = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan StartupDelay = TimeSpan.FromSeconds(30);

    public NteNotificationBackgroundService(
        IServiceProvider serviceProvider,
        ILogger<NteNotificationBackgroundService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("NTE notification background service started");

        try
        {
            await Task.Delay(StartupDelay, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            var interval = DefaultInterval;
            try
            {
                interval = await ResolveIntervalAsync();
                await RunOnceAsync(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("NTE notification background service cancellation requested");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error in NTE notification scan loop; backing off");
                interval = ErrorBackoff;
            }

            try
            {
                await Task.Delay(interval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _logger.LogInformation("NTE notification background service stopped");
    }

    private async Task RunOnceAsync(CancellationToken ct)
    {
        using var scope = _serviceProvider.CreateScope();
        var orchestrator = scope.ServiceProvider.GetRequiredService<INteNotificationService>();
        await orchestrator.RunScanAsync(ct);
    }

    private async Task<TimeSpan> ResolveIntervalAsync()
    {
        try
        {
            using var scope = _serviceProvider.CreateScope();
            var data = scope.ServiceProvider.GetRequiredService<IDataService>();
            var raw = await data.GetConfigSettingValueAsync("NteConfig", "ScanIntervalMinutes");
            if (int.TryParse(raw, out var minutes) && minutes > 0)
            {
                return TimeSpan.FromMinutes(minutes);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load NteConfig/ScanIntervalMinutes; using default");
        }

        return DefaultInterval;
    }
}
