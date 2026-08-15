using Azure;
using Azure.Communication.Sms;
using EvoAPI.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace EvoAPI.Infrastructure.Services;

public class AcsSmsService : ISmsService
{
    private readonly IDataService _dataService;
    private readonly ILogger<AcsSmsService> _logger;

    private SmsClient? _cachedClient;
    private string? _cachedConnectionString;

    public AcsSmsService(IDataService dataService, ILogger<AcsSmsService> logger)
    {
        _dataService = dataService;
        _logger = logger;
    }

    public async Task SendSmsAsync(string from, string to, string body, CancellationToken ct)
    {
        var client = await GetClientAsync();
        if (client == null)
        {
            throw new InvalidOperationException("ACS SMS client could not be created. Check NteConfig/AcsConnectionString.");
        }

        var attempt = 0;
        var delays = new[] { TimeSpan.FromMilliseconds(300), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3) };

        while (true)
        {
            attempt++;
            try
            {
                var response = await client.SendAsync(from, to, body, cancellationToken: ct);
                if (!response.Value.Successful)
                {
                    throw new InvalidOperationException(
                        $"ACS SMS rejected. HttpStatusCode={response.Value.HttpStatusCode}, ErrorMessage={response.Value.ErrorMessage}");
                }
                _logger.LogInformation("SMS sent via ACS. To={To}, MessageId={MessageId}", to, response.Value.MessageId);
                return;
            }
            catch (RequestFailedException ex) when (IsRetriable(ex.Status) && attempt <= delays.Length)
            {
                _logger.LogWarning(ex, "ACS SMS attempt {Attempt} failed (status {Status}); retrying", attempt, ex.Status);
                await Task.Delay(delays[attempt - 1], ct);
            }
        }
    }

    private static bool IsRetriable(int status) => status == 429 || (status >= 500 && status < 600);

    private async Task<SmsClient?> GetClientAsync()
    {
        var conn = await _dataService.GetConfigSettingValueAsync("NteConfig", "AcsConnectionString");
        if (string.IsNullOrWhiteSpace(conn))
        {
            _logger.LogError("NteConfig/AcsConnectionString is empty");
            return null;
        }

        if (_cachedClient != null && conn == _cachedConnectionString)
        {
            return _cachedClient;
        }

        _cachedConnectionString = conn;
        _cachedClient = new SmsClient(conn);
        return _cachedClient;
    }
}
