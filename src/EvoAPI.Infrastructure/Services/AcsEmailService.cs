using Azure;
using Azure.Communication.Email;
using EvoAPI.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace EvoAPI.Infrastructure.Services;

// ACS-backed email sender used by the NTE notification path. The existing
// SmtpEmailService remains the IEmailService default for the rest of the app.
public class AcsEmailService
{
    private readonly IDataService _dataService;
    private readonly ILogger<AcsEmailService> _logger;

    private EmailClient? _cachedClient;
    private string? _cachedConnectionString;

    public AcsEmailService(IDataService dataService, ILogger<AcsEmailService> logger)
    {
        _dataService = dataService;
        _logger = logger;
    }

    public async Task SendEmailAsync(string from, string to, string subject, string htmlBody, CancellationToken ct)
    {
        var client = await GetClientAsync();
        if (client == null)
        {
            throw new InvalidOperationException("ACS Email client could not be created. Check NteConfig/AcsConnectionString.");
        }

        var attempt = 0;
        var delays = new[] { TimeSpan.FromMilliseconds(300), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3) };

        while (true)
        {
            attempt++;
            try
            {
                var operation = await client.SendAsync(
                    wait: WaitUntil.Completed,
                    senderAddress: from,
                    recipientAddress: to,
                    subject: subject,
                    htmlContent: htmlBody,
                    cancellationToken: ct);

                _logger.LogInformation("Email sent via ACS. To={To}, OperationId={OperationId}, Status={Status}",
                    to, operation.Id, operation.Value.Status);
                return;
            }
            catch (RequestFailedException ex) when (IsRetriable(ex.Status) && attempt <= delays.Length)
            {
                _logger.LogWarning(ex, "ACS Email attempt {Attempt} failed (status {Status}); retrying", attempt, ex.Status);
                await Task.Delay(delays[attempt - 1], ct);
            }
        }
    }

    private static bool IsRetriable(int status) => status == 429 || (status >= 500 && status < 600);

    private async Task<EmailClient?> GetClientAsync()
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
        _cachedClient = new EmailClient(conn);
        return _cachedClient;
    }
}
