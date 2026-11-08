using EvoAPI.Core.Interfaces;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Net.Mail;

namespace EvoAPI.Infrastructure.Services;

/// <summary>
/// SMTP email service that reads server configuration from ConfigSetting database table.
/// Matches legacy EvoWS SendEmail behavior.
/// </summary>
public class SmtpEmailService : IEmailService
{
    private readonly IDataService _dataService;
    private readonly ILogger<SmtpEmailService> _logger;

    private static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(60);

    // Office 365 submission port (STARTTLS). SmtpClient defaults to 25, which many networks block
    // outbound (the send then hangs until the timeout). Optional override: EmailServer/Port.
    private const int DefaultPort = 587;

    // Cache SMTP settings to avoid repeated DB calls within the same request
    private string? _cachedHost;
    private string? _cachedFromPassword;
    private int? _cachedPort;

    public SmtpEmailService(IDataService dataService, ILogger<SmtpEmailService> logger)
    {
        _dataService = dataService;
        _logger = logger;
    }

    public async Task SendEmailAsync(string from, string to, string subject, string htmlBody)
    {
        // Don't surface failures - email failures should not break the main workflow
        await TrySendEmailAsync(from, to, subject, htmlBody);
    }

    public async Task<(bool Sent, string? Error)> TrySendEmailAsync(string from, string to, string subject, string htmlBody)
    {
        if (string.IsNullOrWhiteSpace(to))
        {
            _logger.LogWarning("SendEmailAsync called with empty 'to' address. Subject: {Subject}", subject);
            return (false, "No recipient address");
        }

        try
        {
            // Load SMTP settings from ConfigSetting table (cached per request)
            var host = await GetSmtpHostAsync();
            var password = await GetSmtpPasswordAsync();
            var port = await GetSmtpPortAsync();

            if (string.IsNullOrEmpty(host))
            {
                _logger.LogError("SMTP host not configured in ConfigSetting (EmailServer/Host)");
                return (false, "SMTP host not configured (EmailServer/Host)");
            }

            using var msg = new MailMessage();
            msg.From = new MailAddress(from);
            msg.Subject = subject;
            msg.Body = htmlBody;
            msg.IsBodyHtml = true;

            // Support semicolon-delimited multiple recipients (legacy pattern)
            var recipients = to.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var recipient in recipients)
            {
                var trimmed = recipient.Trim();
                if (!string.IsNullOrEmpty(trimmed))
                {
                    msg.To.Add(trimmed);
                }
            }

            if (msg.To.Count == 0)
            {
                _logger.LogWarning("SendEmailAsync: No valid recipients after parsing. Subject: {Subject}", subject);
                return (false, "No valid recipients");
            }

            using var smtp = new SmtpClient();
            smtp.Host = host;
            smtp.Port = port;
            smtp.EnableSsl = true;
            smtp.Credentials = new NetworkCredential(from, password);

            // SmtpClient's Timeout only applies to the synchronous Send; bound the async
            // send ourselves so a stalled connection cannot hang the caller indefinitely.
            using var timeout = new CancellationTokenSource(SendTimeout);
            await smtp.SendMailAsync(msg, timeout.Token);
            _logger.LogInformation("Email sent successfully. To: {To}, Subject: {Subject}", to, subject);
            return (true, null);
        }
        catch (OperationCanceledException)
        {
            _logger.LogError("Email send timed out after {Seconds}s. To: {To}, Subject: {Subject}", SendTimeout.TotalSeconds, to, subject);
            return (false, $"SMTP send timed out after {SendTimeout.TotalSeconds:0}s");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send email. To: {To}, Subject: {Subject}", to, subject);
            return (false, ex.Message);
        }
    }

    private async Task<string?> GetSmtpHostAsync()
    {
        if (_cachedHost != null) return _cachedHost;
        _cachedHost = await _dataService.GetConfigSettingValueAsync("EmailServer", "Host");
        return _cachedHost;
    }

    private async Task<int> GetSmtpPortAsync()
    {
        if (_cachedPort != null) return _cachedPort.Value;
        var raw = await _dataService.GetConfigSettingValueAsync("EmailServer", "Port");
        _cachedPort = int.TryParse(raw, out var port) && port > 0 ? port : DefaultPort;
        return _cachedPort.Value;
    }

    private async Task<string?> GetSmtpPasswordAsync()
    {
        if (_cachedFromPassword != null) return _cachedFromPassword;
        _cachedFromPassword = await _dataService.GetConfigSettingValueAsync("EmailServer", "FromPassword");
        return _cachedFromPassword;
    }
}
