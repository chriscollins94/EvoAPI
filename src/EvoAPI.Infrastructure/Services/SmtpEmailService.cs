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

    // Cache SMTP settings to avoid repeated DB calls within the same request
    private string? _cachedHost;
    private string? _cachedFromPassword;

    public SmtpEmailService(IDataService dataService, ILogger<SmtpEmailService> logger)
    {
        _dataService = dataService;
        _logger = logger;
    }

    public async Task SendEmailAsync(string from, string to, string subject, string htmlBody)
    {
        if (string.IsNullOrWhiteSpace(to))
        {
            _logger.LogWarning("SendEmailAsync called with empty 'to' address. Subject: {Subject}", subject);
            return;
        }

        try
        {
            // Load SMTP settings from ConfigSetting table (cached per request)
            var host = await GetSmtpHostAsync();
            var password = await GetSmtpPasswordAsync();

            if (string.IsNullOrEmpty(host))
            {
                _logger.LogError("SMTP host not configured in ConfigSetting (EmailServer/Host)");
                return;
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
                return;
            }

            using var smtp = new SmtpClient();
            smtp.Host = host;
            smtp.EnableSsl = true;
            smtp.Credentials = new NetworkCredential(from, password);

            await smtp.SendMailAsync(msg);
            _logger.LogInformation("Email sent successfully. To: {To}, Subject: {Subject}", to, subject);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send email. To: {To}, Subject: {Subject}", to, subject);
            // Don't rethrow - email failures should not break the main workflow
        }
    }

    private async Task<string?> GetSmtpHostAsync()
    {
        if (_cachedHost != null) return _cachedHost;
        _cachedHost = await _dataService.GetConfigSettingValueAsync("EmailServer", "Host");
        return _cachedHost;
    }

    private async Task<string?> GetSmtpPasswordAsync()
    {
        if (_cachedFromPassword != null) return _cachedFromPassword;
        _cachedFromPassword = await _dataService.GetConfigSettingValueAsync("EmailServer", "FromPassword");
        return _cachedFromPassword;
    }
}
