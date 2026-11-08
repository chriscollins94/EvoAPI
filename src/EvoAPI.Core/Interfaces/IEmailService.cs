namespace EvoAPI.Core.Interfaces;

/// <summary>
/// Core email sending service - handles SMTP delivery
/// </summary>
public interface IEmailService
{
    /// <summary>
    /// Send an HTML email. Failures are logged and swallowed so they never break
    /// the calling workflow (time-off notices, reports).
    /// </summary>
    /// <param name="from">Sender email address</param>
    /// <param name="to">Recipient(s) - semicolon-delimited for multiple</param>
    /// <param name="subject">Email subject</param>
    /// <param name="htmlBody">HTML email body</param>
    Task SendEmailAsync(string from, string to, string subject, string htmlBody);

    /// <summary>
    /// Same as SendEmailAsync but reports the outcome. Callers that record
    /// delivery status (NTE notifications) use this one.
    /// </summary>
    /// <returns>true when the SMTP server accepted the message; otherwise false, with the reason in <paramref name="error"/>.</returns>
    Task<(bool Sent, string? Error)> TrySendEmailAsync(string from, string to, string subject, string htmlBody);
}
