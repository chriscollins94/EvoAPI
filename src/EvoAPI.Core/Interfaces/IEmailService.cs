namespace EvoAPI.Core.Interfaces;

/// <summary>
/// Core email sending service - handles SMTP delivery
/// </summary>
public interface IEmailService
{
    /// <summary>
    /// Send an HTML email
    /// </summary>
    /// <param name="from">Sender email address</param>
    /// <param name="to">Recipient(s) - semicolon-delimited for multiple</param>
    /// <param name="subject">Email subject</param>
    /// <param name="htmlBody">HTML email body</param>
    Task SendEmailAsync(string from, string to, string subject, string htmlBody);
}
