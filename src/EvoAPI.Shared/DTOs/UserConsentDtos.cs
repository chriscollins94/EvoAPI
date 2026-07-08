namespace EvoAPI.Shared.DTOs;

/// Constants for UserConsent rows. The server stamps type + disclosure version on
/// every write — these are never accepted from the client. Bump the disclosure
/// version whenever the on-screen consent copy changes so re-consent is triggered.
public static class UserConsentConstants
{
    public const string NteSmsAlerts = "NteSmsAlerts";
    public const string NteSmsDisclosureVersion = "v1";
    public const string SourceConnectAppOptInScreen = "connect_app_optin_screen";

    public const string StatusOptedIn = "OptedIn";
}

public class SmsConsentStatusResponse
{
    public bool ConsentRequired { get; set; }

    /// E.164 number consent would apply to; null when no usable mobile is on file.
    public string? PhoneNumber { get; set; }
}

public class UserConsentRecord
{
    public int UserId { get; set; }
    public string ConsentType { get; set; } = string.Empty;
    public string ContactValue { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string DisclosureVersion { get; set; } = string.Empty;
    public string Source { get; set; } = string.Empty;
    public string? IpAddress { get; set; }
    public string? UserAgent { get; set; }
}
