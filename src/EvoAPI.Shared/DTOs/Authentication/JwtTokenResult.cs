namespace EvoAPI.Shared.DTOs.Authentication;

public class JwtTokenResult
{
    public string Token { get; set; } = string.Empty;
    public string XsrfToken { get; set; } = string.Empty;
    public DateTime Expiration { get; set; }
    public int ExpiresInSeconds { get; set; }
}
