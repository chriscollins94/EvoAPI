namespace EvoAPI.Shared.DTOs.Authentication;

public class AuthenticatedUser
{
    public int UserId { get; set; }
    public string Username { get; set; } = string.Empty;
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string Picture { get; set; } = string.Empty;
    public DateTime? PasswordChanged { get; set; }
    public List<string> Functions { get; set; } = new();
    public string AccessLevel { get; set; } = string.Empty; // "ADMIN" or "TECH"
}
