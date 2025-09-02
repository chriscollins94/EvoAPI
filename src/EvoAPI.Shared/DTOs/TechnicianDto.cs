namespace EvoAPI.Shared.DTOs;

public class TechnicianDto
{
    public int Id { get; set; }
    public string? EmployeeNumber { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string Username { get; set; } = string.Empty;
    public string? Email { get; set; }
    public string? Picture { get; set; }
    public string? PhoneMobile { get; set; }

    // Computed properties for display
    public string FullName => $"{FirstName} {LastName}".Trim();
    public string DisplayName => !string.IsNullOrEmpty(FullName) ? FullName : Username;
}
