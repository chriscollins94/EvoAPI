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
    
    // Address fields
    public string? Address1 { get; set; }
    public string? Address2 { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? Zip { get; set; }

    // Computed properties for display
    public string FullName => $"{FirstName} {LastName}".Trim();
    public string DisplayName => !string.IsNullOrEmpty(FullName) ? FullName : Username;
    public string FullAddress => $"{Address1}{(!string.IsNullOrEmpty(Address2) ? $", {Address2}" : "")}{(!string.IsNullOrEmpty(City) ? $", {City}" : "")}{(!string.IsNullOrEmpty(State) ? $", {State}" : "")}{(!string.IsNullOrEmpty(Zip) ? $" {Zip}" : "")}".Trim(' ', ',');
}
