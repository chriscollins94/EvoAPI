namespace EvoAPI.Shared.DTOs;

public class UserDto
{
    public int Id { get; set; }
    public int OId { get; set; }
    public int? AId { get; set; }
    public int? VId { get; set; }
    public int? SupervisorId { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public string Username { get; set; } = string.Empty;
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? Email { get; set; }
    public bool Active { get; set; }
    public int? ZoneId { get; set; }

    // Computed properties for display
    public string FullName => $"{FirstName} {LastName}".Trim();
    public string DisplayName => !string.IsNullOrEmpty(FullName) ? FullName : Username;
}
