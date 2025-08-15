namespace EvoAPI.Shared.DTOs;

public class ZoneDto
{
    public int Id { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public string Number { get; set; } = string.Empty;
    public string? Description { get; set; }
    public string? Acronym { get; set; }
    public int UserId { get; set; }
}
