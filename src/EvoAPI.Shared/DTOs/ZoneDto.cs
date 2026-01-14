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
    
    // Extended properties for region-zone management
    public int ZoneId { get; set; }
    public string ZoneNumber { get; set; } = string.Empty;
    public string ZoneDescription { get; set; } = string.Empty;
    public string ZoneAcronym { get; set; } = string.Empty;
    public string? ZoneEmail { get; set; }
    public int? ZfmUserId { get; set; }
    public string? ZfmName { get; set; }
    public string? ZfmPicture { get; set; }
    public int? RegionId { get; set; }
    public string? RegionName { get; set; }
    public int EmployeeCount { get; set; }
}
