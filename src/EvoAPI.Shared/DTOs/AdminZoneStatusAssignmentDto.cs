namespace EvoAPI.Shared.DTOs;

public class AdminZoneStatusAssignmentDto
{
    public int Id { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public int UserId { get; set; }
    public int ZoneId { get; set; }
    public int StatusSecondaryId { get; set; }

    // Navigation properties for display
    public string? UserDisplayName { get; set; }
    public string? ZoneName { get; set; }
    public string? StatusSecondaryName { get; set; }
}

public class CreateAdminZoneStatusAssignmentRequest
{
    public int UserId { get; set; }
    public int ZoneId { get; set; }
    public int StatusSecondaryId { get; set; }
}

public class UpdateAdminZoneStatusAssignmentRequest
{
    public int Id { get; set; }
    public int UserId { get; set; }
    public int ZoneId { get; set; }
    public int StatusSecondaryId { get; set; }
}

public class DeleteAdminZoneStatusAssignmentRequest
{
    public int UserId { get; set; }
    public int ZoneId { get; set; }
    public int StatusSecondaryId { get; set; }
}

public class StatusAssignmentMatrixDto
{
    public List<ZoneDto> Zones { get; set; } = new();
    public List<StatusSecondaryDto> StatusSecondaries { get; set; } = new();
    public List<UserDto> Users { get; set; } = new();
    public List<AdminZoneStatusAssignmentDto> Assignments { get; set; } = new();
}
