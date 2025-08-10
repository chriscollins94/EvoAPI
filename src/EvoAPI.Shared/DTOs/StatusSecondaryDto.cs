namespace EvoAPI.Shared.DTOs;

public class StatusSecondaryDto
{
    public int Id { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public int StatusId { get; set; }
    public string StatusSecondary { get; set; } = string.Empty;
    public string? Color { get; set; }
    public string? Code { get; set; }
    public int Attack { get; set; }
}

public class UpdateStatusSecondaryRequest
{
    public int Id { get; set; }
    public int StatusId { get; set; }
    public string StatusSecondary { get; set; } = string.Empty;
    public string? Color { get; set; }
    public string? Code { get; set; }
    public int Attack { get; set; }
}
