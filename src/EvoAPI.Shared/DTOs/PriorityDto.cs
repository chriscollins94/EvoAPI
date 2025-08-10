namespace EvoAPI.Shared.DTOs;

public class PriorityDto
{
    public int Id { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public string PriorityName { get; set; } = string.Empty;
    public int? Order { get; set; }
    public string? Color { get; set; }
    public decimal? ArrivalTimeInHours { get; set; }
    public int Attack { get; set; }
}

public class UpdatePriorityRequest
{
    public int Id { get; set; }
    public string PriorityName { get; set; } = string.Empty;
    public int? Order { get; set; }
    public string? Color { get; set; }
    public decimal? ArrivalTimeInHours { get; set; }
    public int Attack { get; set; }
}
