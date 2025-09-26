using System.ComponentModel.DataAnnotations;

namespace EvoAPI.Shared.DTOs;

public class TradeGeneralDto
{
    public int Id { get; set; }
    public string Trade { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
}

public class UserTradeGeneralDto
{
    public int Id { get; set; }
    public int UserId { get; set; }
    public int TradeGeneralId { get; set; }
    public string Trade { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
}

public class UpdateEmployeeTradeGeneralsRequest
{
    [Required]
    public int UserId { get; set; }
    
    [Required]
    public List<int> TradeGeneralIds { get; set; } = new List<int>();
}