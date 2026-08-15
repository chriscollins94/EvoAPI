using System.ComponentModel.DataAnnotations;

namespace EvoAPI.Shared.DTOs.Authentication;

public class LoginRequest
{
    [Required]
    public string Username { get; set; } = string.Empty;

    [Required]
    public string Password { get; set; } = string.Empty;

    [Required]
    public decimal Latitude { get; set; }

    [Required]
    public decimal Longitude { get; set; }
}
