using System.Text.Json.Serialization;

namespace EvoAPI.Shared.DTOs;

public class FleetmaticsTokenResponse
{
    [JsonPropertyName("access_token")]
    public string AccessToken { get; set; } = string.Empty;
    
    [JsonPropertyName("expires_in")]
    public int ExpiresInSeconds { get; set; }
    
    [JsonPropertyName("token_type")]
    public string TokenType { get; set; } = "Bearer";
}

public class FleetmaticsDriverAssignment
{
    [JsonPropertyName("DriverNumber")]
    public string DriverNumber { get; set; } = string.Empty;
    
    [JsonPropertyName("VehicleNumber")]
    public string VehicleNumber { get; set; } = string.Empty;
    
    [JsonPropertyName("StartDateUTC")]
    public DateTime? StartDateUTC { get; set; }
}

public class FleetmaticsDriverAssignmentResponse
{
    [JsonPropertyName("assignments")]
    public List<FleetmaticsDriverAssignment> Assignments { get; set; } = new();
    
    [JsonPropertyName("total_count")]
    public int TotalCount { get; set; }
}

public class FleetmaticsSyncResultDto
{
    public int TotalUsersProcessed { get; set; }
    public int SuccessfulUpdates { get; set; }
    public int Errors { get; set; }
    public TimeSpan Duration { get; set; }
    public List<string> ErrorMessages { get; set; } = new();
    public DateTime SyncDateTime { get; set; } = DateTime.UtcNow;
}

public class UpdateVehicleNumberRequest
{
    public int UserId { get; set; }
    public string VehicleNumber { get; set; } = string.Empty;
}

public class UserFleetmaticsDto
{
    public int UserId { get; set; }
    public string Username { get; set; } = string.Empty;
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string EmployeeNumber { get; set; } = string.Empty;
    public string? CurrentVehicleNumber { get; set; }
    public bool IsActive { get; set; }
}
