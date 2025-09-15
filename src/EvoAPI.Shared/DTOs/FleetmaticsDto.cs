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

public class VehicleLocationDto
{
    public string VehicleNumber { get; set; } = string.Empty;
    public double Latitude { get; set; }
    public double Longitude { get; set; }
    public string Address { get; set; } = string.Empty;
    public DateTime LastUpdateTime { get; set; }
    public double? Speed { get; set; }
    public double? Heading { get; set; }
    public bool IsActive { get; set; } = true;
}

public class FleetmaticsVehicleLocation
{
    [JsonPropertyName("VehicleNumber")]
    public string VehicleNumber { get; set; } = string.Empty;
    
    [JsonPropertyName("Address")]
    public List<FleetmaticsAddressEntry> Address { get; set; } = new();
    
    [JsonPropertyName("Latitude")]
    public double Latitude { get; set; }
    
    [JsonPropertyName("Longitude")]
    public double Longitude { get; set; }
    
    [JsonPropertyName("UpdatedUTC")]
    public string UpdatedUTC { get; set; } = string.Empty;
    
    [JsonPropertyName("Speed")]
    public double? Speed { get; set; }
    
    [JsonPropertyName("Heading")]
    public double? Heading { get; set; }
}

public class FleetmaticsAddressEntry
{
    [JsonPropertyName("Value")]
    public string Value { get; set; } = string.Empty;
    
    [JsonPropertyName("AddressLine1")]
    public string AddressLine1 { get; set; } = string.Empty;
    
    [JsonPropertyName("Locality")]
    public string Locality { get; set; } = string.Empty;
    
    [JsonPropertyName("AdministrativeArea")]
    public string AdministrativeArea { get; set; } = string.Empty;
    
    [JsonPropertyName("PostalCode")]
    public string PostalCode { get; set; } = string.Empty;
    
    [JsonPropertyName("Country")]
    public string Country { get; set; } = string.Empty;
}

// New models to match actual Fleetmatics API response structure
public class FleetmaticsLocationResponse
{
    [JsonPropertyName("Data")]
    public List<FleetmaticsLocationData> Data { get; set; } = new();
}

public class FleetmaticsLocationData
{
    [JsonPropertyName("VehicleNumber")]
    public string VehicleNumber { get; set; } = string.Empty;
    
    [JsonPropertyName("StatusCode")]
    public int StatusCode { get; set; }
    
    [JsonPropertyName("ContentResource")]
    public FleetmaticsContentResource ContentResource { get; set; } = new();
}

public class FleetmaticsContentResource
{
    [JsonPropertyName("Value")]
    public FleetmaticsLocationValue Value { get; set; } = new();
}

public class FleetmaticsLocationValue
{
    [JsonPropertyName("Address")]
    public FleetmaticsValueAddress Address { get; set; } = new();
    
    [JsonPropertyName("Latitude")]
    public double Latitude { get; set; }
    
    [JsonPropertyName("Longitude")]
    public double Longitude { get; set; }
    
    [JsonPropertyName("UpdatedUTC")]
    public string UpdatedUTC { get; set; } = string.Empty;
    
    [JsonPropertyName("Speed")]
    public double? Speed { get; set; }
    
    [JsonPropertyName("Heading")]
    public string Heading { get; set; } = string.Empty;
}

public class FleetmaticsValueAddress
{
    [JsonPropertyName("AddressLine1")]
    public string AddressLine1 { get; set; } = string.Empty;
    
    [JsonPropertyName("Locality")]
    public string Locality { get; set; } = string.Empty;
    
    [JsonPropertyName("AdministrativeArea")]
    public string AdministrativeArea { get; set; } = string.Empty;
    
    [JsonPropertyName("PostalCode")]
    public string PostalCode { get; set; } = string.Empty;
    
    [JsonPropertyName("Country")]
    public string Country { get; set; } = string.Empty;
}
