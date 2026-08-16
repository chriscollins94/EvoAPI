namespace EvoAPI.Shared.DTOs;

public class LocationDto
{
    public int LId { get; set; }
    public int CId { get; set; }
    public int AId { get; set; }
    public string LLocation { get; set; } = string.Empty;
    public string? LPhone { get; set; }
    public string? LHours { get; set; }
    public string? LNote { get; set; }
    public string? LEmail { get; set; }
    public bool LActive { get; set; } = true;
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }

    // Address details (flattened for convenience)
    public string? AAddress1 { get; set; }
    public string? AAddress2 { get; set; }
    public string? ACity { get; set; }
    public string? AState { get; set; }
    public string? AZip { get; set; }
    public string? ALatitude { get; set; }
    public string? ALongitude { get; set; }
}

public class CreateLocationRequest
{
    public string LLocation { get; set; } = string.Empty;
    public string AAddress1 { get; set; } = string.Empty;
    public string? AAddress2 { get; set; }
    public string ACity { get; set; } = string.Empty;
    public string AState { get; set; } = string.Empty;
    public string AZip { get; set; } = string.Empty;
    public string? ALatitude { get; set; }
    public string? ALongitude { get; set; }
    public string? LPhone { get; set; }
    public string? LHours { get; set; }
    public string? LNote { get; set; }
    public string? LEmail { get; set; }
    public bool LActive { get; set; } = true;
}

public class UpdateLocationRequest
{
    public string LLocation { get; set; } = string.Empty;
    public string AAddress1 { get; set; } = string.Empty;
    public string? AAddress2 { get; set; }
    public string ACity { get; set; } = string.Empty;
    public string AState { get; set; } = string.Empty;
    public string AZip { get; set; } = string.Empty;
    public string? ALatitude { get; set; }
    public string? ALongitude { get; set; }
    public string? LPhone { get; set; }
    public string? LHours { get; set; }
    public string? LNote { get; set; }
    public string? LEmail { get; set; }
    public bool LActive { get; set; } = true;
}
