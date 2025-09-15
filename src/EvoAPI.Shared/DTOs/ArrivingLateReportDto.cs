namespace EvoAPI.Shared.DTOs;

public class ArrivingLateReportDto
{
    public string CallCenterName { get; set; } = string.Empty;
    public string Trade { get; set; } = string.Empty;
    public string WorkOrderNumber { get; set; } = string.Empty;
    public int SrId { get; set; }
    public int UserId { get; set; }
    public string EmployeeNumber { get; set; } = string.Empty;
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string VehicleNumber { get; set; } = string.Empty;
    public string StartDateTime { get; set; } = string.Empty;
    public string Address1 { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string State { get; set; } = string.Empty;
    public string Zip { get; set; } = string.Empty;
    
    // Calculated fields for arrival analysis
    public string TechnicianName { get; set; } = string.Empty;
    public string FullAddress { get; set; } = string.Empty;
    public string CurrentVehicleLocation { get; set; } = string.Empty;
    public double? TravelTimeMinutes { get; set; }
    public double? TravelTimeWithTrafficMinutes { get; set; }
    public double? DistanceMiles { get; set; }
    public string RiskLevel { get; set; } = string.Empty;
    public bool RequiresImmediateDeparture { get; set; }
    public int MinutesUntilStart { get; set; }
    public DateTime? ParsedStartDateTime { get; set; }
    public string DataSource { get; set; } = string.Empty; // google_maps, database_cache, estimated
}
