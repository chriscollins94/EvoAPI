namespace EvoAPI.Shared.DTOs;

public class TimecardDiscrepanciesDto
{
    public int TtdId { get; set; }
    public int UserId { get; set; }
    public string TechnicianName { get; set; } = string.Empty;
    public string EmployeeNumber { get; set; } = string.Empty;
    public int TttId { get; set; }
    public string TrackingType { get; set; } = string.Empty;
    public int WorkOrderId { get; set; }
    public string WorkOrderNumber { get; set; } = string.Empty;
    public DateTime InsertDateTime { get; set; }
    
    // GPS Coordinates
    public decimal BrowserLat { get; set; }
    public decimal BrowserLong { get; set; }
    public decimal FleetmaticsLat { get; set; }
    public decimal FleetmaticsLong { get; set; }
    
    // Tracking Details
    public string TtdType { get; set; } = string.Empty;
    public DateTime? WoStartDateTime { get; set; }
    public DateTime? WoEndDateTime { get; set; }
    
    // Distance and Time Metrics
    public decimal DistanceFromStart { get; set; }
    public decimal DistanceFromEnd { get; set; }
    public int TravelTimeToStart { get; set; }
    public int TravelTimeToEnd { get; set; }
    
    // Organization Details
    public string CompanyName { get; set; } = string.Empty;
    public string CallCenterName { get; set; } = string.Empty;
    public string LocationName { get; set; } = string.Empty;
    public string WorkOrderAddress { get; set; } = string.Empty;
}
