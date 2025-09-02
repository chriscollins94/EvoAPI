namespace EvoAPI.Shared.DTOs;

public class WorkOrderSchedulingConflictsDto
{
    public string TechnicianName { get; set; } = string.Empty;
    public string ConflictRisk { get; set; } = string.Empty;
    public int TravelTimeMinutes { get; set; }
    public string GeographicProximity { get; set; } = string.Empty;
    
    // Current work order details
    public string CurrentWorkOrder { get; set; } = string.Empty;
    public int CurrentSrId { get; set; }
    public string CurrentStartFormatted { get; set; } = string.Empty;
    public string CurrentEndFormatted { get; set; } = string.Empty;
    public string CurrentDescription { get; set; } = string.Empty;
    public string CurrentAddress { get; set; } = string.Empty;
    public string CurrentLocation { get; set; } = string.Empty;
    
    // Next work order details
    public string NextWorkOrder { get; set; } = string.Empty;
    public int NextSrId { get; set; }
    public string NextStartFormatted { get; set; } = string.Empty;
    public string NextEndFormatted { get; set; } = string.Empty;
    public string NextDescription { get; set; } = string.Empty;
    public string NextAddress { get; set; } = string.Empty;
    public string NextLocation { get; set; } = string.Empty;
    
    // Company and call center information
    public string CurrentCompany { get; set; } = string.Empty;
    public string CurrentCallCenter { get; set; } = string.Empty;
    public string NextCompany { get; set; } = string.Empty;
    public string NextCallCenter { get; set; } = string.Empty;
    
    // Organization change indicator
    public string OrganizationChange { get; set; } = string.Empty;
}

public class WorkOrderSchedulingConflictsSummaryDto
{
    public string ConflictRisk { get; set; } = string.Empty;
    public int ConflictCount { get; set; }
    public decimal AverageTravelTime { get; set; }
    public int MinTravelTime { get; set; }
    public int MaxTravelTime { get; set; }
}

public class WorkOrderSchedulingConflictsReportDto
{
    public List<WorkOrderSchedulingConflictsDto> Conflicts { get; set; } = new List<WorkOrderSchedulingConflictsDto>();
    public List<WorkOrderSchedulingConflictsSummaryDto> Summary { get; set; } = new List<WorkOrderSchedulingConflictsSummaryDto>();
}
