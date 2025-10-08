namespace EvoAPI.Shared.DTOs;

public class DrivingScorecard
{
    public int UserId { get; set; }
    public int SpeedingOver10 { get; set; }
    public int SpeedingOver20 { get; set; }
    public int HardBreaking { get; set; }
    public int HardBreakingSevere { get; set; }
    public int HardAcceleratingSevere { get; set; }
    public int HarshCorneringSevere { get; set; }
    
    // Note: TotalViolations, Grade, and GradeColor are now calculated on the frontend
    // to account for Math.max(0, ...) logic that prevents negative values in certain columns
}

public class DrivingScorecardWithTechnicianInfo
{
    public int UserId { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string EmployeeNumber { get; set; } = string.Empty;
    public int SpeedingOver10 { get; set; }
    public int SpeedingOver20 { get; set; }
    public int HardBreaking { get; set; }
    public int HardBreakingSevere { get; set; }
    public int HardAcceleratingSevere { get; set; }
    public int HarshCorneringSevere { get; set; }
    
    /// <summary>
    /// Full technician name
    /// </summary>
    public string TechnicianName => $"{FirstName} {LastName}".Trim();
    
    // Note: TotalViolations, Grade, and GradeColor are now calculated on the frontend
    // to account for Math.max(0, ...) logic that prevents negative values in certain columns
}
