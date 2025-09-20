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
    
    /// <summary>
    /// Total violations count
    /// </summary>
    public int TotalViolations => SpeedingOver10 + SpeedingOver20 + HardBreaking + 
                                 HardBreakingSevere + HardAcceleratingSevere + HarshCorneringSevere;
    
    /// <summary>
    /// Get a driving grade based on total violations
    /// A: 0-9 violations, B: 10-19 violations, C: 20-29 violations, D: 30-39 violations, F: 40+ violations
    /// </summary>
    public string Grade
    {
        get
        {
            return TotalViolations switch
            {
                <= 9 => "A",
                <= 19 => "B",
                <= 29 => "C",
                <= 39 => "D",
                _ => "F"
            };
        }
    }
    
    /// <summary>
    /// Get color for the grade (for UI display)
    /// </summary>
    public string GradeColor
    {
        get
        {
            return Grade switch
            {
                "A" => "#22c55e", // green
                "B" => "#84cc16", // lime
                "C" => "#eab308", // yellow
                "D" => "#f97316", // orange
                "F" => "#ef4444", // red
                _ => "#6b7280"    // gray
            };
        }
    }
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
    
    /// <summary>
    /// Total violations count
    /// </summary>
    public int TotalViolations => SpeedingOver10 + SpeedingOver20 + HardBreaking + 
                                 HardBreakingSevere + HardAcceleratingSevere + HarshCorneringSevere;
    
    /// <summary>
    /// Get a driving grade based on total violations
    /// A: 0-9 violations, B: 10-19 violations, C: 20-29 violations, D: 30-39 violations, F: 40+ violations
    /// </summary>
    public string Grade
    {
        get
        {
            return TotalViolations switch
            {
                <= 9 => "A",
                <= 19 => "B",
                <= 29 => "C",
                <= 39 => "D",
                _ => "F"
            };
        }
    }
    
    /// <summary>
    /// Get color for the grade (for UI display)
    /// </summary>
    public string GradeColor
    {
        get
        {
            return Grade switch
            {
                "A" => "#22c55e", // green
                "B" => "#84cc16", // lime
                "C" => "#eab308", // yellow
                "D" => "#f97316", // orange
                "F" => "#ef4444", // red
                _ => "#6b7280"    // gray
            };
        }
    }
}
