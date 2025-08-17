namespace EvoAPI.Shared.DTOs;

public class HighVolumeReportDto
{
    public string Tech { get; set; } = string.Empty;
    public int Today { get; set; }
    public int Previous1 { get; set; }
    public int Previous2 { get; set; }
    public int Previous3 { get; set; }
    public int Previous4 { get; set; }
    public string TodayName { get; set; } = string.Empty;
    public string Previous1Name { get; set; } = string.Empty;
    public string Previous2Name { get; set; } = string.Empty;
    public string Previous3Name { get; set; } = string.Empty;
    public string Previous4Name { get; set; } = string.Empty;
    public int NotCompleted { get; set; } 
}
