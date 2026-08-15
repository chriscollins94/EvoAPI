namespace EvoAPI.Shared.DTOs;

public class StatusChangeHistoryRowDto
{
    public int ScId { get; set; }
    public int SrId { get; set; }
    public string SrRequestNumber { get; set; } = string.Empty;
    public string Trade { get; set; } = string.Empty;
    public string? StatusPrior { get; set; }
    public string StatusNew { get; set; } = string.Empty;
    public int? MinutesInPriorStatus { get; set; }
    public decimal? HoursInPriorStatus { get; set; }
    public decimal? DaysInPriorStatus { get; set; }
    public DateTime? ChangeDateTime { get; set; }
}

public class StatusChangeHistorySummaryDto
{
    public int SIdNew { get; set; }
    public string StatusNew { get; set; } = string.Empty;
    public int UniqueSrCount { get; set; }
    public decimal PercentOfSrs { get; set; }
    public int TransitionCount { get; set; }
    public decimal AvgTransitionsPerSr { get; set; }
}

public class StatusChangeHistoryOptionDto
{
    public int SId { get; set; }
    public string Status { get; set; } = string.Empty;
}
