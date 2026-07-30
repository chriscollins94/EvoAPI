namespace EvoAPI.Shared.DTOs;

/// <summary>
/// One upload batch of performance data (Employee or Zone spreadsheet).
/// </summary>
public class PerformanceUploadDto
{
    public int UploadId { get; set; }
    public string Type { get; set; } = string.Empty;   // 'Employee' | 'Zone'
    public DateTime ReportDate { get; set; }
    public string? Filename { get; set; }
    public int UploadedById { get; set; }
    public string? UploadedByName { get; set; }
    public int RowCount { get; set; }
    public int SkippedCount { get; set; }
    public DateTime InsertDateTime { get; set; }
}

/// <summary>
/// Per-employee performance snapshot (one row of one upload batch).
/// </summary>
public class PerformanceEmployeeDto
{
    public int UserId { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string? EmployeeNumber { get; set; }
    public string? ZoneAcronym { get; set; }
    public int UploadId { get; set; }
    public DateTime ReportDate { get; set; }
    public decimal? Utilization { get; set; }
    public decimal? AchLaborTrip { get; set; }
    public decimal? CallOuts { get; set; }
    public decimal? ServiceItemsPayback { get; set; }
    public decimal? TruckFuelEfficiency { get; set; }
    public decimal? GallonsPerDay { get; set; }
    public decimal? GrossMargin { get; set; }
    public decimal? ReceiptsViolations { get; set; }
    public decimal? Callbacks { get; set; }
    public decimal? PendingTechInfo { get; set; }
    public decimal? PositiveQtrPct { get; set; }
    public string? ProfitGrade { get; set; }
    public decimal? HealthScore { get; set; }
}

/// <summary>
/// Per-zone performance snapshot (one row of one upload batch).
/// </summary>
public class PerformanceZoneDto
{
    public int ZoneId { get; set; }
    public string ZoneAcronym { get; set; } = string.Empty;
    public string? ZoneDescription { get; set; }
    public int UploadId { get; set; }
    public DateTime ReportDate { get; set; }

    // Zone-file-only metrics. The technician file has no equivalent columns, so
    // these live on the zone DTO alone. Both are untargeted.
    public decimal? RevPerTechPerDay { get; set; }
    public decimal? YtdContribution { get; set; }

    public decimal? Utilization { get; set; }
    public decimal? AchLaborTrip { get; set; }
    public decimal? CallOuts { get; set; }
    public decimal? ServiceItemsPayback { get; set; }
    public decimal? TruckFuelEfficiency { get; set; }
    public decimal? GallonsPerDay { get; set; }
    public decimal? GrossMargin { get; set; }
    public decimal? ReceiptsViolations { get; set; }
    public decimal? Callbacks { get; set; }
    public decimal? PendingTechInfo { get; set; }
    public decimal? PositiveQtrPct { get; set; }
}

/// <summary>
/// A configurable metric target (ConfigSetting row, cs_type = 'PerformanceTarget').
/// </summary>
public class PerformanceTargetDto
{
    public string Identifier { get; set; } = string.Empty;   // e.g. 'Perf.Utilization'
    public decimal? Value { get; set; }
    public string? Description { get; set; }
}

/// <summary>
/// One parsed employee row sent up from the admin upload page.
/// </summary>
public class PerformanceEmployeeRowDto
{
    public int? UserId { get; set; }
    public string? EmployeeNumber { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public decimal? Utilization { get; set; }
    public decimal? AchLaborTrip { get; set; }
    public decimal? CallOuts { get; set; }
    public decimal? ServiceItemsPayback { get; set; }
    public decimal? TruckFuelEfficiency { get; set; }
    public decimal? GallonsPerDay { get; set; }
    public decimal? GrossMargin { get; set; }
    public decimal? ReceiptsViolations { get; set; }
    public decimal? Callbacks { get; set; }
    public decimal? PendingTechInfo { get; set; }
    public decimal? PositiveQtrPct { get; set; }
    public string? ProfitGrade { get; set; }
    public decimal? HealthScore { get; set; }
}

/// <summary>
/// One parsed zone summary row sent up from the admin upload page.
/// </summary>
public class PerformanceZoneRowDto
{
    public string ZoneAcronym { get; set; } = string.Empty;
    public decimal? RevPerTechPerDay { get; set; }
    public decimal? YtdContribution { get; set; }
    public decimal? Utilization { get; set; }
    public decimal? AchLaborTrip { get; set; }
    public decimal? CallOuts { get; set; }
    public decimal? ServiceItemsPayback { get; set; }
    public decimal? TruckFuelEfficiency { get; set; }
    public decimal? GallonsPerDay { get; set; }
    public decimal? GrossMargin { get; set; }
    public decimal? ReceiptsViolations { get; set; }
    public decimal? Callbacks { get; set; }
    public decimal? PendingTechInfo { get; set; }
    public decimal? PositiveQtrPct { get; set; }
}

/// <summary>
/// One pie slice of the job-mix breakdown.
/// </summary>
public class JobMixSliceDto
{
    public string Label { get; set; } = string.Empty;
    public int Count { get; set; }
}

/// <summary>
/// Job-mix breakdown for a technician: distinct service requests created in the
/// lookback window, grouped three ways for the report's pie charts.
/// </summary>
public class PerformanceJobMixDto
{
    public int LookbackDays { get; set; }
    public int TotalJobs { get; set; }
    public List<JobMixSliceDto> ParentTrades { get; set; } = new();
    public List<JobMixSliceDto> SubTrades { get; set; } = new();
    public List<JobMixSliceDto> CallCenters { get; set; } = new();
}

/// <summary>
/// Result of an upload: what was inserted and which rows could not be matched.
/// </summary>
public class PerformanceUploadResultDto
{
    public int UploadId { get; set; }
    public int Inserted { get; set; }
    public int Skipped { get; set; }
    public List<string> UnmatchedRows { get; set; } = new();
}
