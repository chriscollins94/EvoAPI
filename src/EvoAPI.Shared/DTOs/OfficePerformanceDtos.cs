namespace EvoAPI.Shared.DTOs;

/// <summary>
/// One office/admin user that holds at least one zone + status assignment
/// (xrefAdminZoneStatusSecondary) — the population of the office performance dashboard.
/// </summary>
public class OfficePerformanceAdminDto
{
    public int UserId { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public int ZoneCount { get; set; }
    public int AssignmentCount { get; set; }
}

/// <summary>
/// A zone an admin is assigned to (used for the dashboard's zone filter).
/// </summary>
public class OfficePerformanceZoneDto
{
    public int ZoneId { get; set; }
    public string? ZoneNumber { get; set; }
    public string? ZoneAcronym { get; set; }
    public string? ZoneDescription { get; set; }
}

/// <summary>
/// Time-in-status metrics for one secondary status: how long service requests sat in it
/// this year (completed stays, from StatusSecondaryChange) and how long the ones sitting
/// in it right now have been there.
/// </summary>
public class OfficeStatusDurationDto
{
    public string GroupKey { get; set; } = string.Empty;      // ss_code, or 'Complete-Other'
    public string StatusName { get; set; } = string.Empty;
    public bool Assigned { get; set; }                        // admin owns this status in >= 1 zone
    public string? AssignedZones { get; set; }                // e.g. "TN1, TN2"
    public int CompletedStintsYtd { get; set; }
    public decimal? AvgDaysYtd { get; set; }
    public int CurrentCount { get; set; }
    public decimal? AvgDaysCurrent { get; set; }
}

/// <summary>
/// Customer-inquiry metrics for one secondary status group: how often SRs entered the
/// status this year, how long they stayed, and how long they had been waiting when a
/// customer called to chase (CustomerInquiry.ci_minutesinstatus, stamped at inquiry time).
/// </summary>
public class OfficeInquiryStatusDto
{
    public string GroupKey { get; set; } = string.Empty;      // ss_code, or 'Complete-Other'
    public string StatusName { get; set; } = string.Empty;
    public bool Assigned { get; set; }                        // admin owns this status in >= 1 zone
    public string? AssignedZones { get; set; }                // e.g. "TN1, TN2"
    public int EntriesYtd { get; set; }
    public decimal? AvgHoursYtd { get; set; }
    public int SrsWithInquiryYtd { get; set; }
    public int InquiryCountYtd { get; set; }
    public decimal? AvgHoursAtInquiry { get; set; }
}

/// <summary>
/// One calendar month of the YTD trend (months are Central time).
/// </summary>
public class OfficeMonthlyTrendDto
{
    public int Month { get; set; }
    public int Opened { get; set; }
    public int Invoiced { get; set; }
}

/// <summary>
/// Everything the office performance dashboard shows for one admin, computed live from
/// ServiceRequest / StatusSecondaryChange / StatusChange / CustomerInquiry, scoped to the
/// zones the admin is assigned in xrefAdminZoneStatusSecondary (optionally one zone).
/// </summary>
public class OfficePerformanceSummaryDto
{
    public int UserId { get; set; }
    public int? ZoneId { get; set; }                          // echo of the filter, null = all zones
    public DateTime YearStart { get; set; }                   // UTC instant the YTD window starts
    public List<OfficePerformanceZoneDto> Zones { get; set; } = new();
    public int OpenNow { get; set; }
    public int OpenedYtd { get; set; }
    public int InvoicedYtd { get; set; }
    public decimal? AvgDaysToInvoice { get; set; }
    public List<OfficeStatusDurationDto> StatusDurations { get; set; } = new();
    public List<OfficeInquiryStatusDto> InquiryStatuses { get; set; } = new();
    public List<OfficeMonthlyTrendDto> MonthlyTrend { get; set; } = new();
}
