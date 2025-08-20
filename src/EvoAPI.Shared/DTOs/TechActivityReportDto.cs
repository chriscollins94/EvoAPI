namespace EvoAPI.Shared.DTOs;

public class TechActivityReportDto
{
    public int TtId { get; set; }
    public int TttId { get; set; }
    public int UserId { get; set; }
    public int? WorkOrderId { get; set; }
    public int? ServiceRequestId { get; set; }
    public int? TradeId { get; set; }
    public string TimeType { get; set; } = string.Empty;
    public string PaidTime { get; set; } = string.Empty;
    public string BeginTime { get; set; } = string.Empty;
    public string EndTime { get; set; } = string.Empty;
    public decimal InvoicedRate { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string FullName => $"{FirstName} {LastName}".Trim();
    public string ServiceRequestNumber { get; set; } = string.Empty;
    public string Trade { get; set; } = string.Empty;
    public string CallCenter { get; set; } = string.Empty;
    public string Company { get; set; } = string.Empty;
    
    // Tech Zone fields (from user table)
    public int? TechZoneId { get; set; }
    public string TechZone { get; set; } = string.Empty;
    
    // Service Request Zone fields (from location/address/tax/zone lookup)
    public int? ServiceRequestZoneId { get; set; }
    public string ServiceRequestZone { get; set; } = string.Empty;
    
    // Calculated properties for reporting
    public DateTime ParsedBeginTime => DateTime.TryParse(BeginTime, out var begin) ? begin : DateTime.MinValue;
    public DateTime ParsedEndTime => DateTime.TryParse(EndTime, out var end) ? end : DateTime.MinValue;
    public double DurationHours => ParsedEndTime > ParsedBeginTime ? (ParsedEndTime - ParsedBeginTime).TotalHours : 0;
    public decimal TotalCost => (decimal)DurationHours * InvoicedRate;
}
