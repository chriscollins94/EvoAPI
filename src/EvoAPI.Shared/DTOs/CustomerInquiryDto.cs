namespace EvoAPI.Shared.DTOs;

public class CreateCustomerInquiryRequest
{
    public int SrId { get; set; }
    public string? Note { get; set; }
}

public class CustomerInquiryDto
{
    public int CiId { get; set; }
    public int SrId { get; set; }
    public int? WoId { get; set; }
    public int UId { get; set; }
    public int? SsId { get; set; }
    public string SecondaryStatus { get; set; } = string.Empty;
    public string RequestNumber { get; set; } = string.Empty;
    public DateTime? StatusStartDateTime { get; set; }
    public int? MinutesInStatus { get; set; }
    public string? Note { get; set; }
    public DateTime InsertDateTime { get; set; }
}

/// Whether a service request can accept a new inquiry right now, and what blocks it if not.
/// Also carries the live status detail the inquiry modal shows.
public class CustomerInquiryEligibilityDto
{
    public int SrId { get; set; }
    public bool CanLog { get; set; }
    public int CooldownHours { get; set; }
    public string RequestNumber { get; set; } = string.Empty;
    public string SecondaryStatus { get; set; } = string.Empty;
    public int? MinutesInStatus { get; set; }
    public DateTime? LastInquiryDateTime { get; set; }
    public string? LastInquiryUser { get; set; }
    public string? LastInquiryNote { get; set; }
    public DateTime? NextAllowedDateTime { get; set; }
}

public enum CustomerInquiryCreateStatus
{
    Created,
    ServiceRequestNotFound,
    WithinCooldown
}

public class CreateCustomerInquiryResult
{
    public CustomerInquiryCreateStatus Status { get; set; }
    public CustomerInquiryDto? Inquiry { get; set; }

    /// Populated when Status is WithinCooldown, so the caller can say who logged the last one.
    public CustomerInquiryEligibilityDto? Eligibility { get; set; }
}
