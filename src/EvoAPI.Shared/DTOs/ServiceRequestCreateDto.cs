namespace EvoAPI.Shared.DTOs;

/// <summary>
/// Payload for creating a new Service Request (and its primary Work Order).
/// Mirrors the legacy EvoWS InsertServiceRequest contract so the created
/// records match what the old schedule page produces. Supports both the
/// "Accepted" path and the "Reject" path (caller sets PId/SsId/sentinel
/// numbers accordingly, exactly as the old UI did).
/// </summary>
public class CreateServiceRequestRequest
{
    public int XcccId { get; set; }
    public int LId { get; set; }
    public int TId { get; set; }
    public int PId { get; set; }

    /// <summary>Labor rate type (pay rate). 0 for the Reject path, which carries no pay rate.</summary>
    public int LrtId { get; set; }

    /// <summary>Status secondary. Null defaults to 1 (Accepted) on the Accepted path; set explicitly for Reject.</summary>
    public int? SsId { get; set; }

    public string? SrSummary { get; set; }
    public string SrRequestNumber { get; set; } = string.Empty;
    public string WoWorkOrderNumber { get; set; } = string.Empty;
    public string? SrIvrRequestNumber { get; set; }
    public string? SrCallNote { get; set; }
    public string? SrOfficeNote { get; set; }

    public decimal? SrNte { get; set; }

    /// <summary>Trip charge (company default, or trade-level override). Used for both worked and quote columns, matching legacy.</summary>
    public decimal? SrTripChargeWorked { get; set; }

    public bool SrRequiresPreArrivalCall { get; set; }
    public bool SrShiftDifferential { get; set; }
}

public class CreateServiceRequestResponse
{
    public int SrId { get; set; }
    public string SrRequestNumber { get; set; } = string.Empty;
    public string WoWorkOrderNumber { get; set; } = string.Empty;
}

/// <summary>
/// Client-reported attachment upload failure, logged to the audit table so it can be
/// troubleshooted later (the office user won't see the browser console).
/// </summary>
public class ServiceRequestAttachmentErrorRequest
{
    public int SrId { get; set; }
    public string? FileName { get; set; }
    public string? Message { get; set; }
}

/// <summary>
/// One technician's scheduling picture for the New Service Request "tech guidance" panel:
/// distance from the job, skill level for the trade, and hours already booked across the
/// next 7 days (today = Today0).
/// </summary>
public class TechUtilizationDto
{
    public int UId { get; set; }
    public string UFirstName { get; set; } = string.Empty;
    public string ULastName { get; set; } = string.Empty;
    public int? SlScore { get; set; }
    public string? SlDescription { get; set; }
    public double? DistanceMiles { get; set; }
    public int Today0 { get; set; }
    public int Today1 { get; set; }
    public int Today2 { get; set; }
    public int Today3 { get; set; }
    public int Today4 { get; set; }
    public int Today5 { get; set; }
    public int Today6 { get; set; }
}
