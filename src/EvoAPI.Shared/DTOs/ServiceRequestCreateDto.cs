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

    /// <summary>Agency, chosen from the ServiceRequestAgencies ConfigSetting list. Null if not recorded.</summary>
    public string? SrAgency { get; set; }

    /// <summary>How the request came in: "Phone Call", "Email" or "Portal". Null if not recorded.</summary>
    public string? SrMethodOfRequest { get; set; }

    // Phone Call path: details of the person who called in the request.
    public string? SrRequestorName { get; set; }
    public string? SrRequestorEmail { get; set; }
    public string? SrRequestorPhone { get; set; }

    /// <summary>Email path: the full pasted email body, including sender info.</summary>
    public string? SrRequestEmailText { get; set; }

    // Portal path: where the request lives and any notes about it.
    public string? SrPortalUrl { get; set; }
    public string? SrPortalNote { get; set; }

    // Point of contact at the job site.
    public string? SrSiteContactName { get; set; }
    public string? SrSiteContactPhone { get; set; }
    public string? SrSiteContactEmail { get; set; }

    /// <summary>True when a purchase order is required. The PO provider fields below only carry values when set.</summary>
    public bool SrPoRequired { get; set; }
    public string? SrPoProviderName { get; set; }
    public string? SrPoProviderPhone { get; set; }
    public string? SrPoProviderEmail { get; set; }

    /// <summary>Email to submit the invoice to (defaulted from the company's 'Billing' contact, editable).</summary>
    public string? SrInvoiceEmail { get; set; }

    /// <summary>Email to submit the quote to (defaulted from the company's 'Quote' contact, editable).</summary>
    public string? SrQuoteEmail { get; set; }
}

public class CreateServiceRequestResponse
{
    public int SrId { get; set; }
    public string SrRequestNumber { get; set; } = string.Empty;
    public string WoWorkOrderNumber { get; set; } = string.Empty;
}

/// <summary>
/// Assign one or more technicians to a just-created Service Request, one Work Order per
/// tech: the first assignment takes the (still unassigned) primary WO, each additional
/// tech gets a newly numbered WO — the same shape the legacy schedule page produces via
/// UpdateWorkOrderAssignOrCreate.
/// </summary>
public class AssignServiceRequestTechniciansRequest
{
    public int SrId { get; set; }
    public List<ServiceRequestTechAssignment> Assignments { get; set; } = new();
}

public class ServiceRequestTechAssignment
{
    public int UId { get; set; }

    /// <summary>Scheduled start, UTC (wo_startdatetime is stored UTC).</summary>
    public DateTime StartDateTimeUtc { get; set; }

    /// <summary>Scheduled end, UTC.</summary>
    public DateTime EndDateTimeUtc { get; set; }
}

public class AssignServiceRequestTechniciansResponse
{
    public int SrId { get; set; }
    public List<AssignedWorkOrderDto> WorkOrders { get; set; } = new();
}

public class AssignedWorkOrderDto
{
    public int WoId { get; set; }
    public string WoWorkOrderNumber { get; set; } = string.Empty;
    public int UId { get; set; }
}

/// <summary>
/// Pre-create double-booking check: for each proposed tech + time window, find existing
/// open work orders (Unassigned/Assigned/Incomplete) already scheduled for that tech that
/// overlap the window. Informational only — assignment is still allowed.
/// </summary>
public class TechScheduleConflictsRequest
{
    public List<ServiceRequestTechAssignment> Assignments { get; set; } = new();
}

public class TechScheduleConflictDto
{
    public int UId { get; set; }
    public int WoId { get; set; }
    public string WoWorkOrderNumber { get; set; } = string.Empty;
    public DateTime StartDateTimeUtc { get; set; }
    public DateTime EndDateTimeUtc { get; set; }
    public string? Location { get; set; }
    public string? Status { get; set; }
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
/// NTE guidance for the New Service Request flow: a recommended Not-To-Exceed built from
/// historical jobs for the selected company + sub-trade, decomposed into displayable line
/// items. Labor is re-priced at today's rates (by the rate type each past job used),
/// materials are billable pre-tax, and each component is taken at the configured percentile.
/// </summary>
public class NteEstimateDto
{
    public bool HasEnoughHistory { get; set; }
    public int JobCount { get; set; }
    public int WindowMonths { get; set; }
    public int Percentile { get; set; }
    public decimal? AvgHours { get; set; }
    public decimal? LaborHours { get; set; }   // percentile hours, for the "~X hrs" note
    public decimal? LaborAmount { get; set; }
    public decimal? MaterialsAmount { get; set; }
    public decimal TripCharge { get; set; }
    public decimal? EstimatedNte { get; set; }
    public decimal? TradeNte { get; set; }      // trade-level NTE shown as a reference regardless
    public List<NteEstimateLineDto> Lines { get; set; } = new();
}

public class NteEstimateLineDto
{
    public string Label { get; set; } = string.Empty;
    public decimal Value { get; set; }
    public string? Note { get; set; }
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
