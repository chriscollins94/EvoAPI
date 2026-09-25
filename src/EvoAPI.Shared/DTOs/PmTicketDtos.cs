namespace EvoAPI.Shared.DTOs;

// ---------------------------------------------------------------------------------------------
// Preventative Maintenance build slice 3: ticket entry (New Service Request, Preventative path).
// Tables: sql/migrations/2026-09-24_create_pm_ticket_tables.sql (ServiceType, PMVisit, PMVisitUnit,
// ServiceRequest.svt_id / fr_id / sr_pmunitcount / sr_servicebydate, Priority.p_allowpreventative).
// ---------------------------------------------------------------------------------------------

#region lookups

public class ServiceTypeDto
{
    public int SvtId { get; set; }
    public string ServiceType { get; set; } = string.Empty;
    public string Code { get; set; } = string.Empty;        // Reactionary | Preventative | Proposal | Administrative
    public string? Description { get; set; }
    public int Order { get; set; }
    public bool Active { get; set; }
}

/// <summary>
/// A PM sub-trade the company can enter a Preventative ticket under: the company has a labor rate for it and its
/// parent trade has an active form rule. Season fields come from the rule's PMSeason row for that sub-trade when one exists.
/// </summary>
public class PmTicketTradeDto
{
    public int TId { get; set; }
    public string Trade { get; set; } = string.Empty;
    public int ParentTId { get; set; }
    public string ParentTrade { get; set; } = string.Empty;
    public int LrId { get; set; }
    public int FrId { get; set; }
    public int? PmsId { get; set; }
    public string? SeasonName { get; set; }
    public string? VisitType { get; set; }
    public string? StartMonthDay { get; set; }
    public string? EndMonthDay { get; set; }
}

#endregion

#region first-time detection

public class PmFirstTimeDto
{
    /// <summary>True when nothing below was found: this would be the first PM at the location for the parent trade.</summary>
    public bool FirstTime { get; set; }
    /// <summary>Earlier PMVisit rows at this location for the parent trade (any status except a rejected ticket).</summary>
    public int PriorVisitCount { get; set; }
    /// <summary>Earlier legacy tickets (no PMVisit) under a PM sub-trade of the parent trade at this location whose status is Complete, Invoiced or Paid.</summary>
    public int PriorLegacyCount { get; set; }
    public DateTime? LastVisitDate { get; set; }
    public string Detail { get; set; } = string.Empty;
}

#endregion

#region pricing

public class PmPriceUnitRequest
{
    public int Sequence { get; set; }
    public int? AsId { get; set; }
    public int? AscId { get; set; }
    public decimal? CapacityTons { get; set; }
    public string? Label { get; set; }
}

public class PmPriceAddOnRequest
{
    public int PmrtId { get; set; }
    public int Quantity { get; set; } = 1;
}

public class PmPriceRequest
{
    public int XcccId { get; set; }
    /// <summary>Parent trade the rule is keyed by (HVAC), not the PM sub-trade.</summary>
    public int TId { get; set; }
    public bool FirstTime { get; set; }
    /// <summary>Company / trade trip charge to use when the rule has none and the first-time rule adds a trip charge.</summary>
    public decimal? FallbackTripCharge { get; set; }
    public List<PmPriceUnitRequest> Units { get; set; } = new();
    public List<PmPriceAddOnRequest> AddOns { get; set; } = new();
    /// <summary>Office override of the computed total (kept in the breakdown as its own line).</summary>
    public decimal? OverrideTotal { get; set; }
    public string? OverrideReason { get; set; }
}

public class PmPriceLineDto
{
    public string Kind { get; set; } = string.Empty;        // Unit | AddOn | TripCharge | Override
    public int? Sequence { get; set; }
    public string Label { get; set; } = string.Empty;
    public int Quantity { get; set; } = 1;
    public decimal UnitPrice { get; set; }
    public decimal Amount { get; set; }
    public int? PmrtId { get; set; }
    public string? TierLabel { get; set; }
    /// <summary>False when no tier row fits the unit (price 0, warning added).</summary>
    public bool Matched { get; set; } = true;
    public string? Note { get; set; }
}

public class PmPriceResponse
{
    public string? BillingMode { get; set; }
    public string? NteGuideline { get; set; }
    public string? FirstTimeRule { get; set; }
    public bool FirstTime { get; set; }
    public List<PmPriceLineDto> Lines { get; set; } = new();
    public decimal UnitsTotal { get; set; }
    public decimal AddOnsTotal { get; set; }
    public decimal TripCharge { get; set; }
    public decimal ComputedTotal { get; set; }
    /// <summary>The override when one was given, else the computed total.</summary>
    public decimal Total { get; set; }
    public decimal? OverrideTotal { get; set; }
    public string? OverrideReason { get; set; }
    public List<string> Warnings { get; set; } = new();
    public DateTime CalculatedAt { get; set; } = DateTime.Now;
}

#endregion

#region visit snapshot

public class PmVisitUnitRequest
{
    public int Sequence { get; set; }
    public int? AsId { get; set; }
    public int? AscId { get; set; }
    public decimal? CapacityTons { get; set; }
    public string? Label { get; set; }
    public decimal? Price { get; set; }
    public string? TierLabel { get; set; }
}

/// <summary>
/// Creates the PMVisit snapshot for a service request that was just created. Idempotent per SR: a second call
/// returns the existing visit untouched.
/// </summary>
public class CreatePmVisitRequest
{
    public int SrId { get; set; }
    public int XcccId { get; set; }
    /// <summary>Parent trade (the rule key).</summary>
    public int TId { get; set; }
    /// <summary>PM sub-trade the ticket was entered under.</summary>
    public int TIdSeason { get; set; }
    public int? PmsId { get; set; }
    public string? VisitType { get; set; }
    public bool FirstTime { get; set; }
    public string? FirstTimeSource { get; set; }          // Auto | Office
    public bool ParamsConfirmed { get; set; }
    public decimal? ContractTotal { get; set; }
    public PmPriceResponse? Pricing { get; set; }
    public List<PmVisitUnitRequest> Units { get; set; } = new();
}

public class PmVisitUnitDto
{
    public int PmvuId { get; set; }
    public int PmvId { get; set; }
    public int Sequence { get; set; }
    public string Source { get; set; } = string.Empty;
    public int? AsId { get; set; }
    public int? AscId { get; set; }
    public string? EquipmentType { get; set; }
    public decimal? CapacityTons { get; set; }
    public string? Label { get; set; }
    public decimal? Price { get; set; }
    public string? TierLabel { get; set; }
    public string Status { get; set; } = string.Empty;
    public bool AssetConfirmed { get; set; }
}

public class PmVisitDto
{
    public int PmvId { get; set; }
    public int SrId { get; set; }
    public int? FrId { get; set; }
    public int? FtId { get; set; }
    public int? FtVersion { get; set; }
    public int? PmsId { get; set; }
    public string? VisitType { get; set; }
    public bool FirstTime { get; set; }
    public string? FirstTimeSource { get; set; }
    public string? BillingMode { get; set; }
    public string? NteGuideline { get; set; }
    public string? FirstTimeRule { get; set; }
    public decimal? ContractTotal { get; set; }
    public string? PricingJson { get; set; }
    public int? CoolingSetpoint { get; set; }
    public int? HeatingSetpoint { get; set; }
    public string? ThermostatSchedule { get; set; }
    public string? ThermostatScheduleNote { get; set; }
    public bool? ThermostatLock { get; set; }
    public bool? AntiAlgae { get; set; }
    public bool? PhotoTimestamp { get; set; }
    public bool? ManagerSeesOldFilters { get; set; }
    public bool? PricingDiscussAllowed { get; set; }
    public bool? ImmediateQuoteRequired { get; set; }
    public bool? ImmediateCallIfIncomplete { get; set; }
    public string? SubmissionDeadline { get; set; }
    public string? SubmissionDeadlineNote { get; set; }
    public string? CloseoutDocs { get; set; }
    public string? SubmissionDestination { get; set; }
    public bool? IvrRequired { get; set; }
    public string? CustomerForm { get; set; }
    public string? CustomerFormUrl { get; set; }
    public int? AttIdCustomerForm { get; set; }
    public string? CustomerFormNote { get; set; }
    public int? ParamsConfirmedUId { get; set; }
    public DateTime? ParamsConfirmedDateTime { get; set; }
    public string Status { get; set; } = string.Empty;
    public DateTime InsertDateTime { get; set; }
    public List<PmVisitUnitDto> Units { get; set; } = new();
}

#endregion
