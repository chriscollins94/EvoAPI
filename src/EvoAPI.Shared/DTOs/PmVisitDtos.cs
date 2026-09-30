namespace EvoAPI.Shared.DTOs;

// ---------------------------------------------------------------------------------------------
// Preventative Maintenance build slice 4a: the technician's PM visit.
// Table: sql/migrations/2026-09-25_create_pm_visit_answer_table.sql (PMVisitAnswer); PMVisit / PMVisitUnit from slice 3.
// ---------------------------------------------------------------------------------------------

/// <summary>One row on the tech's "My PM visits" list: a Preventative work order assigned to them plus its visit state.</summary>
public class PmVisitListItemDto
{
    public int SrId { get; set; }
    public int? WoId { get; set; }
    public string RequestNumber { get; set; } = string.Empty;
    public string? WoNumber { get; set; }
    public string CallCenter { get; set; } = string.Empty;
    public string Company { get; set; } = string.Empty;
    public string Location { get; set; } = string.Empty;
    public string? Address { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string Trade { get; set; } = string.Empty;
    public string? VisitType { get; set; }
    public DateTime? StartDateTime { get; set; }           // wo_startdatetime (UTC as stored)
    public DateTime? EndDateTime { get; set; }
    public DateTime? ServiceByDate { get; set; }
    public string? SecondaryStatus { get; set; }
    public string? StatusColor { get; set; }
    public string? AssignedFirstName { get; set; }
    public string? AssignedLastName { get; set; }
    public int PmvId { get; set; }
    public string PmvStatus { get; set; } = string.Empty;
    public int UnitCount { get; set; }
    public bool CheckedIn { get; set; }
}

/// <summary>Ticket, site and work-order facts the visit page shows in its header and Site tab.</summary>
public class PmVisitHeaderDto
{
    public int SrId { get; set; }
    public string RequestNumber { get; set; } = string.Empty;
    public int? WoId { get; set; }                          // the caller's work order on this SR (primary WO for an admin who is not assigned)
    public string? WoNumber { get; set; }
    public int XcccId { get; set; }
    public int LId { get; set; }
    public int TIdSub { get; set; }                         // PM sub-trade the ticket was entered under
    public int TIdParent { get; set; }                      // parent trade the rule and template are keyed by
    public string Trade { get; set; } = string.Empty;
    public string CallCenter { get; set; } = string.Empty;
    public string Company { get; set; } = string.Empty;
    public string Location { get; set; } = string.Empty;
    public string? Address1 { get; set; }
    public string? Address2 { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? Zip { get; set; }
    public decimal? Latitude { get; set; }
    public decimal? Longitude { get; set; }
    public string? LocationPhone { get; set; }
    public string? LocationHours { get; set; }
    public string? LocationNote { get; set; }
    public string? CallNote { get; set; }
    public string? IvrRequestNumber { get; set; }
    public DateTime? ServiceByDate { get; set; }
    public int? PmUnitCount { get; set; }
    public decimal? Nte { get; set; }
    public string? FlatOrHourly { get; set; }
    public decimal? RateFlat { get; set; }
    public DateTime? StartDateTime { get; set; }
    public DateTime? EndDateTime { get; set; }
    public string? SecondaryStatus { get; set; }
    public string? SecondaryStatusCode { get; set; }
    public string? SiteContactName { get; set; }
    public string? SiteContactPhone { get; set; }
    public bool AssignedToCaller { get; set; }
    public bool CheckedIn { get; set; }
    public DateTime? CheckInDateTime { get; set; }
}

public class PmVisitAnswerDto
{
    public int PmvaId { get; set; }
    public int PmvId { get; set; }
    public int? PmvuId { get; set; }
    public int FqId { get; set; }
    public string? Code { get; set; }
    public int RepeatIndex { get; set; }
    public string? Question { get; set; }
    public string? Answer { get; set; }
    public decimal? Numeric { get; set; }
    public int? AttId { get; set; }
    public string? AttFilename { get; set; }
    public decimal? Latitude { get; set; }
    public decimal? Longitude { get; set; }
    public int? UId { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
}

public class SavePmVisitAnswerRequest
{
    public int? PmvuId { get; set; }
    public int FqId { get; set; }
    public int RepeatIndex { get; set; }
    public string? Question { get; set; }                   // text snapshot from the resolved question
    public string? Answer { get; set; }
    public decimal? Numeric { get; set; }
    public int? AttId { get; set; }
    public decimal? Latitude { get; set; }
    public decimal? Longitude { get; set; }
}

/// <summary>A visit unit with its asset and the sections / questions resolved for it.</summary>
public class PmVisitUnitOpenDto : PmVisitUnitDto
{
    public string? NotServicedReason { get; set; }
    public string? Note { get; set; }
    public AssetDto? Asset { get; set; }
    public string? HeatingType { get; set; }
    public string? Mount { get; set; }
    public Dictionary<string, int>? RepeatCounts { get; set; }
    public List<FormPreviewSectionDto> Sections { get; set; } = new();   // Unit-phase sections resolved for this unit
}

/// <summary>Everything the visit page needs in one call.</summary>
public class PmVisitOpenDto
{
    public PmVisitHeaderDto Header { get; set; } = new();
    public PmVisitDto Visit { get; set; } = new();
    public LocationTradeProfileDto? Profile { get; set; }
    public List<string> ProfileAccessNames { get; set; } = new();
    public List<string> ProfileMountNames { get; set; } = new();
    public string? TemplateName { get; set; }
    public int? TemplateVersion { get; set; }
    public List<FormPreviewSectionDto> SiteSections { get; set; } = new();
    public List<FormPreviewSectionDto> CheckoutSections { get; set; } = new();
    public List<PmVisitUnitOpenDto> Units { get; set; } = new();
    public List<PmVisitAnswerDto> Answers { get; set; } = new();
    public PmVisitLookupsDto Lookups { get; set; } = new();
    // checkout (slice 4b)
    public List<PmVisitFindingDto> Findings { get; set; } = new();
    public List<PmVisitMaterialDto> Materials { get; set; } = new();
    public PmFindingOptionsDto FindingOptions { get; set; } = new();
    public string TechName { get; set; } = string.Empty;
    public PmVisitInclusionsDto Inclusions { get; set; } = new();
}

/// <summary>What the customer's PM price already includes (from the rule), so materials can be entered at $0 when covered.</summary>
public class PmVisitInclusionsDto
{
    public bool FiltersIncluded { get; set; }
    public bool NoFilterChange { get; set; }
    public int? FreeFilters { get; set; }
    public int? FreeBeltChangesPerYear { get; set; }
    public bool BeltsChargeable { get; set; }
}

public class PmVisitLookupsDto
{
    public List<FormAssetCategoryDto> Categories { get; set; } = new();
    public List<AssetComponentTypeDto> ComponentTypes { get; set; } = new();
    public List<AssetAttributeTypeDto> AttributeTypes { get; set; } = new();
    public List<AssetMountLocationDto> MountLocations { get; set; } = new();
    public List<string> HeatingTypes { get; set; } = new();
}

public class PmCheckInRequest
{
    public decimal? Latitude { get; set; }
    public decimal? Longitude { get; set; }
}

public class PmCheckInResultDto
{
    public string Status { get; set; } = string.Empty;
    public string? Weather { get; set; }
    public decimal? OutdoorTempF { get; set; }
    public string? WeatherSource { get; set; }
    public DateTime? WeatherDateTime { get; set; }
    public string? WeatherError { get; set; }
}

public class SavePmVisitUnitRequest
{
    public string? Status { get; set; }                     // Pending | Serviced | NotServiced
    public string? NotServicedReason { get; set; }
    public bool? AssetConfirmed { get; set; }
    public string? Note { get; set; }
}

public class AddPmVisitUnitRequest
{
    public SaveAssetRequest Asset { get; set; } = new();
}

public class PmVisitExistsDto
{
    public bool Exists { get; set; }
    public int? PmvId { get; set; }
    public string? Status { get; set; }
}

#region checkout (slice 4b)

/// <summary>A deficiency / recommendation recorded on the visit (dictionary F182-F191, P25).</summary>
public class PmVisitFindingDto
{
    public int PmvfId { get; set; }
    public int PmvId { get; set; }
    public int? PmvuId { get; set; }
    public string? UnitLabel { get; set; }
    public string? Component { get; set; }
    public string? Description { get; set; }
    public string? Severity { get; set; }
    public string? Impact { get; set; }
    public string? Risk { get; set; }
    public string? Action { get; set; }
    public string? Parts { get; set; }
    public string? LaborEstimate { get; set; }
    public bool QuoteRequired { get; set; }
    public string? CallCenterContacted { get; set; }
    public string? ContactDetails { get; set; }
    public int? AttId { get; set; }
    public string? AttFilename { get; set; }
    public int? SrIdProposal { get; set; }
    public string? ProposalRequestNumber { get; set; }
    public int? UId { get; set; }
    public DateTime InsertDateTime { get; set; }
}

public class SavePmVisitFindingRequest
{
    public int? PmvuId { get; set; }
    public string? Component { get; set; }
    public string? Description { get; set; }
    public string? Severity { get; set; }
    public string? Impact { get; set; }
    public string? Risk { get; set; }
    public string? Action { get; set; }
    public string? Parts { get; set; }
    public string? LaborEstimate { get; set; }
    public bool QuoteRequired { get; set; }
    public string? CallCenterContacted { get; set; }
    public string? ContactDetails { get; set; }
    public int? AttId { get; set; }
}

/// <summary>The pick lists for a finding, taken from the template's finding questions (F184-F187) so the wording stays the dictionary's.</summary>
public class PmFindingOptionsDto
{
    public List<string> Severity { get; set; } = new();
    public List<string> Impact { get; set; } = new();
    public List<string> Risk { get; set; } = new();
    public List<string> Action { get; set; } = new();
}

/// <summary>A material on the ticket's work orders (xrefWorkOrderServiceItem), read-only here; written through the legacy service.</summary>
public class PmVisitMaterialDto
{
    public int XwosiId { get; set; }
    public int WoId { get; set; }
    public int? AsId { get; set; }
    public string? UnitLabel { get; set; }
    public string? Name { get; set; }
    public string? Description { get; set; }
    public decimal? BaseCost { get; set; }
    public decimal? Quantity { get; set; }
    public bool ForQuote { get; set; }
    public DateTime InsertDateTime { get; set; }
}

public class PmVisitSubmitRequest
{
    /// <summary>The tech's confirmation that the customer's own form was completed (only asked when the rule requires one).</summary>
    public bool? CustomerFormCompleted { get; set; }
    /// <summary>Submit even though recommended items are open (required items always block).</summary>
    public bool Force { get; set; }
}

public class PmVisitSubmitResultDto
{
    public bool Submitted { get; set; }
    public string Status { get; set; } = string.Empty;          // Submitted | Incomplete, or the unchanged status when blocked
    /// <summary>Secondary status code the page passes to the legacy check-out (Complete-3 / Incomplete-7).</summary>
    public string? SsCode { get; set; }
    public int? ProposalSrId { get; set; }
    public string? ProposalRequestNumber { get; set; }
    public string? ProposalNote { get; set; }
    public List<PmVisitValidatorItemDto> Blocking { get; set; } = new();
}

public class PmVisitValidatorItemDto
{
    public string Scope { get; set; } = string.Empty;
    public int? PmvuId { get; set; }
    public string Code { get; set; } = string.Empty;
    public string Question { get; set; } = string.Empty;
    public int RepeatIndex { get; set; }
}

/// <summary>What the submit needs to know about the ticket to build the proposal SR.</summary>
public class PmVisitSrBasicsDto
{
    public int SrId { get; set; }
    public int XcccId { get; set; }
    public int LId { get; set; }
    public int TId { get; set; }
    public int TIdParent { get; set; }
    public int PId { get; set; }
    public string RequestNumber { get; set; } = string.Empty;
    public decimal? TripCharge { get; set; }
    public string? IvrRequestNumber { get; set; }
    public string? SiteContactName { get; set; }
    public string? SiteContactPhone { get; set; }
    public string? SiteContactEmail { get; set; }
}

#endregion

/// <summary>Current conditions at a site, from the weather provider.</summary>
public class WeatherResultDto
{
    public string Description { get; set; } = string.Empty;
    public decimal? TemperatureF { get; set; }
    public string Source { get; set; } = string.Empty;
    public DateTime ObservedAt { get; set; }
}
