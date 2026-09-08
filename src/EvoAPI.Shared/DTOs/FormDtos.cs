namespace EvoAPI.Shared.DTOs;

// Forms engine (PM build slice 1): generic form templates / rules, plus the PM-specific rule terms.
// Column-to-property mapping lives in FormRepository; these are the wire shapes for the
// Form Templates settings page and the PM CUSTOMER RULES tab.

#region Templates

/// <summary>
/// A parent trade that takes part in PM (has a "PM - ..." sub-trade, or already owns a template)
/// with its active form template, if any.
/// </summary>
public class FormParentTradeDto
{
    public int TId { get; set; }
    public string Trade { get; set; } = string.Empty;
    public int? ActiveTemplateId { get; set; }
    public int? ActiveVersion { get; set; }
    public int TemplateCount { get; set; }
    public List<string> PmTrades { get; set; } = new();     // the "PM - ..." sub-trades under this parent
}

/// <summary>
/// One form template version (question bank) for a parent trade.
/// </summary>
public class FormTemplateDto
{
    public int FtId { get; set; }
    public int TId { get; set; }
    public string TradeName { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public int Version { get; set; }
    public bool Active { get; set; }
    public string? Note { get; set; }
    public int SectionCount { get; set; }
    public int QuestionCount { get; set; }
    public int RuleCount { get; set; }          // form rules pinned to this version
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
}

public class FormTemplateDetailDto : FormTemplateDto
{
    public List<FormSectionDto> Sections { get; set; } = new();
}

public class FormSectionDto
{
    public int FsId { get; set; }
    public int FtId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Phase { get; set; } = string.Empty;       // Site | Asset | Unit | Findings | Checkout
    public int Order { get; set; }
    public bool RepeatPerUnit { get; set; }
    public string? Condition { get; set; }                  // JSON
    public bool Active { get; set; }
    public List<FormQuestionDto> Questions { get; set; } = new();
}

public class FormQuestionDto
{
    public int FqId { get; set; }
    public int FsId { get; set; }
    public string Code { get; set; } = string.Empty;        // F135 / T044 / P16
    public int ClatId { get; set; }
    public string? AnswerType { get; set; }                 // CheckListAnswerType.clat_type
    public string Question { get; set; } = string.Empty;
    public int Order { get; set; }
    public string Requirement { get; set; } = string.Empty; // Always | Recommended | Optional | Conditional | Configured
    public string? Condition { get; set; }                  // JSON
    public int? FalId { get; set; }
    public string? AnswerListName { get; set; }
    public string? AnswerListValues { get; set; }
    public string? AnswerValues { get; set; }
    public string? DataType { get; set; }
    public string? Unit { get; set; }
    public decimal? Min { get; set; }
    public decimal? Max { get; set; }
    public string? RepeatKey { get; set; }
    public string? CalcFormula { get; set; }
    public string WritesTo { get; set; } = "Visit";
    public string PhotoRequired { get; set; } = "No";
    public string? PhotoTiming { get; set; }
    public string? LinkedCode { get; set; }
    public decimal? EstMinutes { get; set; }
    public string? Seasons { get; set; }
    public string? SkipAnswer { get; set; }
    public int? SkipToOrder { get; set; }
    public string? TriggerNote { get; set; }
    public bool Active { get; set; }
}

public class FormAnswerListDto
{
    public int FalId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Values { get; set; } = string.Empty;      // semicolon separated
    public string? FailValues { get; set; }
    public bool Active { get; set; }
    public int QuestionCount { get; set; }
}

public class SaveFormTemplateRequest
{
    public int TId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? Note { get; set; }
    public bool Active { get; set; } = true;
}

public class SaveFormSectionRequest
{
    public string Name { get; set; } = string.Empty;
    public string Phase { get; set; } = "Unit";
    public int Order { get; set; }
    public bool RepeatPerUnit { get; set; }
    public string? Condition { get; set; }
    public bool Active { get; set; } = true;
}

public class SaveFormQuestionRequest
{
    public int? FsId { get; set; }                          // update only: move to another section of the same template
    public string Code { get; set; } = string.Empty;
    public int ClatId { get; set; }
    public string Question { get; set; } = string.Empty;
    public int Order { get; set; }
    public string Requirement { get; set; } = "Always";
    public string? Condition { get; set; }
    public int? FalId { get; set; }
    public string? AnswerValues { get; set; }
    public string? DataType { get; set; }
    public string? Unit { get; set; }
    public decimal? Min { get; set; }
    public decimal? Max { get; set; }
    public string? RepeatKey { get; set; }
    public string? CalcFormula { get; set; }
    public string WritesTo { get; set; } = "Visit";
    public string PhotoRequired { get; set; } = "No";
    public string? PhotoTiming { get; set; }
    public string? LinkedCode { get; set; }
    public decimal? EstMinutes { get; set; }
    public string? Seasons { get; set; }
    public string? SkipAnswer { get; set; }
    public int? SkipToOrder { get; set; }
    public string? TriggerNote { get; set; }
    public bool Active { get; set; } = true;
}

public class SaveFormAnswerListRequest
{
    public string Name { get; set; } = string.Empty;
    public string Values { get; set; } = string.Empty;
    public string? FailValues { get; set; }
    public bool Active { get; set; } = true;
}

#endregion

#region Rules

/// <summary>
/// A parent trade the PM CUSTOMER RULES tab shows a card for: the company has a PM sub-trade
/// assigned under it on the TRADES tab, or a rule already exists.
/// </summary>
public class FormCompanyTradeDto
{
    public int TId { get; set; }
    public string Trade { get; set; } = string.Empty;
    public List<string> PmTrades { get; set; } = new();
    public bool HasRule { get; set; }
    public int? FrId { get; set; }
    public int? ActiveTemplateId { get; set; }
}

/// <summary>
/// Generic form rule (which template a company pairing + trade is on, plus question overrides)
/// with the PM terms nested under Pm.
/// </summary>
public class FormRuleDto
{
    public int FrId { get; set; }
    public int XcccId { get; set; }
    public int TId { get; set; }
    public string TradeName { get; set; } = string.Empty;
    public int? FtId { get; set; }
    public string? TemplateName { get; set; }
    public int? TemplateVersion { get; set; }
    public bool Active { get; set; }
    public string? Note { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }

    public PmRuleDto? Pm { get; set; }
    public List<PmRateTierDto> Tiers { get; set; } = new();
    public List<PmSeasonDto> Seasons { get; set; } = new();
    public List<FormRuleQuestionDto> QuestionOverrides { get; set; } = new();
}

/// <summary>
/// PM business terms for one form rule (PMRule row).
/// </summary>
public class PmRuleDto
{
    public int PmrId { get; set; }
    public int FrId { get; set; }
    // billing
    public int? PmbmId { get; set; }
    public string? BillingMode { get; set; }
    public string? NteGuideline { get; set; }
    public string? FirstTimeRule { get; set; }
    public decimal? HourlyRate { get; set; }
    public decimal? HourCapPerUnit { get; set; }
    public decimal? HourCapPerVisit { get; set; }
    public decimal? TripCharge { get; set; }
    public int? AttIdPricingContract { get; set; }
    public string? PricingNote { get; set; }
    // inclusions
    public bool? FiltersIncluded { get; set; }
    public bool? NoFilterChange { get; set; }
    public int? FreeFilters { get; set; }
    public int? FreeBeltChangesPerYear { get; set; }
    public bool? BeltsChargeable { get; set; }
    // customer form
    public string? CustomerForm { get; set; }
    public string? CustomerFormUrl { get; set; }
    public int? AttIdCustomerForm { get; set; }
    public string? CustomerFormNote { get; set; }
    // parameters
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
    // contacts
    public string? ContactName { get; set; }
    public string? ContactEmail { get; set; }
    public string? NotifyEmail { get; set; }
}

/// <summary>
/// Create / update a form rule. Pm carries the PM terms; the property names match PmRuleDto so the
/// critical-audit diff can compare the two by name.
/// </summary>
public class SaveFormRuleRequest
{
    public int? FtId { get; set; }
    public bool Active { get; set; } = true;
    public string? Note { get; set; }
    public SavePmRuleRequest? Pm { get; set; }
}

public class SavePmRuleRequest
{
    public int? PmbmId { get; set; }
    public string? NteGuideline { get; set; }
    public string? FirstTimeRule { get; set; }
    public decimal? HourlyRate { get; set; }
    public decimal? HourCapPerUnit { get; set; }
    public decimal? HourCapPerVisit { get; set; }
    public decimal? TripCharge { get; set; }
    public int? AttIdPricingContract { get; set; }
    public string? PricingNote { get; set; }
    public bool? FiltersIncluded { get; set; }
    public bool? NoFilterChange { get; set; }
    public int? FreeFilters { get; set; }
    public int? FreeBeltChangesPerYear { get; set; }
    public bool? BeltsChargeable { get; set; }
    public string? CustomerForm { get; set; }
    public string? CustomerFormUrl { get; set; }
    public int? AttIdCustomerForm { get; set; }
    public string? CustomerFormNote { get; set; }
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
    public string? ContactName { get; set; }
    public string? ContactEmail { get; set; }
    public string? NotifyEmail { get; set; }
}

public class PmBillingModeDto
{
    public int PmbmId { get; set; }
    public string Mode { get; set; } = string.Empty;
    public string? Description { get; set; }
    public int Order { get; set; }
}

public class PmRateTierDto
{
    public int PmrtId { get; set; }
    public int PmrId { get; set; }
    public int? AscId { get; set; }
    public string? EquipmentType { get; set; }
    public string? Label { get; set; }
    public decimal? MinTons { get; set; }
    public decimal? MaxTons { get; set; }
    public decimal? FirstUnitPrice { get; set; }
    public decimal? AdditionalUnitPrice { get; set; }
    public bool AddOn { get; set; }
    public string? AddOnLabel { get; set; }
    public int Order { get; set; }
    public bool Active { get; set; } = true;
}

public class PmSeasonDto
{
    public int PmsId { get; set; }
    public int PmrId { get; set; }
    public string Name { get; set; } = string.Empty;
    public int? TIdSeason { get; set; }
    public string? SeasonTradeName { get; set; }
    public string? VisitType { get; set; }
    public string? StartMonthDay { get; set; }
    public string? EndMonthDay { get; set; }
    public int? VisitsPerYear { get; set; }
    public int? IntervalDays { get; set; }
    public int Order { get; set; }
    public bool Active { get; set; } = true;
}

public class FormRuleQuestionDto
{
    public int XfrqId { get; set; }
    public int FrId { get; set; }
    public int FqId { get; set; }
    public bool Enabled { get; set; } = true;
    public string? RequirementOverride { get; set; }
    public string? QuestionOverride { get; set; }
    public string? PhotoOverride { get; set; }
}

/// <summary>
/// Sub-trade / category lookups the rules tab needs for its dropdowns.
/// </summary>
public class FormTradeOptionDto
{
    public int TId { get; set; }
    public string Trade { get; set; } = string.Empty;
}

public class FormAssetCategoryDto
{
    public int AscId { get; set; }
    public string Category { get; set; } = string.Empty;
}

#endregion

#region Preview (template ∩ rule ∩ season ∩ equipment / heating type)

public class FormPreviewDto
{
    public int FtId { get; set; }
    public string TemplateName { get; set; } = string.Empty;
    public int Version { get; set; }
    public string? Season { get; set; }
    public string? EquipmentType { get; set; }
    public string? HeatingType { get; set; }
    public string? Mount { get; set; }                       // from the asset, when the preview names a unit
    public int? AsId { get; set; }
    public string? AssetLabel { get; set; }
    public Dictionary<string, string>? AssetAttributes { get; set; }
    public Dictionary<string, int>? RepeatCounts { get; set; }
    public int TotalQuestions { get; set; }
    public int IncludedQuestions { get; set; }
    public int AssetDependentQuestions { get; set; }
    public int AnswerDependentQuestions { get; set; }
    public List<FormPreviewSectionDto> Sections { get; set; } = new();
}

public class FormPreviewSectionDto
{
    public int FsId { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Phase { get; set; } = string.Empty;
    public bool RepeatPerUnit { get; set; }
    public bool Included { get; set; }
    public string? Reason { get; set; }
    public int IncludedCount { get; set; }
    public List<FormPreviewQuestionDto> Questions { get; set; } = new();
}

public class FormPreviewQuestionDto
{
    public int FqId { get; set; }
    public string Code { get; set; } = string.Empty;
    public string Question { get; set; } = string.Empty;
    public string? AnswerType { get; set; }
    public string TemplateRequirement { get; set; } = string.Empty;
    public string Requirement { get; set; } = string.Empty;    // effective for this customer
    public string PhotoRequired { get; set; } = string.Empty;  // effective for this customer
    public bool Included { get; set; }
    public bool DependsOnAsset { get; set; }                   // condition needs an asset fact / mounting
    public bool DependsOnAnswer { get; set; }                  // condition needs a prior answer
    public bool CustomerEnabled { get; set; }                  // Configured question switched on by the rule
    public string? Reason { get; set; }
    public string? Condition { get; set; }
    public string? RepeatKey { get; set; }
    public int? RepeatCount { get; set; }                     // times the question repeats on the named unit (circuits, compressors ...)
}

#endregion
