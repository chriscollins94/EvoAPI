namespace EvoAPI.Shared.DTOs;

public class CompanyDetailDto
{
    public int XcccId { get; set; }
    public int CompanyId { get; set; }
    public int CallCenterId { get; set; }
    public string CompanyName { get; set; } = string.Empty;
    public string CallCenterName { get; set; } = string.Empty;
    
    // General Info Fields
    public decimal? TripCharge { get; set; }
    public int? BillableRuleId { get; set; }
    public string? BillableRuleDescription { get; set; }
    public int? BillableRuleRoundToMinute { get; set; }
    public int? TermsId { get; set; }
    public string? TermsDescription { get; set; }
    public int? TermsNumberOfDays { get; set; }
    public bool TaxExempt { get; set; }
    public int MinimumLaborChargeMinutes { get; set; }
    public decimal MarkupPercentage { get; set; }
    public decimal MarkupPercentageSupplier { get; set; }
    public decimal? MarkupTriggerAmount { get; set; }
    public bool Active { get; set; }
    public bool FirmQuote { get; set; }
    public bool InvoiceDateShow { get; set; }
    public bool IvrRequestNumber { get; set; }
    public bool CollectPaymentOnSite { get; set; }
    public bool NteGuidance { get; set; }
    public string? ClientRepresentative { get; set; }
    public string? LicenseRepresentative { get; set; }
    public string? Agencies { get; set; }
    public string? InvoiceExtraText { get; set; }
    public string? Note { get; set; }
    public string? PortalUrl { get; set; }
    public string? PortalName { get; set; }
    public string? PortalCredentials { get; set; }

    // Timestamps
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    
    // Related data
    public List<MaterialsMarkupDto> MaterialsMarkup { get; set; } = new();
    public List<BillableRuleDto> BillableRules { get; set; } = new();
    public List<TermsDto> Terms { get; set; } = new();
}

public class MaterialsMarkupDto
{
    public int MmId { get; set; }
    public int XcccId { get; set; }
    public int FromPrice { get; set; }
    public int ToPrice { get; set; }
    public decimal MarkupPercentage { get; set; }
    public decimal MarkupHighQuantity { get; set; }
    public decimal MarkupFoundational { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
}

public class BillableRuleDto
{
    public int BrId { get; set; }
    public string Description { get; set; } = string.Empty;
    public int RoundToMinute { get; set; }
    public int Order { get; set; }
}

public class TermsDto
{
    public int TermsId { get; set; }
    public string Description { get; set; } = string.Empty;
    public int NumberOfDays { get; set; }
    public int Order { get; set; }
}

public class CompanyListDto
{
    public int XcccId { get; set; }
    public int CompanyId { get; set; }
    public int CallCenterId { get; set; }
    public string CompanyName { get; set; } = string.Empty;
    public bool Active { get; set; }
    public string? Note { get; set; }

    // Surfaced for the New Service Request flow (so schedulers don't need the
    // CompanyAdminOnly company-detail endpoint): default trip charge, whether this
    // company uses IVR request numbers, and whether to show NTE guidance.
    public decimal? TripCharge { get; set; }
    public bool IvrRequestNumber { get; set; }
    public bool NteGuidance { get; set; }

    // Comma-delimited Agency options for the New Service Request Agency dropdown
    // (hidden when empty).
    public string? Agencies { get; set; }
}

public class UpdateCompanyGeneralInfoRequest
{
    public int XcccId { get; set; }
    public string CompanyName { get; set; } = string.Empty;
    public decimal? TripCharge { get; set; }
    public int? BillableRuleId { get; set; }
    public int? TermsId { get; set; }
    public bool TaxExempt { get; set; }
    public int MinimumLaborChargeMinutes { get; set; }
    public decimal MarkupPercentage { get; set; }
    public decimal MarkupPercentageSupplier { get; set; }
    public decimal? MarkupTriggerAmount { get; set; }
    public bool Active { get; set; }
    public bool FirmQuote { get; set; }
    public bool InvoiceDateShow { get; set; }
    public bool IvrRequestNumber { get; set; }
    public bool CollectPaymentOnSite { get; set; }
    public bool NteGuidance { get; set; }
    public string? ClientRepresentative { get; set; }
    public string? LicenseRepresentative { get; set; }
    public string? Agencies { get; set; }
    public string? InvoiceExtraText { get; set; }
    public string? Note { get; set; }
    public string? PortalUrl { get; set; }
    public string? PortalName { get; set; }
    public string? PortalCredentials { get; set; }
}

public class CreateCompanyRequest
{
    public string CompanyName { get; set; } = string.Empty;
}

public class CreateCompanyResponse
{
    public int CId { get; set; }
    public string CompanyName { get; set; } = string.Empty;
}

public class AssignCompanyCallCenterRequest
{
    public int CId { get; set; }
    public int CcId { get; set; }
}

public class AssignCompanyCallCenterResponse
{
    public int XcccId { get; set; }
    public int CId { get; set; }
    public int CcId { get; set; }
}

public class CompanyCallCenterPairingDto
{
    public int XcccId { get; set; }
    public int CcId { get; set; }
    public string CcName { get; set; } = string.Empty;
    public bool Active { get; set; }
}

public class CompanyWithCallCentersDto
{
    public int CId { get; set; }
    public string CompanyName { get; set; } = string.Empty;
    public bool Active { get; set; }
    public List<CompanyCallCenterPairingDto> CallCenters { get; set; } = new();
}

public class CreateMaterialsMarkupRequest
{
    public int XcccId { get; set; }
    public int FromPrice { get; set; }
    public int ToPrice { get; set; }
    public decimal MarkupPercentage { get; set; }
    public decimal MarkupHighQuantity { get; set; }
    public decimal MarkupFoundational { get; set; }
}

public class UpdateMaterialsMarkupRequest
{
    public int MmId { get; set; }
    public int FromPrice { get; set; }
    public int ToPrice { get; set; }
    public decimal MarkupPercentage { get; set; }
    public decimal MarkupHighQuantity { get; set; }
    public decimal MarkupFoundational { get; set; }
}

public class CompanyPriorityDto
{
    public int XcpId { get; set; }
    public int CompanyId { get; set; }
    public int PriorityId { get; set; }
    public string PriorityName { get; set; } = string.Empty;
    public string CompanySpecificName { get; set; } = string.Empty;
    public decimal ArrivalTimeInHours { get; set; }
    public int PriorityOrder { get; set; }
}

public class UpdateCompanyPriorityRequest
{
    public int XcpId { get; set; }
    public int CompanyId { get; set; }
    public int PriorityId { get; set; }
    public string CompanySpecificName { get; set; } = string.Empty;
    public decimal ArrivalTimeInHours { get; set; }
}

public class PortalInfoCallCenterDto
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string? PortalName { get; set; }
    public string? PortalUrl { get; set; }
    public string? PortalCredentials { get; set; }
}

public class PortalInfoCompanyDto
{
    public int XcccId { get; set; }
    public int CompanyId { get; set; }
    public int CallCenterId { get; set; }
    public string CompanyName { get; set; } = string.Empty;
    public string CallCenterName { get; set; } = string.Empty;
    public string? PortalName { get; set; }
    public string? PortalUrl { get; set; }
    public string? PortalCredentials { get; set; }
}

public class PortalInfoReportDto
{
    public List<PortalInfoCallCenterDto> CallCenters { get; set; } = new();
    public List<PortalInfoCompanyDto> Companies { get; set; } = new();
}

