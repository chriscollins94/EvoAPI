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
    public int? TermsId { get; set; }
    public string? TermsDescription { get; set; }
    public int? TermsNumberOfDays { get; set; }
    public bool TaxExempt { get; set; }
    public int MinimumLaborChargeMinutes { get; set; }
    public int MarkupPercentage { get; set; }
    public int MarkupPercentageSupplier { get; set; }
    public bool Active { get; set; }
    public bool FirmQuote { get; set; }
    public bool InvoiceDateShow { get; set; }
    public bool IvrRequestNumber { get; set; }
    public string? ClientRepresentative { get; set; }
    public string? LicenseRepresentative { get; set; }
    public string? InvoiceExtraText { get; set; }
    public string? Note { get; set; }
    
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
    public int MarkupPercentage { get; set; }
    public int MarkupHighQuantity { get; set; }
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
}

public class UpdateCompanyGeneralInfoRequest
{
    public int XcccId { get; set; }
    public decimal? TripCharge { get; set; }
    public int? BillableRuleId { get; set; }
    public int? TermsId { get; set; }
    public bool TaxExempt { get; set; }
    public int MinimumLaborChargeMinutes { get; set; }
    public int MarkupPercentage { get; set; }
    public int MarkupPercentageSupplier { get; set; }
    public bool Active { get; set; }
    public bool FirmQuote { get; set; }
    public bool InvoiceDateShow { get; set; }
    public bool IvrRequestNumber { get; set; }
    public string? ClientRepresentative { get; set; }
    public string? LicenseRepresentative { get; set; }
    public string? InvoiceExtraText { get; set; }
    public string? Note { get; set; }
}

public class CreateMaterialsMarkupRequest
{
    public int XcccId { get; set; }
    public int FromPrice { get; set; }
    public int ToPrice { get; set; }
    public int MarkupPercentage { get; set; }
    public int MarkupHighQuantity { get; set; }
}

public class UpdateMaterialsMarkupRequest
{
    public int MmId { get; set; }
    public int FromPrice { get; set; }
    public int ToPrice { get; set; }
    public int MarkupPercentage { get; set; }
    public int MarkupHighQuantity { get; set; }
}
