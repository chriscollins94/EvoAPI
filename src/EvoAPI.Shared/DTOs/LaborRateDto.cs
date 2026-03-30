namespace EvoAPI.Shared.DTOs;

public class LaborRateDto
{
    public int LrId { get; set; }
    public int XcccId { get; set; }
    public int TId { get; set; }
    public string? TradeName { get; set; }
    public string? TradeDescription { get; set; }
    public string? ParentTradeName { get; set; }
    public int? TNte { get; set; }
    public string? LrDescriptionOverride { get; set; }
    public int? LrNte { get; set; }
    public decimal? LrRateRegular { get; set; }
    public decimal? LrRateOvertime { get; set; }
    public decimal? LrRateHoliday { get; set; }
    public decimal? LrRateSpecial { get; set; }
    public decimal? LrRateScheduledAfterHours { get; set; }
    public decimal? LrRateRegularDiscount { get; set; }
    public decimal? LrRateRegularDiscountHoursLimit { get; set; }
    public decimal? LrRateHelper { get; set; }
    public decimal? LrRateHelperOvertime { get; set; }
    public decimal? LrRateFlat { get; set; }
    public string? LrFlatOrHourly { get; set; }
    public decimal? LrTripCharge { get; set; }
    public int? LrMarkup { get; set; }
    public string? LrNote { get; set; }
    public bool TActive { get; set; }
    public DateTime LrInsertDateTime { get; set; }
    public DateTime? LrModifiedDateTime { get; set; }
}

public class CompanyTradeDto
{
    public int TId { get; set; }
    public int? TIdParent { get; set; }
    public string TradeName { get; set; } = string.Empty;
    public string? TradeDescription { get; set; }
    public string? ParentTradeName { get; set; }
    public int? TNte { get; set; }
    public bool TParentOnly { get; set; }
    public bool THighVolume { get; set; }
}

public class CheckListDto
{
    public int ClId { get; set; }
    public int XcccId { get; set; }
    public int CltId { get; set; }
    public string ClName { get; set; } = string.Empty;
    public bool ClPublicForQuote { get; set; }
    public bool ClPublicForInvoice { get; set; }
    public bool ClActive { get; set; }
    public string? CltType { get; set; }
    public List<CheckListQuestionDto>? Questions { get; set; }
}

public class CreateLaborRateRequest
{
    public int TId { get; set; }
    public string? LrDescriptionOverride { get; set; }
    public int? LrNte { get; set; }
    public decimal? LrRateRegular { get; set; }
    public decimal? LrRateOvertime { get; set; }
    public decimal? LrRateHoliday { get; set; }
    public decimal? LrRateSpecial { get; set; }
    public decimal? LrRateScheduledAfterHours { get; set; }
    public decimal? LrRateRegularDiscount { get; set; }
    public decimal? LrRateRegularDiscountHoursLimit { get; set; }
    public decimal? LrRateHelper { get; set; }
    public decimal? LrRateHelperOvertime { get; set; }
    public decimal? LrRateFlat { get; set; }
    public string? LrFlatOrHourly { get; set; }
    public decimal? LrTripCharge { get; set; }
    public int? LrMarkup { get; set; }
    public string? LrNote { get; set; }
}

public class UpdateLaborRateRequest
{
    public string? LrDescriptionOverride { get; set; }
    public int? LrNte { get; set; }
    public decimal? LrRateRegular { get; set; }
    public decimal? LrRateOvertime { get; set; }
    public decimal? LrRateHoliday { get; set; }
    public decimal? LrRateSpecial { get; set; }
    public decimal? LrRateScheduledAfterHours { get; set; }
    public decimal? LrRateRegularDiscount { get; set; }
    public decimal? LrRateRegularDiscountHoursLimit { get; set; }
    public decimal? LrRateHelper { get; set; }
    public decimal? LrRateHelperOvertime { get; set; }
    public decimal? LrRateFlat { get; set; }
    public string? LrFlatOrHourly { get; set; }
    public decimal? LrTripCharge { get; set; }
    public int? LrMarkup { get; set; }
    public string? LrNote { get; set; }
}
