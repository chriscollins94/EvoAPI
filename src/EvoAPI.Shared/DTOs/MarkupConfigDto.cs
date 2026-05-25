namespace EvoAPI.Shared.DTOs;

// Snapshot of the markup/tax configuration that applies to a single service
// request. Loaded once per SR (Company/Call Center/Trade) and used both by
// the labor-context endpoint (display) and by the markup calculator (math).
//
// Mirrors evo's invoice-time inputs:
//   - LaborRate.lr_markup            -> TradeMarkupPercent      (tier 1)
//   - MaterialsMarkup rows           -> MaterialsRanges          (tier 2)
//   - xccc.xccc_markuppercentage     -> CompanyDefaultPercent    (tier 3)
//   - xccc.xccc_markuppercentagesupplier -> CompanySupplierPercent (always added)
//   - xccc.xccc_taxexempt + ConfigSetting taxFlatRate -> tax inputs
//
// Per the Quote-AI design we deliberately skip evo's effective-date gates —
// markup/tax apply whenever configured, since this is a forecasting tool for
// new SRs.
public class MarkupConfigDto
{
    public int?    TradeMarkupPercent      { get; set; }   // null = no trade override
    public int?    CompanyDefaultPercent   { get; set; }
    public int?    CompanySupplierPercent  { get; set; }
    public List<MarkupRangeDto> MaterialsRanges { get; set; } = new();
    public bool    TaxExempt               { get; set; }
    public decimal TaxFlatRate             { get; set; }   // percent, e.g. 7
    public string  Summary                 { get; set; } = string.Empty; // one-liner for UI/PDF footer
}

public class MarkupRangeDto
{
    public decimal From          { get; set; }   // mm_from
    public decimal To            { get; set; }   // mm_to
    public int     Markup        { get; set; }   // mm_markup
    public int     HighQtyMarkup { get; set; }   // mm_markuphighquantity (qty > 10 trigger)
}
