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
    public decimal? TradeMarkupPercent      { get; set; }   // null = no trade override
    public decimal? CompanyDefaultPercent   { get; set; }
    public decimal? CompanySupplierPercent  { get; set; }
    public List<MarkupRangeDto> MaterialsRanges { get; set; } = new();
    public bool    TaxExempt               { get; set; }
    public decimal TaxFlatRate             { get; set; }   // percent, e.g. 7

    // xccc_markuptriggeramount. When > 0, the SR's full flat-markup total is
    // compared against this dollar amount: below it, the MaterialsRanges tier
    // is skipped in favor of the flat CompanyDefaultPercent; at/above it, the
    // normal cascade applies. null / <= 0 means "always use the normal cascade".
    public decimal? TriggerAmount          { get; set; }
    public string  Summary                 { get; set; } = string.Empty; // one-liner for UI/PDF footer
}

public class MarkupRangeDto
{
    public decimal From          { get; set; }   // mm_from
    public decimal To            { get; set; }   // mm_to
    public decimal Markup        { get; set; }   // mm_markup ("Quoted" markup - the AI quote path always uses this per D9)
    public decimal HighQtyMarkup { get; set; }   // mm_markuphighquantity (qty > 10 trigger)
}
