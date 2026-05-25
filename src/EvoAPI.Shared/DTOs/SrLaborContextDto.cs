namespace EvoAPI.Shared.DTOs;

// Looked up per selected service request: who the customer is, what call
// center, the trade, and the hourly labor rate (driven by lrt.lrt_fieldname
// which maps to a column on LaborRate). Returned to the Quote AI UI so the
// office user can confirm context before generating, and so the server can
// append the rate to the AI system prompt at request time.
public class SrLaborContextDto
{
    public int     SrId           { get; set; }
    public string  RequestNumber  { get; set; } = string.Empty;
    public string? Company        { get; set; }
    public string? CallCenter     { get; set; }
    public string? Trade          { get; set; }
    public string? RateType       { get; set; }   // e.g. "Regular" (lrt_laborratetype)
    public string? RateField      { get; set; }   // e.g. "lr_rateregular" (lrt_fieldname)
    public decimal? RatePerHour   { get; set; }   // resolved from RateField on LaborRate
    public bool    HasRate        => RatePerHour.HasValue && RatePerHour.Value > 0;
    public MarkupConfigDto? MarkupConfig { get; set; }

    // Trip charge resolved per evo's invoice cascade:
    //   1. LaborRate.lr_tripcharge (flat $)
    //   2. sr.sr_tripcharge_quote   (percent of hourly rate)
    //   3. xccc.xccc_tripcharge     (percent of hourly rate, company default)
    // Summary is a human-readable one-liner for the UI chip / PDF.
    public decimal? TripChargeAmount  { get; set; }
    public string?  TripChargeSource  { get; set; } // "trade-flat" | "sr-percent" | "company-percent" | "none"
    public string?  TripChargeSummary { get; set; }
    public bool     HasTripCharge     => TripChargeAmount.HasValue && TripChargeAmount.Value > 0;
}
