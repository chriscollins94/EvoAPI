namespace EvoAPI.Shared.DTOs;

// Spend-to-date for one service request, as the NTE scan sees it. Breakdown
// is kept so AuditNTE detail and the diagnostics script can show
// why a percent came out the way it did. See NTE_NOTIFICATION_PLAN.md.
public class NteSpendResult
{
    public int SrId { get; set; }
    public decimal Nte { get; set; }
    public decimal Labor { get; set; }
    public decimal ServiceItems { get; set; }          // incurred items only (xwosi_forquote = 0)
    public decimal QuotedItemsExcluded { get; set; }   // quoted items left out of ServiceItems (D7), for the audit detail
    public decimal TripCharge { get; set; }
    public int OpenCheckIns { get; set; }    // open check-ins counted (within MaxOpenCheckInHours)
    public int StaleCheckIns { get; set; }   // open check-ins ignored as forgotten check-outs (D8)

    public decimal Spent => Math.Round(Labor + ServiceItems + TripCharge, 2);
    public decimal Percent => Nte > 0 ? Math.Round(Spent / Nte * 100m, 1) : 0m;
}
