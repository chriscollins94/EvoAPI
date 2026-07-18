using EvoAPI.Shared.DTOs;

namespace EvoAPI.Infrastructure.Pricing;

// Pure pricing logic — mirrors evo's invoice formula:
//   total = ((basecost * qty) + tax) * (1 + (markup + supplierMarkup) / 100)
//
// Markup cascade per evo (FileController CreateInvoice, ~lines 1821-1879):
//   1. LaborRate.lr_markup (trade-level override)
//   2. MaterialsMarkup range match on basecost; if qty > HighQtyThreshold use
//      mm_markuphighquantity instead of mm_markup; keep the largest matching
//      value if more than one range overlaps.
//   3. Company default xccc_markuppercentage.
// Supplier markup (xccc_markuppercentagesupplier) is always added on top.
//
// Effective-date gates from evo are deliberately omitted — this is a
// forecasting tool for new SRs, so markup/tax apply whenever configured.
public static class MarkupCalculator
{
    public const int HighQtyThreshold = 10;

    public class Result
    {
        public decimal BaseCost            { get; set; }
        public decimal Quantity            { get; set; }
        public decimal TaxAmount           { get; set; }
        public decimal TaxPercent          { get; set; }
        public decimal MarkupPercent       { get; set; } // cascaded value (before supplier)
        public decimal SupplierPercent     { get; set; }
        public decimal EffectiveMarkup     => MarkupPercent + SupplierPercent;
        public decimal LineTotal           { get; set; } // ((cost*qty)+tax) * (1+effective/100), 2dp rounded
        public string  Source              { get; set; } = "none"; // trade / materials-range / company-default / none
    }

    // useMaterialsRanges == false forces the flat-company-markup fallback below
    // the xccc_markuptriggeramount threshold: the MaterialsRanges tier is skipped
    // and the cascade falls through to CompanyDefaultPercent. Trade-level markup
    // still wins and supplier markup is still added, matching evo's behavior.
    public static Result Calculate(MarkupConfigDto config, decimal baseCost, decimal quantity, bool taxable = true, bool useMaterialsRanges = true)
    {
        var r = new Result { BaseCost = baseCost, Quantity = quantity };

        var subtotal = baseCost * quantity;

        // ---- Tax ----------------------------------------------------------
        if (taxable && !config.TaxExempt && config.TaxFlatRate > 0)
        {
            r.TaxAmount  = subtotal * (config.TaxFlatRate / 100m);
            r.TaxPercent = config.TaxFlatRate;
        }

        // ---- Markup cascade ----------------------------------------------
        if (config.TradeMarkupPercent.HasValue && config.TradeMarkupPercent.Value > 0)
        {
            r.MarkupPercent = config.TradeMarkupPercent.Value;
            r.Source        = "trade";
        }
        else if (useMaterialsRanges && config.MaterialsRanges.Count > 0)
        {
            decimal best = 0;
            foreach (var range in config.MaterialsRanges)
            {
                if (baseCost < range.From || baseCost > range.To) continue;
                var pick = quantity > HighQtyThreshold ? range.HighQtyMarkup : range.Markup;
                if (pick > best) best = pick;
            }
            if (best > 0)
            {
                r.MarkupPercent = best;
                r.Source        = "materials-range";
            }
            // If no range matched, evo leaves markup at 0 and does NOT fall
            // back to the company default — preserve that behavior.
        }
        else if (config.CompanyDefaultPercent.HasValue && config.CompanyDefaultPercent.Value > 0)
        {
            r.MarkupPercent = config.CompanyDefaultPercent.Value;
            r.Source        = "company-default";
        }

        if (config.CompanySupplierPercent.HasValue && config.CompanySupplierPercent.Value > 0)
            r.SupplierPercent = config.CompanySupplierPercent.Value;

        // ---- Final --------------------------------------------------------
        var totalRaw = (subtotal + r.TaxAmount) * (1m + r.EffectiveMarkup / 100m);
        r.LineTotal  = Math.Round(totalRaw, 2, MidpointRounding.AwayFromZero);

        return r;
    }
}
