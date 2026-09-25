using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Services;

/// <summary>
/// Prices a Preventative ticket from a customer's PM rule (PM build slice 3, plan section 6.2 T3).
///
/// Contract Tiered rules carry PMRateTier rows: a tier is keyed by equipment type (asc_id, NULL = any type) and a
/// tonnage band (min/max, NULL = open), with a first-unit price and an additional-unit price. Rows flagged AddOn
/// are add-on lines (belt change, coil cleaner) whose price sits in FirstUnitPrice.
///
/// Rules applied (agreed with Chris 2026-09-24):
///   - The first-unit price applies to the first priced unit on the ticket; every later unit uses the additional price.
///   - A unit matches the tier for its own equipment type first, then a type-less tier; within those the tonnage must sit
///     inside the band. A unit with no tonnage only matches tiers with no tonnage band. No match = price 0 plus a warning.
///   - Add-ons carry a quantity (belt change x units, coil cleaner x 1).
///   - First-time PM with rule TripChargeExtra adds one trip-charge line (rule trip charge, else the fallback passed in).
///   - An office override replaces the total; the computed total and the override stay in the breakdown.
/// Pure and deterministic so the same request always prices the same way; the result is stored on the visit as JSON.
/// </summary>
public static class PmPricingCalculator
{
    public static PmPriceResponse Calculate(FormRuleDto? rule, PmPriceRequest request)
    {
        var response = new PmPriceResponse
        {
            BillingMode = rule?.Pm?.BillingMode,
            NteGuideline = rule?.Pm?.NteGuideline,
            FirstTimeRule = rule?.Pm?.FirstTimeRule,
            FirstTime = request.FirstTime
        };

        var activeTiers = (rule?.Tiers ?? new List<PmRateTierDto>()).Where(t => t.Active).ToList();
        var tiers = activeTiers.Where(t => !t.AddOn).OrderBy(t => t.Order).ThenBy(t => t.PmrtId).ToList();
        var addOns = activeTiers.Where(t => t.AddOn).ToList();

        if (rule == null)
            response.Warnings.Add("No form rule exists for this company and trade; nothing could be priced.");
        else if (tiers.Count == 0 && (request.Units?.Count ?? 0) > 0)
            response.Warnings.Add("The rule has no price tiers; units could not be priced.");

        // units
        var first = true;
        foreach (var unit in (request.Units ?? new List<PmPriceUnitRequest>()).OrderBy(u => u.Sequence))
        {
            var tier = MatchTier(tiers, unit);
            var line = new PmPriceLineDto
            {
                Kind = "Unit",
                Sequence = unit.Sequence,
                Label = string.IsNullOrWhiteSpace(unit.Label) ? $"Unit {unit.Sequence}" : unit.Label!,
                Quantity = 1
            };
            if (tier == null)
            {
                line.Matched = false;
                line.UnitPrice = 0;
                line.Amount = 0;
                line.Note = "No price tier matches this unit";
                response.Warnings.Add($"Unit {unit.Sequence}: no price tier matches ({DescribeUnit(unit)}).");
            }
            else
            {
                var price = first
                    ? (tier.FirstUnitPrice ?? tier.AdditionalUnitPrice ?? 0m)
                    : (tier.AdditionalUnitPrice ?? tier.FirstUnitPrice ?? 0m);
                line.UnitPrice = price;
                line.Amount = price;
                line.PmrtId = tier.PmrtId;
                line.TierLabel = TierLabel(tier);
                line.Note = first ? "First unit" : "Additional unit";
                first = false;
            }
            response.Lines.Add(line);
            response.UnitsTotal += line.Amount;
        }

        // add-ons
        foreach (var a in request.AddOns ?? new List<PmPriceAddOnRequest>())
        {
            if (a.Quantity <= 0) continue;
            var tier = addOns.FirstOrDefault(t => t.PmrtId == a.PmrtId);
            if (tier == null)
            {
                response.Warnings.Add($"Add-on {a.PmrtId} is not on this rule and was skipped.");
                continue;
            }
            var price = tier.FirstUnitPrice ?? 0m;
            var line = new PmPriceLineDto
            {
                Kind = "AddOn",
                Label = tier.AddOnLabel ?? tier.Label ?? "Add-on",
                Quantity = a.Quantity,
                UnitPrice = price,
                Amount = Math.Round(price * a.Quantity, 2),
                PmrtId = tier.PmrtId
            };
            response.Lines.Add(line);
            response.AddOnsTotal += line.Amount;
        }

        // first-time trip charge
        if (request.FirstTime && string.Equals(rule?.Pm?.FirstTimeRule, "TripChargeExtra", StringComparison.OrdinalIgnoreCase))
        {
            var trip = rule!.Pm!.TripCharge ?? request.FallbackTripCharge ?? 0m;
            if (trip > 0)
            {
                response.Lines.Add(new PmPriceLineDto
                {
                    Kind = "TripCharge",
                    Label = "Trip charge (first-time PM)",
                    Quantity = 1,
                    UnitPrice = trip,
                    Amount = trip,
                    Note = rule.Pm.TripCharge.HasValue ? "From the PM rule" : "Company / trade trip charge"
                });
                response.TripCharge = trip;
            }
            else
            {
                response.Warnings.Add("The first-time rule adds a trip charge, but no trip charge amount is set on the rule or the company.");
            }
        }

        response.ComputedTotal = Math.Round(response.UnitsTotal + response.AddOnsTotal + response.TripCharge, 2);
        response.Total = response.ComputedTotal;

        if (request.OverrideTotal.HasValue && request.OverrideTotal.Value != response.ComputedTotal)
        {
            response.OverrideTotal = request.OverrideTotal;
            response.OverrideReason = request.OverrideReason;
            response.Total = Math.Round(request.OverrideTotal.Value, 2);
            response.Lines.Add(new PmPriceLineDto
            {
                Kind = "Override",
                Label = "Office override of the contract total",
                Quantity = 1,
                UnitPrice = response.Total - response.ComputedTotal,
                Amount = response.Total - response.ComputedTotal,
                Note = string.IsNullOrWhiteSpace(request.OverrideReason) ? null : request.OverrideReason
            });
        }

        return response;
    }

    /// <summary>Own equipment type first, then a type-less tier; tonnage must sit in the band (open bounds allowed).</summary>
    public static PmRateTierDto? MatchTier(List<PmRateTierDto> tiers, PmPriceUnitRequest unit)
    {
        bool TonsFit(PmRateTierDto t)
        {
            if (!unit.CapacityTons.HasValue) return !t.MinTons.HasValue && !t.MaxTons.HasValue;
            var tons = unit.CapacityTons.Value;
            if (t.MinTons.HasValue && tons < t.MinTons.Value) return false;
            if (t.MaxTons.HasValue && tons > t.MaxTons.Value) return false;
            return true;
        }
        if (unit.AscId.HasValue)
        {
            var own = tiers.FirstOrDefault(t => t.AscId == unit.AscId && TonsFit(t));
            if (own != null) return own;
        }
        return tiers.FirstOrDefault(t => !t.AscId.HasValue && TonsFit(t));
    }

    public static string TierLabel(PmRateTierDto t)
    {
        if (!string.IsNullOrWhiteSpace(t.Label)) return t.Label!;
        var type = string.IsNullOrWhiteSpace(t.EquipmentType) ? "Any type" : t.EquipmentType!;
        var band = (t.MinTons, t.MaxTons) switch
        {
            (null, null) => "",
            (null, var max) => $" ≤{max:0.#} t",
            (var min, null) => $" ≥{min:0.#} t",
            (var min, var max) => $" {min:0.#}–{max:0.#} t"
        };
        return type + band;
    }

    private static string DescribeUnit(PmPriceUnitRequest u) =>
        (u.AscId.HasValue ? $"type {u.AscId}" : "no type") + ", " + (u.CapacityTons.HasValue ? $"{u.CapacityTons:0.#} t" : "no tonnage");
}
