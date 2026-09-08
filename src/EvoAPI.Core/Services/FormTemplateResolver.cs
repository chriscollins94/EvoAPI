using System.Text.Json;
using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Services;

/// <summary>
/// Resolves "template ∩ form rule ∩ season ∩ equipment / heating type" into the list of
/// questions a tech would see. Used by the rules-tab preview in slice 1; the tech visit page
/// (later slice) evaluates the same condition JSON against the real asset and answers.
///
/// Condition JSON (fs_condition / fq_condition):
///   keys in one object are ANDed; {"any":[{...},{...}]} is OR.
///   equipmentType / heatingType / season : decided here when the caller supplies the value
///   attr / mount                          : decided from the stored asset when the preview names a unit, else DependsOnAsset
///   whenCode / whenIn                     : need an answer  -> DependsOnAnswer
/// </summary>
public static class FormTemplateResolver
{
    private enum Tri { True, False, Unknown }

    private sealed class Outcome
    {
        public Tri Result = Tri.True;
        public bool DependsOnAsset;
        public bool DependsOnAnswer;
        public List<string> Parts = new();
        public string Describe() => string.Join(" and ", Parts);
    }

    public static FormPreviewDto Resolve(FormTemplateDetailDto template, FormRuleDto? rule, string? season, string? equipmentType, string? heatingType) =>
        Resolve(template, rule, new FormResolveFacts { Season = season, EquipmentType = equipmentType, HeatingType = heatingType });

    /// <summary>Full form: facts may include a stored asset's mounting, attributes and repeat counts (preview for a real unit).</summary>
    public static FormPreviewDto Resolve(FormTemplateDetailDto template, FormRuleDto? rule, FormResolveFacts facts)
    {
        facts.Season = Blank(facts.Season);
        facts.EquipmentType = Blank(facts.EquipmentType);
        facts.HeatingType = Blank(facts.HeatingType);
        facts.Mount = Blank(facts.Mount);
        var overrides = (rule?.QuestionOverrides ?? new List<FormRuleQuestionDto>()).ToDictionary(o => o.FqId, o => o);

        var preview = new FormPreviewDto
        {
            FtId = template.FtId,
            TemplateName = template.Name,
            Version = template.Version,
            Season = facts.Season,
            EquipmentType = facts.EquipmentType,
            HeatingType = facts.HeatingType,
            Mount = facts.Mount,
            AsId = facts.AsId,
            AssetLabel = facts.AssetLabel,
            AssetAttributes = facts.Attributes.Count > 0 ? new Dictionary<string, string>(facts.Attributes) : null,
            RepeatCounts = facts.RepeatCounts.Count > 0 ? new Dictionary<string, int>(facts.RepeatCounts) : null
        };

        foreach (var s in template.Sections.OrderBy(x => x.Order))
        {
            var sec = new FormPreviewSectionDto { FsId = s.FsId, Name = s.Name, Phase = s.Phase, RepeatPerUnit = s.RepeatPerUnit, Included = true };
            var secOutcome = Evaluate(s.Condition, facts);
            if (!s.Active)
            {
                sec.Included = false;
                sec.Reason = "Section is inactive";
            }
            else if (secOutcome.Result == Tri.False)
            {
                sec.Included = false;
                sec.Reason = "Section condition not met: " + secOutcome.Describe();
            }
            else if (secOutcome.Result == Tri.Unknown)
            {
                sec.Reason = "Section shown when " + secOutcome.Describe();
            }

            foreach (var q in s.Questions.OrderBy(x => x.Order))
            {
                preview.TotalQuestions++;
                var pq = ResolveQuestion(q, overrides, facts, sec.Included, secOutcome);
                if (pq.Included)
                {
                    preview.IncludedQuestions++;
                    sec.IncludedCount++;
                    if (pq.DependsOnAsset) preview.AssetDependentQuestions++;
                    if (pq.DependsOnAnswer) preview.AnswerDependentQuestions++;
                }
                sec.Questions.Add(pq);
            }
            preview.Sections.Add(sec);
        }
        return preview;
    }

    private static FormPreviewQuestionDto ResolveQuestion(FormQuestionDto q, Dictionary<int, FormRuleQuestionDto> overrides, FormResolveFacts facts, bool sectionIncluded, Outcome sectionOutcome)
    {
        overrides.TryGetValue(q.FqId, out var ovr);
        var configured = string.Equals(q.Requirement, "Configured", StringComparison.OrdinalIgnoreCase);
        var customerEnabled = configured && ovr != null && ovr.Enabled;

        var pq = new FormPreviewQuestionDto
        {
            FqId = q.FqId,
            Code = q.Code,
            Question = !string.IsNullOrWhiteSpace(ovr?.QuestionOverride) ? ovr!.QuestionOverride! : q.Question,
            AnswerType = q.AnswerType,
            TemplateRequirement = q.Requirement,
            Condition = q.Condition,
            RepeatKey = q.RepeatKey,
            RepeatCount = !string.IsNullOrWhiteSpace(q.RepeatKey) && facts.RepeatCounts.TryGetValue(q.RepeatKey, out var repeatCount) ? repeatCount : null,
            CustomerEnabled = customerEnabled,
            Included = true
        };

        // effective requirement / photo rule for this customer
        var reqOverride = Blank(ovr?.RequirementOverride);
        pq.Requirement = reqOverride != null && !string.Equals(reqOverride, "Hidden", StringComparison.OrdinalIgnoreCase)
            ? reqOverride
            : (customerEnabled ? "Always" : q.Requirement);
        var photoOverride = Blank(ovr?.PhotoOverride);
        pq.PhotoRequired = photoOverride
            ?? (string.Equals(q.PhotoRequired, "CustomerConfigured", StringComparison.OrdinalIgnoreCase) ? (customerEnabled ? "Always" : "No") : q.PhotoRequired);

        if (!sectionIncluded)
            return Exclude(pq, "Section excluded");
        if (!q.Active)
            return Exclude(pq, "Inactive in the template");
        if (ovr != null && !ovr.Enabled)
            return Exclude(pq, "Turned off for this customer");
        if (string.Equals(reqOverride, "Hidden", StringComparison.OrdinalIgnoreCase))
            return Exclude(pq, "Hidden for this customer");
        if (configured && !customerEnabled)
            return Exclude(pq, "Customer rule not switched on");

        if (facts.Season != null && !string.IsNullOrWhiteSpace(q.Seasons))
        {
            var seasons = q.Seasons.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (!seasons.Any(x => string.Equals(x, facts.Season, StringComparison.OrdinalIgnoreCase)))
                return Exclude(pq, $"Not part of a {facts.Season} visit (applies to {string.Join(" / ", seasons)})");
        }

        var outcome = Evaluate(q.Condition, facts);
        if (outcome.Result == Tri.False)
            return Exclude(pq, "Condition not met: " + outcome.Describe());

        var reasons = new List<string>();
        if (customerEnabled) reasons.Add("switched on by the customer rule");
        if (outcome.Result == Tri.Unknown) reasons.Add("shown when " + outcome.Describe());
        else if (sectionOutcome.Result == Tri.Unknown) reasons.Add("section shown when " + sectionOutcome.Describe());
        if (facts.Season == null && !string.IsNullOrWhiteSpace(q.Seasons)) reasons.Add("only on " + q.Seasons.Replace(";", " / ") + " visits");
        pq.DependsOnAsset = outcome.DependsOnAsset || sectionOutcome.DependsOnAsset;
        pq.DependsOnAnswer = outcome.DependsOnAnswer || sectionOutcome.DependsOnAnswer;
        pq.Reason = reasons.Count > 0 ? string.Join("; ", reasons) : null;
        return pq;
    }

    private static FormPreviewQuestionDto Exclude(FormPreviewQuestionDto pq, string reason)
    {
        pq.Included = false;
        pq.Reason = reason;
        return pq;
    }

    private static Outcome Evaluate(string? conditionJson, FormResolveFacts facts)
    {
        var outcome = new Outcome();
        if (string.IsNullOrWhiteSpace(conditionJson)) return outcome;
        try
        {
            using var doc = JsonDocument.Parse(conditionJson);
            EvaluateObject(doc.RootElement, facts, outcome);
        }
        catch (JsonException)
        {
            outcome.Result = Tri.Unknown;
            outcome.Parts.Add("(unreadable condition)");
        }
        return outcome;
    }

    private static void EvaluateObject(JsonElement obj, FormResolveFacts facts, Outcome outcome)
    {
        if (obj.ValueKind != JsonValueKind.Object) return;
        var results = new List<Tri>();
        foreach (var prop in obj.EnumerateObject())
        {
            switch (prop.Name)
            {
                case "any":
                {
                    var anyResults = new List<Tri>();
                    var parts = new List<string>();
                    foreach (var child in prop.Value.EnumerateArray())
                    {
                        var sub = new Outcome();
                        EvaluateObject(child, facts, sub);
                        anyResults.Add(sub.Result);
                        parts.Add(sub.Describe());
                        outcome.DependsOnAsset |= sub.DependsOnAsset;
                        outcome.DependsOnAnswer |= sub.DependsOnAnswer;
                    }
                    results.Add(anyResults.Contains(Tri.True) ? Tri.True : anyResults.All(r => r == Tri.False) ? Tri.False : Tri.Unknown);
                    outcome.Parts.Add("(" + string.Join(" or ", parts) + ")");
                    break;
                }
                case "equipmentType":
                    results.Add(Match(facts.EquipmentType, prop.Value));
                    outcome.Parts.Add("equipment type is " + Join(prop.Value));
                    break;
                case "heatingType":
                    results.Add(Match(facts.HeatingType, prop.Value));
                    outcome.Parts.Add("heating type is " + Join(prop.Value));
                    break;
                case "season":
                    results.Add(Match(facts.Season, prop.Value));
                    outcome.Parts.Add("visit type is " + Join(prop.Value));
                    break;
                case "attr":
                {
                    // {"attr":{"BeltDriven":true,"Circuits":2}}: decided from the asset's stored attributes when it is known
                    var parts = new List<string>();
                    foreach (var a in prop.Value.EnumerateObject())
                    {
                        parts.Add(a.Name + (a.Value.ValueKind == JsonValueKind.True ? "" : " = " + a.Value.ToString()));
                        if (facts.Attributes.TryGetValue(a.Name, out var have) && !string.IsNullOrWhiteSpace(have))
                            results.Add(AttributeMatches(have, a.Value) ? Tri.True : Tri.False);
                        else
                        {
                            results.Add(Tri.Unknown);
                            outcome.DependsOnAsset = true;
                        }
                    }
                    outcome.Parts.Add("the asset has " + string.Join(", ", parts));
                    break;
                }
                case "mount":
                    if (facts.Mount != null) results.Add(Match(facts.Mount, prop.Value));
                    else
                    {
                        results.Add(Tri.Unknown);
                        outcome.DependsOnAsset = true;
                    }
                    outcome.Parts.Add("the unit is mounted " + Join(prop.Value));
                    break;
                case "whenCode":
                {
                    results.Add(Tri.Unknown);
                    outcome.DependsOnAnswer = true;
                    var whenIn = obj.TryGetProperty("whenIn", out var wi) ? Join(wi) : "answered";
                    outcome.Parts.Add(prop.Value.GetString() + (whenIn == "*" ? " is answered" : " is " + whenIn));
                    break;
                }
                case "whenIn":
                    break;
                default:
                    results.Add(Tri.Unknown);
                    outcome.Parts.Add(prop.Name + " " + prop.Value.ToString());
                    break;
            }
        }
        outcome.Result = results.Contains(Tri.False) ? Tri.False : results.Contains(Tri.Unknown) ? Tri.Unknown : Tri.True;
    }

    /// <summary>Compares a stored attribute value with the value a condition asks for (true/false, a number, or text).</summary>
    private static bool AttributeMatches(string have, JsonElement want)
    {
        static bool Truthy(string s) => s.Equals("true", StringComparison.OrdinalIgnoreCase) || s.Equals("yes", StringComparison.OrdinalIgnoreCase) || s == "1";
        switch (want.ValueKind)
        {
            case JsonValueKind.True: return Truthy(have);
            case JsonValueKind.False: return !Truthy(have);
            case JsonValueKind.Number:
                return decimal.TryParse(have, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var d) && d == want.GetDecimal();
            case JsonValueKind.Array:
                return want.EnumerateArray().Any(x => string.Equals(x.ToString(), have, StringComparison.OrdinalIgnoreCase));
            default:
                return string.Equals(want.ToString(), have, StringComparison.OrdinalIgnoreCase);
        }
    }

    private static Tri Match(string? fact, JsonElement list)
    {
        if (fact == null) return Tri.Unknown;
        if (list.ValueKind == JsonValueKind.String)
            return string.Equals(list.GetString(), fact, StringComparison.OrdinalIgnoreCase) ? Tri.True : Tri.False;
        if (list.ValueKind != JsonValueKind.Array) return Tri.Unknown;
        foreach (var item in list.EnumerateArray())
            if (string.Equals(item.GetString(), fact, StringComparison.OrdinalIgnoreCase)) return Tri.True;
        return Tri.False;
    }

    private static string Join(JsonElement list)
    {
        if (list.ValueKind == JsonValueKind.Array)
            return string.Join(" / ", list.EnumerateArray().Select(x => x.ToString()));
        return list.ToString();
    }

    private static string? Blank(string? s) => string.IsNullOrWhiteSpace(s) ? null : s.Trim();
}
