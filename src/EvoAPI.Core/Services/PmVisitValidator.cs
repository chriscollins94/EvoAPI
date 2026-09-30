using System.Text.Json;
using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Services;

/// <summary>
/// Server-side check before a PM visit is submitted (PM build slice 4b). Walks the resolved sections the tech saw and
/// lists every required question without an answer, evaluating answer-dependent conditions ({"whenCode","whenIn"}, "any")
/// against the saved answers the same way the visit page does, so a hidden follow-up is never demanded.
/// Asset / season / type conditions were already decided when the sections were resolved.
/// </summary>
public static class PmVisitValidator
{
    public class Unresolved
    {
        public string Scope { get; set; } = string.Empty;   // Site | Checkout | unit label
        public int? PmvuId { get; set; }
        public string Code { get; set; } = string.Empty;
        public string Question { get; set; } = string.Empty;
        public int RepeatIndex { get; set; }
    }

    /// <summary>Answers for one scope (unit or site level) keyed by code and repeat index.</summary>
    public sealed class Scope
    {
        public string Name { get; set; } = string.Empty;
        public int? PmvuId { get; set; }
        public List<FormPreviewSectionDto> Sections { get; set; } = new();
        public Dictionary<string, PmVisitAnswerDto> ByCode { get; set; } = new(StringComparer.OrdinalIgnoreCase);       // repeat 0 / first answer
        public Dictionary<string, PmVisitAnswerDto> ByCodeIndex { get; set; } = new(StringComparer.OrdinalIgnoreCase);  // "code#index"
        public Dictionary<string, PmVisitAnswerDto>? SiteByCode { get; set; }                                          // fallback for unit scopes
    }

    public static Scope BuildScope(string name, int? pmvuId, List<FormPreviewSectionDto> sections, IEnumerable<PmVisitAnswerDto> answers, Dictionary<string, PmVisitAnswerDto>? siteByCode)
    {
        var scope = new Scope { Name = name, PmvuId = pmvuId, Sections = sections, SiteByCode = siteByCode };
        foreach (var a in answers.Where(a => (a.PmvuId ?? 0) == (pmvuId ?? 0) && a.Code != null))
        {
            if (!HasValue(a)) continue;
            scope.ByCodeIndex[$"{a.Code}#{a.RepeatIndex}"] = a;
            if (!scope.ByCode.ContainsKey(a.Code!) || a.RepeatIndex == 0) scope.ByCode[a.Code!] = a;
        }
        return scope;
    }

    public static bool HasValue(PmVisitAnswerDto? a) => a != null && (!string.IsNullOrWhiteSpace(a.Answer) || a.AttId.HasValue);

    /// <summary>Required (Always) questions that are visible and still unanswered. Extra rules: which codes are required regardless / never.</summary>
    public static List<Unresolved> Check(Scope scope, ISet<string>? forceRequired = null, ISet<string>? forceOptional = null)
    {
        var result = new List<Unresolved>();
        string? Get(string code) => scope.ByCode.TryGetValue(code, out var a) ? a.Answer : (scope.SiteByCode != null && scope.SiteByCode.TryGetValue(code, out var s) ? s.Answer : null);

        foreach (var s in scope.Sections)
        {
            foreach (var q in s.Questions.Where(q => q.Included))
            {
                if (string.Equals(q.AnswerType, "Readonly", StringComparison.OrdinalIgnoreCase)) continue;
                var required = string.Equals(q.Requirement, "Always", StringComparison.OrdinalIgnoreCase);
                if (forceRequired != null && forceRequired.Contains(q.Code)) required = true;
                if (forceOptional != null && forceOptional.Contains(q.Code)) required = false;
                if (!required) continue;
                if (q.DependsOnAnswer && EvaluateWhen(q.Condition, Get) != true) continue;

                var indexes = !string.IsNullOrWhiteSpace(q.RepeatKey) && q.RepeatCount is > 0 ? Enumerable.Range(1, Math.Min(q.RepeatCount.Value, 20)) : new[] { 0 };
                foreach (var idx in indexes)
                {
                    if (scope.ByCodeIndex.TryGetValue($"{q.Code}#{idx}", out var a) && HasValue(a)) continue;
                    result.Add(new Unresolved { Scope = scope.Name, PmvuId = scope.PmvuId, Code = q.Code, Question = q.Question, RepeatIndex = idx });
                }
            }
        }
        return result;
    }

    /// <summary>true / false / null (referenced answer missing) for the answer-dependent part of a condition.</summary>
    public static bool? EvaluateWhen(string? conditionJson, Func<string, string?> getAnswerByCode)
    {
        if (string.IsNullOrWhiteSpace(conditionJson)) return true;
        try
        {
            using var doc = JsonDocument.Parse(conditionJson);
            return EvalObject(doc.RootElement, getAnswerByCode);
        }
        catch (JsonException) { return true; }
    }

    private static bool? EvalObject(JsonElement obj, Func<string, string?> get)
    {
        if (obj.ValueKind != JsonValueKind.Object) return true;
        var results = new List<bool?>();
        foreach (var prop in obj.EnumerateObject())
        {
            if (prop.Name == "any")
            {
                var subs = prop.Value.ValueKind == JsonValueKind.Array ? prop.Value.EnumerateArray().Select(v => EvalObject(v, get)).ToList() : new List<bool?>();
                results.Add(subs.Contains(true) ? true : subs.All(r => r == false) ? false : null);
            }
            else if (prop.Name == "whenCode")
            {
                var answer = get(prop.Value.ToString());
                if (string.IsNullOrWhiteSpace(answer)) { results.Add(null); continue; }
                if (!obj.TryGetProperty("whenIn", out var whenIn)) { results.Add(true); continue; }
                var wanted = whenIn.ValueKind == JsonValueKind.Array ? whenIn.EnumerateArray().Select(x => x.ToString()).ToList() : new List<string> { whenIn.ToString() };
                if (wanted.Contains("*")) { results.Add(true); continue; }
                var have = answer.Split(';').Select(x => x.Trim());
                results.Add(have.Any(h => wanted.Any(w => string.Equals(w, h, StringComparison.OrdinalIgnoreCase))));
            }
        }
        if (results.Contains(false)) return false;
        if (results.Contains(null)) return null;
        return true;
    }
}
