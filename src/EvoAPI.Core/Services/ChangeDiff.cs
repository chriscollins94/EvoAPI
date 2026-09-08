using System.Collections;
using System.Reflection;

namespace EvoAPI.Core.Services;

/// <summary>
/// Before/after dictionaries for the critical audit log, built by reflection over a request object's simple
/// properties (strings, numbers, bools, dates). Collections and nested classes are skipped; callers diff those
/// separately if they matter. When there is no "before", every provided value is reported as new.
/// </summary>
public static class ChangeDiff
{
    public static (Dictionary<string, object?> oldValues, Dictionary<string, object?> newValues) Diff(object? before, object request, string prefix = "")
    {
        var oldValues = new Dictionary<string, object?>();
        var newValues = new Dictionary<string, object?>();
        var beforeProps = before?.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance).ToDictionary(p => p.Name, p => p, StringComparer.OrdinalIgnoreCase);
        foreach (var prop in request.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!IsSimple(prop.PropertyType)) continue;
            var newValue = prop.GetValue(request);
            object? oldValue = null;
            if (beforeProps != null && beforeProps.TryGetValue(prop.Name, out var bp) && IsSimple(bp.PropertyType))
                oldValue = bp.GetValue(before);
            if (before != null && Equal(oldValue, newValue)) continue;
            if (before == null && newValue == null) continue;
            oldValues[prefix + prop.Name] = oldValue;
            newValues[prefix + prop.Name] = newValue;
        }
        return (oldValues, newValues);
    }

    private static bool IsSimple(Type t)
    {
        t = Nullable.GetUnderlyingType(t) ?? t;
        return t.IsPrimitive || t.IsEnum || t == typeof(string) || t == typeof(decimal) || t == typeof(DateTime) || t == typeof(DateOnly) || t == typeof(Guid);
    }

    private static bool Equal(object? a, object? b)
    {
        if (a == null && b == null) return true;
        if (a == null || b == null)
            return (a ?? b) is string s && string.IsNullOrEmpty(s);
        if (a is string sa && b is string sb) return string.Equals(sa.Trim(), sb.Trim(), StringComparison.Ordinal);
        if (a is IEnumerable && !(a is string)) return false;
        try { return Convert.ToDecimal(a) == Convert.ToDecimal(b); } catch { return a.Equals(b); }
    }
}
