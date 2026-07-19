using Microsoft.AspNetCore.Authorization;

namespace EvoAPI.Shared.Attributes;

/// <summary>
/// Requires the user to have the Admin - Performance function claim.
/// </summary>
public class PerformanceOnlyAttribute : AuthorizeAttribute
{
    public PerformanceOnlyAttribute() : base("PerformanceOnly") { }
}
