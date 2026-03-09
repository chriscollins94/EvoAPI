using Microsoft.AspNetCore.Authorization;

namespace EvoAPI.Shared.Attributes;

/// <summary>
/// Requires the user to have the Admin - Attack Points function claim.
/// </summary>
public class AttackPointsOnlyAttribute : AuthorizeAttribute
{
    public AttackPointsOnlyAttribute() : base("AttackPointsOnly") { }
}
