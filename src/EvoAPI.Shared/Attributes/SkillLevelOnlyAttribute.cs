using Microsoft.AspNetCore.Authorization;

namespace EvoAPI.Shared.Attributes;

/// <summary>
/// Requires the user to have the Admin - Skill Level function claim.
/// </summary>
public class SkillLevelOnlyAttribute : AuthorizeAttribute
{
    public SkillLevelOnlyAttribute() : base("SkillLevelOnly") { }
}
