using Microsoft.AspNetCore.Authorization;

namespace EvoAPI.Shared.Attributes;

/// <summary>
/// Requires user to be logged in and have ADMIN access level
/// </summary>
public class AdminOnlyAttribute : AuthorizeAttribute
{
    public AdminOnlyAttribute() : base("AdminOnly") { }
}
