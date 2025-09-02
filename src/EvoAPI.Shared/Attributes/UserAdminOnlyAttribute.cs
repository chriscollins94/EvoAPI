using Microsoft.AspNetCore.Authorization;

namespace EvoAPI.Shared.Attributes;

/// <summary>
/// Requires user to be logged in, have ADMIN access level, and have the "Admin - User Admin Edit" claim
/// </summary>
public class UserAdminOnlyAttribute : AuthorizeAttribute
{
    public UserAdminOnlyAttribute() : base("UserAdminOnly") { }
}
