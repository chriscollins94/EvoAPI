using Microsoft.AspNetCore.Authorization;

namespace EvoAPI.Shared.Attributes;

/// <summary>
/// Requires user to have the "Admin - Company" function.
/// </summary>
public class CompanyAdminOnlyAttribute : AuthorizeAttribute
{
    public CompanyAdminOnlyAttribute() : base("CompanyAdminOnly") { }
}
