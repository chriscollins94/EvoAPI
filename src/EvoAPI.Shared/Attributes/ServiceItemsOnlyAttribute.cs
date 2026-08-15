using Microsoft.AspNetCore.Authorization;

namespace EvoAPI.Shared.Attributes;

/// <summary>
/// Requires the user to have the Admin - Service Items function claim.
/// </summary>
public class ServiceItemsOnlyAttribute : AuthorizeAttribute
{
    public ServiceItemsOnlyAttribute() : base("ServiceItemsOnly") { }
}
