using Microsoft.AspNetCore.Authorization;

namespace EvoAPI.Shared.Attributes;

/// <summary>
/// Requires user to be logged in (authenticated)
/// </summary>
public class EvoAuthorizeAttribute : AuthorizeAttribute
{
    public EvoAuthorizeAttribute() : base("LoggedIn") { }
}
