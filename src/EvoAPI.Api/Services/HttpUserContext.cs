using EvoAPI.Core.Interfaces;

namespace EvoAPI.Api.Services;

/// <summary>
/// Resolves the current user from the JWT claims on the active HTTP request.
/// Claim names match BaseController ("id", "username"/"unique_name").
/// </summary>
public class HttpUserContext : IUserContext
{
    private readonly IHttpContextAccessor _httpContextAccessor;

    public HttpUserContext(IHttpContextAccessor httpContextAccessor)
    {
        _httpContextAccessor = httpContextAccessor;
    }

    public int? UserId
    {
        get
        {
            var value = _httpContextAccessor.HttpContext?.User?.FindFirst("id")?.Value;
            return int.TryParse(value, out var id) && id > 0 ? id : null;
        }
    }

    public string? Username
    {
        get
        {
            var user = _httpContextAccessor.HttpContext?.User;
            return user?.FindFirst("username")?.Value ?? user?.FindFirst("unique_name")?.Value;
        }
    }
}
