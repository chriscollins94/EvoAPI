namespace EvoAPI.Core.Interfaces;

/// <summary>
/// Identity of the user behind the current request, used to stamp
/// SESSION_CONTEXT on database connections so triggers (StatusChange u_id,
/// ServiceRequestActivity) can attribute writes. Null outside an HTTP
/// request (background jobs) or when unauthenticated.
/// </summary>
public interface IUserContext
{
    int? UserId { get; }
    string? Username { get; }
}
