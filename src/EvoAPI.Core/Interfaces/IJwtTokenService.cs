using EvoAPI.Shared.DTOs.Authentication;
using System.Security.Claims;

namespace EvoAPI.Core.Interfaces;

public interface IJwtTokenService
{
    /// <summary>
    /// Creates a JWT token compatible with EvoWS format
    /// </summary>
    JwtTokenResult CreateJwtToken(AuthenticatedUser user, string xsrfToken, int timeoutInMinutes);

    /// <summary>
    /// Validates a JWT token and returns the claims principal
    /// </summary>
    ClaimsPrincipal? ValidateJwtToken(string jwtToken);
}
