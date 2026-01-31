using EvoAPI.Shared.DTOs.Authentication;

namespace EvoAPI.Core.Interfaces;

public interface IAuthenticationService
{
    /// <summary>
    /// Validates user credentials and returns authenticated user information
    /// </summary>
    Task<AuthenticatedUser> ValidateCredentialsAsync(string username, string password, bool require2fa = false);

    /// <summary>
    /// Calculates the weekly rotating 2FA secure code
    /// </summary>
    string CalculateSecureCode();

    /// <summary>
    /// Retrieves user permissions and functions from the database
    /// </summary>
    Task<List<string>> GetUserPermissionsAsync(string username);
}
