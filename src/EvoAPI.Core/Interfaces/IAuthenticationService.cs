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
    /// Whether the weekly 3-digit login code is enabled
    /// (ConfigSetting cs_type='featureflag', cs_identifier='Weekly2faCode').
    /// Missing/unparseable row counts as enabled so the flag fails safe.
    /// </summary>
    Task<bool> IsWeekly2faCodeEnabledAsync();

    /// <summary>
    /// Retrieves user permissions and functions from the database
    /// </summary>
    Task<List<string>> GetUserPermissionsAsync(string username);

    /// <summary>
    /// Changes a user's password after verifying their current password.
    /// On success, updates u_password and bumps u_passwordchanged to NOW.
    /// </summary>
    Task ChangePasswordAsync(int userId, string currentPassword, string newPassword);
}
