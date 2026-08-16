using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs.Authentication;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Globalization;

namespace EvoAPI.Core.Services;

public class AuthenticationService : IAuthenticationService
{
    private readonly IDataService _dataService;
    private readonly IConfiguration _configuration;
    private readonly ILogger<AuthenticationService> _logger;

    public AuthenticationService(
        IDataService dataService,
        IConfiguration configuration,
        ILogger<AuthenticationService> logger)
    {
        _dataService = dataService;
        _configuration = configuration;
        _logger = logger;
    }

    public async Task<AuthenticatedUser> ValidateCredentialsAsync(string username, string password, bool require2fa = false)
    {
        _logger.LogInformation("Validating credentials for user: {Username}, require2fa: {Require2fa}", username, require2fa);

        // First, query user to check if they require 2FA
        var userCheckSql = @"
            SELECT u.u_id, u.u_2fa, u.u_active
            FROM [User] u
            WHERE u.u_username = @username";

        var userCheckParams = new Dictionary<string, object> { { "@username", username } };
        var userCheckResult = await _dataService.ExecuteQueryAsync(userCheckSql, userCheckParams);

        if (userCheckResult.Rows.Count == 0)
        {
            _logger.LogWarning("User not found: {Username}", username);
            throw new UnauthorizedAccessException("Invalid username or password");
        }

        var userRow = userCheckResult.Rows[0];
        var userRequires2fa = Convert.ToInt32(userRow["u_2fa"]) == 1;
        var userActive = Convert.ToInt32(userRow["u_active"]) == 1;

        _logger.LogInformation("User check for {Username}: Active={Active}, Requires2FA={Requires2FA}", 
            username, userActive, userRequires2fa);

        // If account is not active, reject
        if (!userActive)
        {
            _logger.LogWarning("Account not active: {Username}", username);
            throw new UnauthorizedAccessException("Invalid username or password");
        }

        // If account requires 2FA but no valid code was provided, reject
        // (unless the Weekly2faCode feature flag has the code disabled globally)
        if (userRequires2fa && !require2fa && await IsWeekly2faCodeEnabledAsync())
        {
            _logger.LogWarning("2FA required but not provided for user: {Username}", username);
            throw new UnauthorizedAccessException("Invalid username or password");
        }

        // Now validate credentials
        var sql = @"
            SELECT
                u.u_id,
                u.u_username,
                u.u_password,
                u.u_firstname,
                u.u_lastname,
                u.u_picture,
                u.u_passwordchanged,
                u.u_active,
                u.u_2fa
            FROM [User] u
            WHERE u.u_username = @username
              AND u.u_password = @password
              AND u.u_active = 1";

        var parameters = new Dictionary<string, object>
        {
            { "@username", username },
            { "@password", password }
        };

        _logger.LogInformation("Validating password for user: {Username}, checking password match in database", username);

        var result = await _dataService.ExecuteQueryAsync(sql, parameters);

        if (result.Rows.Count != 1)
        {
            _logger.LogWarning("Password validation failed for user: {Username}. Expected 1 row, got {RowCount}", username, result.Rows.Count);
            throw new UnauthorizedAccessException("Invalid username or password");
        }

        var row = result.Rows[0];

        // Get user permissions (using f_functionidentifier which includes TECH/ADMIN)
        var functions = await GetUserPermissionsAsync(username);

        // Determine access level from function identifiers
        var accessLevel = DetermineAccessLevel(functions);

        return new AuthenticatedUser
        {
            UserId = Convert.ToInt32(row["u_id"]),
            Username = row["u_username"].ToString() ?? string.Empty,
            FirstName = row["u_firstname"].ToString() ?? string.Empty,
            LastName = row["u_lastname"].ToString() ?? string.Empty,
            Picture = row["u_picture"].ToString() ?? string.Empty,
            PasswordChanged = row["u_passwordchanged"] == DBNull.Value
                ? null
                : Convert.ToDateTime(row["u_passwordchanged"]),
            Functions = functions,
            AccessLevel = accessLevel
        };
    }

    public async Task ChangePasswordAsync(int userId, string currentPassword, string newPassword)
    {
        _logger.LogInformation("Password change requested for user id: {UserId}", userId);

        // Verify the current password belongs to this user (and the account is active).
        var verifySql = @"
            SELECT u_id
            FROM [User]
            WHERE u_id = @userId
              AND u_password = @currentPassword
              AND u_active = 1";

        var verifyParams = new Dictionary<string, object>
        {
            { "@userId", userId },
            { "@currentPassword", currentPassword }
        };

        var verifyResult = await _dataService.ExecuteQueryAsync(verifySql, verifyParams);
        if (verifyResult.Rows.Count != 1)
        {
            _logger.LogWarning("Password change failed for user id {UserId}: current password did not match", userId);
            throw new UnauthorizedAccessException("Current password is incorrect");
        }

        if (string.Equals(currentPassword, newPassword, StringComparison.Ordinal))
        {
            throw new ArgumentException("New password cannot be the same as the current password");
        }

        var updateSql = @"
            UPDATE [User]
            SET u_password = @newPassword,
                u_passwordchanged = GETDATE(),
                u_modifieddatetime = GETDATE()
            WHERE u_id = @userId";

        var updateParams = new Dictionary<string, object>
        {
            { "@userId", userId },
            { "@newPassword", newPassword }
        };

        var rowsAffected = await _dataService.ExecuteNonQueryAsync(updateSql, updateParams);
        if (rowsAffected != 1)
        {
            _logger.LogError("Password change for user id {UserId} affected {RowsAffected} rows (expected 1)", userId, rowsAffected);
            throw new InvalidOperationException("Password update failed");
        }

        _logger.LogInformation("Password changed successfully for user id: {UserId}", userId);
    }

    public string CalculateSecureCode()
    {
        // Get seed values from appsettings.json
        var seedLogin = _configuration.GetValue<int>("Authentication:SeedLogin");
        var seedWeek = _configuration.GetValue<int>("Authentication:SeedWeek");
        var weekOfYear = GetWeekOfYear(DateTime.Now);

        _logger.LogInformation("2FA Code Calculation: SeedLogin={SeedLogin}, SeedWeek={SeedWeek}, WeekOfYear={WeekOfYear}", 
            seedLogin, seedWeek, weekOfYear);

        var calculation = Math.Ceiling((double)seedLogin / (seedWeek * weekOfYear));
        var result = calculation.ToString();

        _logger.LogInformation("2FA Code Calculation: {SeedLogin} / ({SeedWeek} * {WeekOfYear}) = {Calculation}, Result: {Result}", 
            seedLogin, seedWeek, weekOfYear, calculation, result);

        // Return last 3 digits
        var code = result.Length >= 3
            ? result.Substring(result.Length - 3)
            : result.PadLeft(3, '0');
        
        _logger.LogInformation("2FA Code: {Code}", code);

        return code;
    }

    public async Task<bool> IsWeekly2faCodeEnabledAsync()
    {
        // Anything other than an explicit off value ('0'/'false') keeps the
        // code required, so a missing or mistyped row can't silently drop 2FA.
        var value = await _dataService.GetConfigSettingValueAsync("featureflag", "Weekly2faCode");
        var enabled = !string.Equals(value?.Trim(), "0", StringComparison.OrdinalIgnoreCase)
                   && !string.Equals(value?.Trim(), "false", StringComparison.OrdinalIgnoreCase);

        _logger.LogInformation("Weekly2faCode feature flag: cs_value={Value}, enabled={Enabled}", value ?? "(missing)", enabled);
        return enabled;
    }

    public async Task<List<string>> GetUserPermissionsAsync(string username)
    {
        // Use f_functionidentifier to match legacy JWT format
        // Legacy code in TokenManager.cs uses f_functionidentifier for function claims
        var sql = @"
            SELECT DISTINCT f.f_functionidentifier
            FROM [User] u
            INNER JOIN xrefUserRole xur ON u.u_id = xur.u_id
            INNER JOIN Role r ON r.r_id = xur.r_id
            INNER JOIN xrefRoleFunction xrf ON r.r_id = xrf.r_id
            INNER JOIN [Function] f ON xrf.f_id = f.f_id
            WHERE u.u_username = @username
            ORDER BY f.f_functionidentifier";

        var parameters = new Dictionary<string, object>
        {
            { "@username", username }
        };

        var result = await _dataService.ExecuteQueryAsync(sql, parameters);

        var functions = new List<string>();
        foreach (DataRow row in result.Rows)
        {
            functions.Add(row["f_functionidentifier"].ToString() ?? string.Empty);
        }

        return functions;
    }

    private int GetWeekOfYear(DateTime date)
    {
        // Use ISO 8601 week date system for consistent 2FA calculations
        // This ensures the same week number regardless of system culture/locale
        var culture = new CultureInfo("en-US");
        var calendar = culture.Calendar;
        // ISO 8601: Week starts on Monday, first week contains first Thursday
        var isoRule = CalendarWeekRule.FirstFourDayWeek;
        var monday = DayOfWeek.Monday;

        return calendar.GetWeekOfYear(date, isoRule, monday);
    }

    private string DetermineAccessLevel(List<string> functions)
    {
        // Legacy logic: Check for exact matches of "ADMIN" and "TECH" in function identifiers
        // ADMIN takes priority over TECH
        if (functions.Contains("ADMIN"))
            return "ADMIN";

        if (functions.Contains("TECH"))
            return "TECH";

        return "USER";
    }
}
