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
        _logger.LogInformation("Validating credentials for user: {Username}", username);

        // Query user from database
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
              AND u.u_active = 1
              AND (@require2fa = 0 OR u.u_2fa = 1)";

        var parameters = new Dictionary<string, object>
        {
            { "@username", username },
            { "@password", password },
            { "@require2fa", require2fa ? 1 : 0 }
        };

        var result = await _dataService.ExecuteQueryAsync(sql, parameters);

        if (result.Rows.Count != 1)
        {
            _logger.LogWarning("Authentication failed for user: {Username}", username);
            throw new UnauthorizedAccessException("Invalid username or password");
        }

        var row = result.Rows[0];

        // Get user permissions
        var functions = await GetUserPermissionsAsync(username);

        // Determine access level
        var accessLevel = DetermineAccessLevel(functions);

        // Add access level to functions list for frontend compatibility
        // Frontend checks user.function.includes('ADMIN') or includes('TECH')
        if (!string.IsNullOrEmpty(accessLevel) && !functions.Contains(accessLevel))
        {
            functions.Add(accessLevel);
        }

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

    public string CalculateSecureCode()
    {
        // Get seed values from appsettings.json
        var seedLogin = _configuration.GetValue<int>("Authentication:SeedLogin");
        var seedWeek = _configuration.GetValue<int>("Authentication:SeedWeek");
        var weekOfYear = GetWeekOfYear(DateTime.Now);

        var calculation = Math.Ceiling((double)seedLogin / (seedWeek * weekOfYear));
        var result = calculation.ToString();

        // Return last 3 digits
        return result.Length >= 3
            ? result.Substring(result.Length - 3)
            : result.PadLeft(3, '0');
    }

    public async Task<List<string>> GetUserPermissionsAsync(string username)
    {
        var sql = @"
            SELECT DISTINCT f.f_function
            FROM [User] u
            INNER JOIN xrefUserRole xur ON u.u_id = xur.u_id
            INNER JOIN Role r ON r.r_id = xur.r_id
            INNER JOIN xrefRoleFunction xrf ON r.r_id = xrf.r_id
            INNER JOIN [Function] f ON xrf.f_id = f.f_id
            WHERE u.u_username = @username
            ORDER BY f.f_function";

        var parameters = new Dictionary<string, object>
        {
            { "@username", username }
        };

        var result = await _dataService.ExecuteQueryAsync(sql, parameters);

        var functions = new List<string>();
        foreach (DataRow row in result.Rows)
        {
            functions.Add(row["f_function"].ToString() ?? string.Empty);
        }

        return functions;
    }

    private int GetWeekOfYear(DateTime date)
    {
        var culture = CultureInfo.CurrentCulture;
        var calendar = culture.Calendar;
        var calendarWeekRule = culture.DateTimeFormat.CalendarWeekRule;
        var firstDayOfWeek = culture.DateTimeFormat.FirstDayOfWeek;

        return calendar.GetWeekOfYear(date, calendarWeekRule, firstDayOfWeek);
    }

    private string DetermineAccessLevel(List<string> functions)
    {
        if (functions.Any(f => f.Contains("ADMIN", StringComparison.OrdinalIgnoreCase)))
            return "ADMIN";

        if (functions.Any(f => f.Equals("TECH", StringComparison.OrdinalIgnoreCase)))
            return "TECH";

        return "USER";
    }
}
