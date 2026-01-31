using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs.Authentication;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;

namespace EvoAPI.Core.Services;

public class TimeTrackingService : ITimeTrackingService
{
    private readonly IDataService _dataService;
    private readonly IConfiguration _configuration;
    private readonly ILogger<TimeTrackingService> _logger;

    public TimeTrackingService(
        IDataService dataService,
        IConfiguration configuration,
        ILogger<TimeTrackingService> logger)
    {
        _dataService = dataService;
        _configuration = configuration;
        _logger = logger;
    }

    public async Task<int> CreateLoginTrackingAsync(int userId, decimal latitude, decimal longitude)
    {
        _logger.LogInformation("Creating login tracking record for user: {UserId}", userId);

        var loginTypeId = _configuration.GetValue<int>("TimeTracking:LoginTypeId", 1);

        var sql = @"
            INSERT INTO TimeTracking (ttt_id, u_id, tt_begin, tt_begin_lat, tt_begin_lon)
            VALUES (@ttt_id, @u_id, GETDATE(), @latitude, @longitude);
            SELECT CAST(SCOPE_IDENTITY() as int);";

        var parameters = new Dictionary<string, object>
        {
            { "@ttt_id", loginTypeId },
            { "@u_id", userId },
            { "@latitude", latitude },
            { "@longitude", longitude }
        };

        var result = await _dataService.ExecuteQueryAsync(sql, parameters);

        if (result.Rows.Count > 0)
        {
            var ttId = Convert.ToInt32(result.Rows[0][0]);
            _logger.LogInformation("Login tracking record created with ID: {TimeTrackingId}", ttId);
            return ttId;
        }

        _logger.LogWarning("Failed to create login tracking record for user: {UserId}", userId);
        return 0;
    }

    public async Task<bool> CreateLogoutTrackingAsync(int userId, decimal latitude, decimal longitude)
    {
        _logger.LogInformation("Creating logout tracking record for user: {UserId}", userId);

        var loginTypeId = _configuration.GetValue<int>("TimeTracking:LoginTypeId", 1);

        var sql = @"
            UPDATE TimeTracking
            SET tt_end = GETDATE(),
                tt_end_lat = @latitude,
                tt_end_lon = @longitude
            WHERE u_id = @u_id
              AND ttt_id = @ttt_id
              AND tt_end IS NULL
              AND CAST(tt_begin as DATE) = CAST(GETDATE() as DATE)";

        var parameters = new Dictionary<string, object>
        {
            { "@u_id", userId },
            { "@ttt_id", loginTypeId },
            { "@latitude", latitude },
            { "@longitude", longitude }
        };

        var rowsAffected = await _dataService.ExecuteNonQueryAsync(sql, parameters);

        _logger.LogInformation("Logout tracking updated: {RowsAffected} rows affected", rowsAffected);
        return rowsAffected > 0;
    }

    public async Task<TimeTrackingStatus> GetTimeTrackingStatusAsync(int userId)
    {
        _logger.LogInformation("Retrieving time tracking status for user: {UserId}", userId);

        var sql = @"
            SELECT
                MIN(CASE WHEN ttt_id = 1 THEN tt_begin END) as LoginTime,
                MIN(CASE WHEN ttt_id = 2 THEN tt_begin END) as ClockInTime,
                MIN(CASE WHEN ttt_id = 3 THEN tt_begin END) as CheckInTime,
                MIN(CASE WHEN ttt_id = 4 THEN tt_begin END) as BreakTime,
                ISNULL(SUM(DATEDIFF(MINUTE, tt_begin, ISNULL(tt_end, GETDATE()))), 0) as MinutesWorked
            FROM TimeTracking
            WHERE u_id = @u_id
              AND CAST(tt_begin as DATE) = CAST(GETDATE() as DATE)";

        var parameters = new Dictionary<string, object>
        {
            { "@u_id", userId }
        };

        var result = await _dataService.ExecuteQueryAsync(sql, parameters);

        if (result.Rows.Count == 0)
        {
            _logger.LogInformation("No time tracking records found for user: {UserId} today", userId);
            return new TimeTrackingStatus { MinutesWorkedToday = 0 };
        }

        var row = result.Rows[0];

        return new TimeTrackingStatus
        {
            LoginTime = row["LoginTime"] == DBNull.Value ? null : Convert.ToDateTime(row["LoginTime"]),
            ClockInTime = row["ClockInTime"] == DBNull.Value ? null : Convert.ToDateTime(row["ClockInTime"]),
            CheckInTime = row["CheckInTime"] == DBNull.Value ? null : Convert.ToDateTime(row["CheckInTime"]),
            BreakTime = row["BreakTime"] == DBNull.Value ? null : Convert.ToDateTime(row["BreakTime"]),
            MinutesWorkedToday = Convert.ToInt32(row["MinutesWorked"])
        };
    }
}
