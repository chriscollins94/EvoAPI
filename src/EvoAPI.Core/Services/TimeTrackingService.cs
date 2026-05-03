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

        // Idempotent: if an active record of this type already exists, return its id
        // instead of inserting a duplicate. HOLDLOCK+UPDLOCK serializes concurrent
        // requests so two in-flight calls cannot both pass the existence check.
        var sql = @"
            BEGIN TRANSACTION;

            DECLARE @existingId int;
            SELECT @existingId = tt_id
            FROM TimeTracking WITH (HOLDLOCK, UPDLOCK)
            WHERE u_id = @u_id
              AND ttt_id = @ttt_id
              AND tt_end IS NULL;

            IF @existingId IS NULL
            BEGIN
                INSERT INTO TimeTracking (ttt_id, u_id, tt_begin, tt_begin_lat, tt_begin_lon)
                VALUES (@ttt_id, @u_id, GETUTCDATE(), @latitude, @longitude);
                SET @existingId = CAST(SCOPE_IDENTITY() AS int);
            END

            COMMIT TRANSACTION;
            SELECT @existingId;";

        var parameters = new Dictionary<string, object>
        {
            { "@ttt_id", loginTypeId },
            { "@u_id", userId },
            { "@latitude", latitude },
            { "@longitude", longitude }
        };

        var result = await _dataService.ExecuteQueryAsync(sql, parameters);

        if (result.Rows.Count > 0 && result.Rows[0][0] != DBNull.Value)
        {
            var ttId = Convert.ToInt32(result.Rows[0][0]);
            _logger.LogInformation("Login tracking record ensured with ID: {TimeTrackingId}", ttId);
            return ttId;
        }

        _logger.LogWarning("Failed to create login tracking record for user: {UserId}", userId);
        return 0;
    }

    public async Task<bool> CreateLogoutTrackingAsync(int userId, decimal latitude, decimal longitude)
    {
        _logger.LogInformation("Creating logout tracking record for user: {UserId}", userId);

        var loginTypeId = _configuration.GetValue<int>("TimeTracking:LoginTypeId", 1);

        // Close whatever login session is currently open, regardless of when it
        // started — handles users who didn't formally log out the prior day.
        var sql = @"
            UPDATE TimeTracking
            SET tt_end = GETUTCDATE(),
                tt_end_lat = @latitude,
                tt_end_lon = @longitude
            WHERE u_id = @u_id
              AND ttt_id = @ttt_id
              AND tt_end IS NULL";

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
                ISNULL(SUM(DATEDIFF(MINUTE, tt_begin, ISNULL(tt_end, GETUTCDATE()))), 0) as MinutesWorked
            FROM TimeTracking
            WHERE u_id = @u_id
              AND CAST(tt_begin as DATE) = CAST(GETUTCDATE() as DATE)";

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

    public async Task<int> ClockInAsync(int userId, decimal latitude, decimal longitude)
    {
        _logger.LogInformation("Clock in for user: {UserId}", userId);

        // Idempotent: rapid/duplicate requests (e.g. React double-fire, retries)
        // return the existing open clock-in's id instead of inserting a new row.
        var sql = @"
            BEGIN TRANSACTION;

            DECLARE @existingId int;
            SELECT @existingId = tt_id
            FROM TimeTracking WITH (HOLDLOCK, UPDLOCK)
            WHERE u_id = @u_id
              AND ttt_id = 2
              AND tt_end IS NULL;

            IF @existingId IS NULL
            BEGIN
                INSERT INTO TimeTracking (ttt_id, u_id, tt_begin, tt_begin_lat, tt_begin_lon)
                VALUES (2, @u_id, GETUTCDATE(), @latitude, @longitude);
                SET @existingId = CAST(SCOPE_IDENTITY() AS int);
            END

            COMMIT TRANSACTION;
            SELECT @existingId;";

        var parameters = new Dictionary<string, object>
        {
            { "@u_id", userId },
            { "@latitude", latitude },
            { "@longitude", longitude }
        };

        var result = await _dataService.ExecuteQueryAsync(sql, parameters);

        if (result.Rows.Count > 0 && result.Rows[0][0] != DBNull.Value)
        {
            var ttId = Convert.ToInt32(result.Rows[0][0]);
            _logger.LogInformation("Clock in record ensured with ID: {TimeTrackingId}", ttId);
            return ttId;
        }

        _logger.LogWarning("Failed to create clock in record for user: {UserId}", userId);
        return 0;
    }

    public async Task<bool> ClockOutAsync(int userId, decimal latitude, decimal longitude)
    {
        _logger.LogInformation("Clock out for user: {UserId}", userId);

        // Close whatever clock-in is currently open for this user, regardless of
        // when it started — supports overnight shifts and stranded sessions that
        // span UTC midnight.
        var sql = @"
            UPDATE TimeTracking
            SET tt_end = GETUTCDATE(),
                tt_end_lat = @latitude,
                tt_end_lon = @longitude
            WHERE u_id = @u_id
              AND ttt_id = 2
              AND tt_end IS NULL";

        var parameters = new Dictionary<string, object>
        {
            { "@u_id", userId },
            { "@latitude", latitude },
            { "@longitude", longitude }
        };

        var rowsAffected = await _dataService.ExecuteNonQueryAsync(sql, parameters);

        _logger.LogInformation("Clock out updated: {RowsAffected} rows affected", rowsAffected);
        return rowsAffected > 0;
    }

    public async Task<int> StartBreakAsync(int userId, decimal latitude, decimal longitude)
    {
        _logger.LogInformation("Start break for user: {UserId}", userId);

        // Idempotent: duplicate start-break requests return the existing open
        // break's id rather than inserting another row.
        var sql = @"
            BEGIN TRANSACTION;

            DECLARE @existingId int;
            SELECT @existingId = tt_id
            FROM TimeTracking WITH (HOLDLOCK, UPDLOCK)
            WHERE u_id = @u_id
              AND ttt_id = 4
              AND tt_end IS NULL;

            IF @existingId IS NULL
            BEGIN
                INSERT INTO TimeTracking (ttt_id, u_id, tt_begin, tt_begin_lat, tt_begin_lon)
                VALUES (4, @u_id, GETUTCDATE(), @latitude, @longitude);
                SET @existingId = CAST(SCOPE_IDENTITY() AS int);
            END

            COMMIT TRANSACTION;
            SELECT @existingId;";

        var parameters = new Dictionary<string, object>
        {
            { "@u_id", userId },
            { "@latitude", latitude },
            { "@longitude", longitude }
        };

        var result = await _dataService.ExecuteQueryAsync(sql, parameters);

        if (result.Rows.Count > 0 && result.Rows[0][0] != DBNull.Value)
        {
            var ttId = Convert.ToInt32(result.Rows[0][0]);
            _logger.LogInformation("Break record ensured with ID: {TimeTrackingId}", ttId);
            return ttId;
        }

        _logger.LogWarning("Failed to create break record for user: {UserId}", userId);
        return 0;
    }

    public async Task<bool> EndBreakAsync(int userId, decimal latitude, decimal longitude)
    {
        _logger.LogInformation("End break for user: {UserId}", userId);

        // Close whatever break is currently open, regardless of when it started.
        var sql = @"
            UPDATE TimeTracking
            SET tt_end = GETUTCDATE(),
                tt_end_lat = @latitude,
                tt_end_lon = @longitude
            WHERE u_id = @u_id
              AND ttt_id = 4
              AND tt_end IS NULL";

        var parameters = new Dictionary<string, object>
        {
            { "@u_id", userId },
            { "@latitude", latitude },
            { "@longitude", longitude }
        };

        var rowsAffected = await _dataService.ExecuteNonQueryAsync(sql, parameters);

        _logger.LogInformation("End break updated: {RowsAffected} rows affected", rowsAffected);
        return rowsAffected > 0;
    }
}
