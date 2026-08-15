using EvoAPI.Shared.DTOs.Authentication;

namespace EvoAPI.Core.Interfaces;

public interface ITimeTrackingService
{
    /// <summary>
    /// Creates a login TimeTracking record (ttt_id = 1) with geolocation
    /// </summary>
    Task<int> CreateLoginTrackingAsync(int userId, decimal latitude, decimal longitude);

    /// <summary>
    /// Updates the logout time and geolocation for the current login session
    /// </summary>
    Task<bool> CreateLogoutTrackingAsync(int userId, decimal latitude, decimal longitude);

    /// <summary>
    /// Retrieves the aggregate TimeTracking status for today
    /// </summary>
    Task<TimeTrackingStatus> GetTimeTrackingStatusAsync(int userId);

    /// <summary>
    /// Clock in (ttt_id = 2) with geolocation
    /// </summary>
    Task<int> ClockInAsync(int userId, decimal latitude, decimal longitude);

    /// <summary>
    /// Clock out (ttt_id = 2) with geolocation
    /// </summary>
    Task<bool> ClockOutAsync(int userId, decimal latitude, decimal longitude);

    /// <summary>
    /// Start break (ttt_id = 4) with geolocation
    /// </summary>
    Task<int> StartBreakAsync(int userId, decimal latitude, decimal longitude);

    /// <summary>
    /// End break (ttt_id = 4) with geolocation
    /// </summary>
    Task<bool> EndBreakAsync(int userId, decimal latitude, decimal longitude);
}
