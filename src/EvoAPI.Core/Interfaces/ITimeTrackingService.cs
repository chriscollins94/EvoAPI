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
}
