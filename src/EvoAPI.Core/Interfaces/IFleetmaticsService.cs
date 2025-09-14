using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface IFleetmaticsService
{
    /// <summary>
    /// Authenticates with Fleetmatics API and returns an access token
    /// </summary>
    /// <returns>Access token valid for 20 minutes</returns>
    Task<string> GetAccessTokenAsync();
    
    /// <summary>
    /// Gets vehicle assignment for a specific driver
    /// </summary>
    /// <param name="driverIdentifier">Driver identifier (username or employee ID)</param>
    /// <returns>Vehicle number assigned to the driver</returns>
    Task<string?> GetDriverVehicleAssignmentAsync(string driverIdentifier);
    
    /// <summary>
    /// Synchronizes vehicle assignments for all active users
    /// </summary>
    /// <returns>Sync result summary</returns>
    Task<FleetmaticsSyncResultDto> SyncAllVehicleAssignmentsAsync();
    
    /// <summary>
    /// Gets all users who need Fleetmatics synchronization
    /// </summary>
    /// <returns>List of users eligible for sync</returns>
    Task<List<UserFleetmaticsDto>> GetUsersForSyncAsync();
    
    /// <summary>
    /// Updates a user's vehicle number in the database
    /// </summary>
    /// <param name="userId">User ID</param>
    /// <param name="vehicleNumber">New vehicle number</param>
    /// <returns>True if successful</returns>
    Task<bool> UpdateUserVehicleNumberAsync(int userId, string vehicleNumber);
}
