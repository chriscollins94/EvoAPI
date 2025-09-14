using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("EvoApi/fleetmatics")]
public class FleetmaticsController : BaseController
{
    private readonly IFleetmaticsService _fleetmaticsService;
    private readonly IConfiguration _configuration;

    public FleetmaticsController(
        IFleetmaticsService fleetmaticsService,
        IAuditService auditService,
        IConfiguration configuration)
    {
        _fleetmaticsService = fleetmaticsService;
        _configuration = configuration;
        InitializeAuditService(auditService);
    }

    /// <summary>
    /// Manually triggers vehicle assignment synchronization for all users (Admin only)
    /// </summary>
    [HttpPost("sync-vehicle-assignments")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<FleetmaticsSyncResultDto>>> SyncVehicleAssignments()
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            var syncResult = await _fleetmaticsService.SyncAllVehicleAssignmentsAsync();
            
            stopwatch.Stop();
            await LogAuditAsync("FleetmaticsVehicleSync", syncResult, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<FleetmaticsSyncResultDto>
            {
                Success = true,
                Message = $"Vehicle assignments synchronized successfully. Updated {syncResult.SuccessfulUpdates} of {syncResult.TotalUsersProcessed} users.",
                Data = syncResult,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("FleetmaticsVehicleSync", ex);
            
            return StatusCode(500, new ApiResponse<FleetmaticsSyncResultDto>
            {
                Success = false,
                Message = "Failed to synchronize vehicle assignments"
            });
        }
    }

    /// <summary>
    /// Gets vehicle assignment for a specific driver using their employee number
    /// </summary>
    [HttpGet("driver-assignment/{employeeNumber}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<string>>> GetDriverAssignment(string employeeNumber)
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            if (string.IsNullOrWhiteSpace(employeeNumber))
            {
                return BadRequest(new ApiResponse<string>
                {
                    Success = false,
                    Message = "Employee number is required"
                });
            }

            var vehicleNumber = await _fleetmaticsService.GetDriverVehicleAssignmentAsync(employeeNumber);
            
            stopwatch.Stop();
            await LogAuditAsync("GetFleetmaticsDriverAssignment", 
                new { employeeNumber, vehicleNumber }, 
                stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            if (!string.IsNullOrEmpty(vehicleNumber))
            {
                return Ok(new ApiResponse<string>
                {
                    Success = true,
                    Message = "Driver assignment retrieved successfully",
                    Data = vehicleNumber,
                    Count = 1
                });
            }
            else
            {
                return Ok(new ApiResponse<string>
                {
                    Success = true,
                    Message = "No vehicle assignment found for employee number",
                    Data = null,
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetFleetmaticsDriverAssignment", ex, new { employeeNumber });
            
            return StatusCode(500, new ApiResponse<string>
            {
                Success = false,
                Message = "Failed to retrieve driver assignment"
            });
        }
    }

    /// <summary>
    /// Gets all users eligible for Fleetmatics synchronization (Admin only)
    /// </summary>
    [HttpGet("sync-eligible-users")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<List<UserFleetmaticsDto>>>> GetSyncEligibleUsers()
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            var users = await _fleetmaticsService.GetUsersForSyncAsync();
            
            stopwatch.Stop();
            await LogAuditAsync("GetFleetmaticsSyncEligibleUsers", 
                new { userCount = users.Count }, 
                stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<List<UserFleetmaticsDto>>
            {
                Success = true,
                Message = "Sync eligible users retrieved successfully",
                Data = users,
                Count = users.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetFleetmaticsSyncEligibleUsers", ex);
            
            return StatusCode(500, new ApiResponse<List<UserFleetmaticsDto>>
            {
                Success = false,
                Message = "Failed to retrieve sync eligible users"
            });
        }
    }

    /// <summary>
    /// Updates vehicle number for a specific user (Admin only)
    /// </summary>
    [HttpPut("update-vehicle-number")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<object>>> UpdateVehicleNumber([FromBody] UpdateVehicleNumberRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            if (request.UserId <= 0)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Valid user ID is required"
                });
            }

            if (string.IsNullOrWhiteSpace(request.VehicleNumber))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Vehicle number is required"
                });
            }

            var success = await _fleetmaticsService.UpdateUserVehicleNumberAsync(request.UserId, request.VehicleNumber);
            
            stopwatch.Stop();
            await LogAuditAsync("UpdateUserVehicleNumber", request, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            if (success)
            {
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Vehicle number updated successfully"
                });
            }
            else
            {
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to update vehicle number"
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("UpdateUserVehicleNumber", ex, request);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to update vehicle number"
            });
        }
    }

    /// <summary>
    /// Tests Fleetmatics API connectivity (Admin only)
    /// </summary>
    [HttpGet("test-connection")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<object>>> TestConnection()
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            // Test by getting an access token
            var token = await _fleetmaticsService.GetAccessTokenAsync();
            
            stopwatch.Stop();
            await LogAuditAsync("FleetmaticsConnectionTest", 
                new { hasToken = !string.IsNullOrEmpty(token) }, 
                stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Fleetmatics API connection successful",
                Data = new { ConnectionStatus = "OK", TokenReceived = !string.IsNullOrEmpty(token) }
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("FleetmaticsConnectionTest", ex);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = $"Fleetmatics API connection failed: {ex.Message}"
            });
        }
    }

    /// <summary>
    /// Gets information about the Fleetmatics background sync service (Admin only)
    /// </summary>
    [HttpGet("sync-service-status")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<object>>> GetSyncServiceStatus()
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            var syncHour = _configuration.GetValue<int>("Fleetmatics:SyncHour", 2);
            var now = DateTime.Now;
            var nextRun = DateTime.Today.AddHours(syncHour);
            
            // If we've already passed today's sync time, schedule for tomorrow
            if (now >= nextRun)
            {
                nextRun = nextRun.AddDays(1);
            }
            
            var timeUntilNext = nextRun - now;
            
            stopwatch.Stop();
            await LogAuditAsync("FleetmaticsSyncServiceStatus", 
                new { syncHour, nextRun, timeUntilNext }, 
                stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Sync service status retrieved successfully",
                Data = new 
                { 
                    ServiceStatus = "Running",
                    ScheduledSyncHour = syncHour,
                    NextScheduledRun = nextRun.ToString("yyyy-MM-dd HH:mm:ss"),
                    TimeUntilNextRun = $"{timeUntilNext.Days}d {timeUntilNext.Hours}h {timeUntilNext.Minutes}m",
                    CurrentTime = now.ToString("yyyy-MM-dd HH:mm:ss")
                }
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("FleetmaticsSyncServiceStatus", ex);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to retrieve sync service status"
            });
        }
    }

    /// <summary>
    /// DEBUG: Gets raw Fleetmatics API response for debugging JSON parsing issues
    /// </summary>
    [HttpGet("debug-raw-response/{employeeNumber}")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<object>>> GetRawFleetmaticsResponse(string employeeNumber)
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            if (string.IsNullOrWhiteSpace(employeeNumber))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Employee number is required"
                });
            }

            // Get token first
            var token = await _fleetmaticsService.GetAccessTokenAsync();
            var baseUrl = _configuration["Fleetmatics:BaseUrl"];
            var atmosphereAppId = _configuration["Fleetmatics:AtmosphereAppId"];

            // Make raw HTTP request
            using var httpClient = new HttpClient();
            // Fleetmatics requires special Atmosphere authorization format
            httpClient.DefaultRequestHeaders.Authorization = 
                new System.Net.Http.Headers.AuthenticationHeaderValue("Atmosphere", 
                    $"atmosphere_app_id={atmosphereAppId}, Bearer {token}");

            var url = $"{baseUrl}/da/v1/driverassignments/drivers/{Uri.EscapeDataString(employeeNumber)}/currentassignment";
            var response = await httpClient.GetAsync(url);
            var rawContent = await response.Content.ReadAsStringAsync();
            
            stopwatch.Stop();
            await LogAuditAsync("FleetmaticsRawResponseDebug", 
                new { employeeNumber, statusCode = response.StatusCode, content = rawContent }, 
                stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Raw Fleetmatics response retrieved",
                Data = new 
                { 
                    StatusCode = response.StatusCode.ToString(),
                    Headers = response.Headers.ToDictionary(h => h.Key, h => string.Join(", ", h.Value)),
                    ContentType = response.Content.Headers.ContentType?.ToString(),
                    RawContent = rawContent,
                    ContentLength = rawContent.Length,
                    StartsWithChar = rawContent.Length > 0 ? rawContent[0].ToString() : "EMPTY"
                }
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("FleetmaticsRawResponseDebug", ex, new { employeeNumber });
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to get raw Fleetmatics response"
            });
        }
    }
}
