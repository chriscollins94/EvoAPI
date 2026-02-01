using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class TimeClockController : BaseController
{
    private readonly ITimeTrackingService _timeTrackingService;
    private readonly ILogger<TimeClockController> _logger;

    public TimeClockController(
        IAuditService auditService,
        ITimeTrackingService timeTrackingService,
        ILogger<TimeClockController> logger)
    {
        InitializeAuditService(auditService);
        _timeTrackingService = timeTrackingService;
        _logger = logger;
    }

    [HttpPost("clockin")]
    [Authorize]
    public async Task<ActionResult<ApiResponse<object>>> ClockIn([FromBody] TimeClockRequest request)
    {
        try
        {
            _ = LogAuditAsync("Clock In", $"User: {Username}");

            var ttId = await _timeTrackingService.ClockInAsync(UserId, request.Latitude, request.Longitude);

            if (ttId > 0)
            {
                _ = LogAuditAsync("Clock In Success", $"User: {Username}, TimeTrackingId: {ttId}");
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Clocked in successfully",
                    Data = new { TimeTrackingId = ttId },
                    Count = 1,
                    Timestamp = DateTime.UtcNow
                });
            }

            return BadRequest(new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to clock in",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Clock in error for user: {Username}", Username);
            _ = LogAuditErrorAsync("Clock In Error", ex);

            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred during clock in",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpPost("clockout")]
    [Authorize]
    public async Task<ActionResult<ApiResponse<object>>> ClockOut([FromBody] TimeClockRequest request)
    {
        try
        {
            _ = LogAuditAsync("Clock Out", $"User: {Username}");

            var success = await _timeTrackingService.ClockOutAsync(UserId, request.Latitude, request.Longitude);

            if (success)
            {
                _ = LogAuditAsync("Clock Out Success", $"User: {Username}");
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Clocked out successfully",
                    Data = null,
                    Count = 1,
                    Timestamp = DateTime.UtcNow
                });
            }

            return BadRequest(new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to clock out - no active clock-in found",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Clock out error for user: {Username}", Username);
            _ = LogAuditErrorAsync("Clock Out Error", ex);

            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred during clock out",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpPost("startbreak")]
    [Authorize]
    public async Task<ActionResult<ApiResponse<object>>> StartBreak([FromBody] TimeClockRequest request)
    {
        try
        {
            _ = LogAuditAsync("Start Break", $"User: {Username}");

            var ttId = await _timeTrackingService.StartBreakAsync(UserId, request.Latitude, request.Longitude);

            if (ttId > 0)
            {
                _ = LogAuditAsync("Start Break Success", $"User: {Username}, TimeTrackingId: {ttId}");
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Break started successfully",
                    Data = new { TimeTrackingId = ttId },
                    Count = 1,
                    Timestamp = DateTime.UtcNow
                });
            }

            return BadRequest(new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to start break",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Start break error for user: {Username}", Username);
            _ = LogAuditErrorAsync("Start Break Error", ex);

            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred during start break",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
    }

    [HttpPost("endbreak")]
    [Authorize]
    public async Task<ActionResult<ApiResponse<object>>> EndBreak([FromBody] TimeClockRequest request)
    {
        try
        {
            _ = LogAuditAsync("End Break", $"User: {Username}");

            var success = await _timeTrackingService.EndBreakAsync(UserId, request.Latitude, request.Longitude);

            if (success)
            {
                _ = LogAuditAsync("End Break Success", $"User: {Username}");
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Break ended successfully",
                    Data = null,
                    Count = 1,
                    Timestamp = DateTime.UtcNow
                });
            }

            return BadRequest(new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to end break - no active break found",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "End break error for user: {Username}", Username);
            _ = LogAuditErrorAsync("End Break Error", ex);

            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred during end break",
                Data = null,
                Count = 0,
                Timestamp = DateTime.UtcNow
            });
        }
    }
}

public class TimeClockRequest
{
    public decimal Latitude { get; set; }
    public decimal Longitude { get; set; }
}
