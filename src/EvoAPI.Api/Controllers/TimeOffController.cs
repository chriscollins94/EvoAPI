using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Data;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("EvoApi/timeoff")]
public class TimeOffController : BaseController
{
    private readonly IDataService _dataService;
    private readonly IAuditService _auditService2;
    private readonly ILogger<TimeOffController> _logger;
    private readonly ITimeOffEmailService _timeOffEmailService;

    public TimeOffController(
        IDataService dataService,
        IAuditService auditService,
        ILogger<TimeOffController> logger,
        ITimeOffEmailService timeOffEmailService)
    {
        _dataService = dataService;
        _auditService2 = auditService;
        _logger = logger;
        _timeOffEmailService = timeOffEmailService;
        InitializeAuditService(auditService);
    }

    /// <summary>
    /// Get all time off request types (parent categories)
    /// </summary>
    [HttpGet("types")]
    public async Task<ActionResult<ApiResponse<List<TimeOffRequestTypeDto>>>> GetTimeOffRequestTypes()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var dt = await _dataService.GetTimeOffRequestTypesAsync();
            var types = dt.AsEnumerable().Select(row => new TimeOffRequestTypeDto
            {
                TortId = Convert.ToInt32(row["tort_id"]),
                TortType = row["tort_type"]?.ToString() ?? string.Empty
            }).ToList();

            stopwatch.Stop();
            await LogAuditAsync("GetTimeOffRequestTypes", new { count = types.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<List<TimeOffRequestTypeDto>>
            {
                Success = true,
                Message = "Request types retrieved successfully",
                Data = types,
                Count = types.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetTimeOffRequestTypes");
            await LogAuditErrorAsync("GetTimeOffRequestTypes", ex);

            return StatusCode(500, new ApiResponse<List<TimeOffRequestTypeDto>>
            {
                Success = false,
                Message = "Failed to retrieve request types"
            });
        }
    }

    /// <summary>
    /// Get type details (sub-types) for a given parent type
    /// </summary>
    [HttpGet("type-details/{tortId}")]
    public async Task<ActionResult<ApiResponse<List<TimeOffRequestTypeDetailDto>>>> GetTimeOffRequestTypeDetails(int tortId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var dt = await _dataService.GetTimeOffRequestTypeDetailsAsync(tortId);
            var details = dt.AsEnumerable().Select(row => new TimeOffRequestTypeDetailDto
            {
                TortdId = Convert.ToInt32(row["tortd_id"]),
                TortId = Convert.ToInt32(row["tort_id"]),
                TortdTypedetail = row["tortd_typedetail"]?.ToString() ?? string.Empty,
                TortdWorkflowrequired = Convert.ToBoolean(row["tortd_workflowrequired"]),
                TortdAdmincancreate = Convert.ToBoolean(row["tortd_admincancreate"]),
                TortdTechcancreate = row["tortd_techcancreate"] != DBNull.Value && Convert.ToBoolean(row["tortd_techcancreate"]),
                TortdMaxdaysoff = row["tortd_maxdaysoff"] != DBNull.Value ? Convert.ToInt32(row["tortd_maxdaysoff"]) : 30
            }).ToList();

            stopwatch.Stop();
            await LogAuditAsync("GetTimeOffRequestTypeDetails", new { tortId, count = details.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<List<TimeOffRequestTypeDetailDto>>
            {
                Success = true,
                Message = "Type details retrieved successfully",
                Data = details,
                Count = details.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetTimeOffRequestTypeDetails for tortId={TortId}", tortId);
            await LogAuditErrorAsync("GetTimeOffRequestTypeDetails", ex);

            return StatusCode(500, new ApiResponse<List<TimeOffRequestTypeDetailDto>>
            {
                Success = false,
                Message = "Failed to retrieve type details"
            });
        }
    }

    /// <summary>
    /// Get time off balance for a user
    /// </summary>
    [HttpGet("balance/{userId}")]
    public async Task<ActionResult<ApiResponse<TimeOffBalanceDto>>> GetTimeOffBalance(int userId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Non-admin users can only view their own balance
            if (!IsAdmin && userId != UserId)
            {
                return Forbid();
            }

            var dt = await _dataService.GetTimeOffBalanceAsync(userId);
            if (dt.Rows.Count == 0)
            {
                return NotFound(new ApiResponse<TimeOffBalanceDto>
                {
                    Success = false,
                    Message = "User not found"
                });
            }

            var row = dt.Rows[0];
            var balance = new TimeOffBalanceDto
            {
                UserId = Convert.ToInt32(row["u_id"]),
                Username = row["u_username"]?.ToString() ?? string.Empty,
                FirstName = row["u_firstname"]?.ToString() ?? string.Empty,
                LastName = row["u_lastname"]?.ToString() ?? string.Empty,
                Email = row["u_email"]?.ToString() ?? string.Empty,
                DaysAvailablePto = row["u_daysavailablepto"] != DBNull.Value ? Convert.ToInt32(row["u_daysavailablepto"]) : 0,
                DaysAvailableVacation = row["u_daysavailablevacation"] != DBNull.Value ? Convert.ToInt32(row["u_daysavailablevacation"]) : 0
            };

            stopwatch.Stop();
            await LogAuditAsync("GetTimeOffBalance", new { userId }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<TimeOffBalanceDto>
            {
                Success = true,
                Message = "Balance retrieved successfully",
                Data = balance,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetTimeOffBalance for userId={UserId}", userId);
            await LogAuditErrorAsync("GetTimeOffBalance", ex);

            return StatusCode(500, new ApiResponse<TimeOffBalanceDto>
            {
                Success = false,
                Message = "Failed to retrieve balance"
            });
        }
    }

    /// <summary>
    /// Get time off requests for a user (prior requests)
    /// </summary>
    [HttpGet("requests")]
    public async Task<ActionResult<ApiResponse<List<TimeOffRequestDto>>>> GetTimeOffRequests([FromQuery] int? userId = null)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Default to current user if no userId specified
            var targetUserId = userId ?? UserId;

            // Non-admin users can only view their own requests
            if (!IsAdmin && targetUserId != UserId)
            {
                return Forbid();
            }

            var dt = await _dataService.GetTimeOffRequestsAsync(targetUserId);
            var requests = dt.AsEnumerable().Select(row => new TimeOffRequestDto
            {
                TorId = Convert.ToInt32(row["tor_id"]),
                TorStartdate = Convert.ToDateTime(row["tor_startdate"]),
                TorEnddate = Convert.ToDateTime(row["tor_enddate"]),
                TortdTypedetail = row["tortd_typedetail"]?.ToString() ?? string.Empty,
                TortdId = Convert.ToInt32(row["tortd_id"]),
                TorsStatus = row["tors_status"]?.ToString() ?? string.Empty,
                TorsId = Convert.ToInt32(row["tors_id"]),
                UserId = Convert.ToInt32(row["u_id"]),
                FirstName = row["u_firstname"]?.ToString() ?? string.Empty,
                LastName = row["u_lastname"]?.ToString() ?? string.Empty,
                DaysAvailableVacation = row["u_daysavailablevacation"] != DBNull.Value ? Convert.ToInt32(row["u_daysavailablevacation"]) : 0,
                DaysAvailablePto = row["u_daysavailablepto"] != DBNull.Value ? Convert.ToInt32(row["u_daysavailablepto"]) : 0,
                TorTotalhours = row["tor_totalhours"] != DBNull.Value ? Convert.ToInt32(row["tor_totalhours"]) : 0,
                TorNote = row["tor_note"]?.ToString() ?? string.Empty,
                TorNotereason = row["tor_notereason"]?.ToString() ?? string.Empty,
                ZoneId = row["z_id"] != DBNull.Value ? Convert.ToInt32(row["z_id"]) : 0
            }).ToList();

            stopwatch.Stop();
            await LogAuditAsync("GetTimeOffRequests", new { targetUserId, count = requests.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<List<TimeOffRequestDto>>
            {
                Success = true,
                Message = "Requests retrieved successfully",
                Data = requests,
                Count = requests.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetTimeOffRequests");
            await LogAuditErrorAsync("GetTimeOffRequests", ex);

            return StatusCode(500, new ApiResponse<List<TimeOffRequestDto>>
            {
                Success = false,
                Message = "Failed to retrieve requests"
            });
        }
    }

    /// <summary>
    /// Submit a new time off request
    /// </summary>
    [HttpPost("requests")]
    public async Task<ActionResult<ApiResponse<TimeOffRequestDto>>> CreateTimeOffRequest([FromBody] CreateTimeOffRequestDto request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Non-admin users can only create for themselves
            if (!IsAdmin && request.UserId != UserId)
            {
                return Forbid();
            }

            // Set admin creator if creating for another user
            if (request.UserId != UserId)
            {
                request.UserIdAdmincreated = UserId;
            }

            // ====================================================
            // Status Routing Logic (matches legacy EvoWS behavior)
            // ====================================================
            int statusId;

            // Get the type detail info to check workflow requirements
            var typeDetails = await _dataService.GetTimeOffRequestTypeDetailsAsync(0);
            var typeDetailRow = typeDetails.AsEnumerable()
                .FirstOrDefault(r => Convert.ToInt32(r["tortd_id"]) == request.TortdId);
            bool workflowRequired = typeDetailRow != null && Convert.ToBoolean(typeDetailRow["tortd_workflowrequired"]);

            if (!workflowRequired)
            {
                // No workflow required → Auto-approve
                statusId = 1;
            }
            else
            {
                // Check if user is in auto-approved list (system admins)
                var autoApprovedList = await _dataService.GetConfigSettingValueAsync("Config", "UsersTimeOffAutoApproved");
                var autoApprovedIds = ParseCommaSeparatedIds(autoApprovedList);

                if (autoApprovedIds.Contains(request.UserId))
                {
                    // System admin → Auto-approve
                    statusId = 1;
                }
                else
                {
                    // Check if user is a tech
                    bool isTech = await _dataService.IsUserTechAsync(request.UserId);

                    // Check if user is a ZFM
                    var zfmList = await _dataService.GetConfigSettingValueAsync("Config", "UsersZFM");
                    var zfmIds = ParseCommaSeparatedIds(zfmList);
                    bool isZFM = zfmIds.Contains(request.UserId);

                    if (!isTech || isZFM)
                    {
                        // Non-tech or ZFM → Admin review (status 3)
                        statusId = 3;
                    }
                    else
                    {
                        // Tech → ZFM review (status 2)
                        statusId = 2;
                    }
                }
            }

            // Insert the main request
            var torId = await _dataService.InsertTimeOffRequestAsync(request, statusId);
            if (torId == null)
            {
                return StatusCode(500, new ApiResponse<TimeOffRequestDto>
                {
                    Success = false,
                    Message = "Failed to create time off request"
                });
            }

            // Insert the detail rows
            if (request.Details.Count > 0)
            {
                await _dataService.InsertTimeOffRequestDetailsAsync(torId.Value, request.Details);
            }

            // If auto-approved, create service requests/work orders
            if (statusId == 1)
            {
                await _dataService.InsertTimeOffRequestServiceRequestsAsync(torId.Value, request.UserId, request.TortdId);
            }

            // ====================================================
            // Email Notifications
            // ====================================================

            // Check for same-day request notification
            bool isSameDay = request.StartDate.Date <= DateTime.Today;
            if (isSameDay && statusId != 1)  // Don't send same-day for auto-approved
            {
                bool isTechForEmail = await _dataService.IsUserTechAsync(request.UserId);
                _ = Task.Run(async () =>
                {
                    try { await _timeOffEmailService.SendSameDayNotificationAsync(torId.Value, request.UserId, request.TortdId, isTechForEmail); }
                    catch (Exception ex) { _logger.LogError(ex, "Error sending same-day email for torId={TorId}", torId); }
                });
            }

            // Send routing-specific email
            if (statusId == 2)
            {
                // Tech request → notify ZFM
                _ = Task.Run(async () =>
                {
                    try { await _timeOffEmailService.SendZFMReviewNotificationAsync(torId.Value, request.UserId); }
                    catch (Exception ex) { _logger.LogError(ex, "Error sending ZFM review email for torId={TorId}", torId); }
                });
            }
            else if (statusId == 3)
            {
                // Non-tech/ZFM → notify admin approvers
                bool exceeding = false;
                string? balanceType = null;

                // Check if exceeding balance
                var balanceDt = await _dataService.GetTimeOffBalanceAsync(request.UserId);
                if (balanceDt.Rows.Count > 0)
                {
                    int totalHours = request.Details.Sum(d => d.EndHour - d.StartHour);
                    if (request.TortdId == 1) // Vacation
                    {
                        var available = balanceDt.Rows[0]["u_daysavailablevacation"] != DBNull.Value
                            ? Convert.ToInt32(balanceDt.Rows[0]["u_daysavailablevacation"]) : 0;
                        if (available < totalHours) { exceeding = true; balanceType = "vacation"; }
                    }
                    else if (request.TortdId == 2) // PTO
                    {
                        var available = balanceDt.Rows[0]["u_daysavailablepto"] != DBNull.Value
                            ? Convert.ToInt32(balanceDt.Rows[0]["u_daysavailablepto"]) : 0;
                        if (available < totalHours) { exceeding = true; balanceType = "PTO"; }
                    }
                }

                _ = Task.Run(async () =>
                {
                    try { await _timeOffEmailService.SendAdminEscalationNotificationAsync(torId.Value, request.UserId, exceeding, balanceType); }
                    catch (Exception ex) { _logger.LogError(ex, "Error sending admin escalation email for torId={TorId}", torId); }
                });
            }

            stopwatch.Stop();
            await LogAuditAsync("CreateTimeOffRequest", new { torId, userId = request.UserId, tortdId = request.TortdId, statusId }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<TimeOffRequestDto>
            {
                Success = true,
                Message = "Time off request submitted successfully",
                Data = new TimeOffRequestDto { TorId = torId.Value },
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in CreateTimeOffRequest");
            await LogAuditErrorAsync("CreateTimeOffRequest", ex);

            return StatusCode(500, new ApiResponse<TimeOffRequestDto>
            {
                Success = false,
                Message = "Failed to create time off request"
            });
        }
    }

    /// <summary>
    /// Parse comma-separated user ID string into a HashSet
    /// </summary>
    private static HashSet<int> ParseCommaSeparatedIds(string? commaSeparatedList)
    {
        var result = new HashSet<int>();
        if (string.IsNullOrWhiteSpace(commaSeparatedList)) return result;

        foreach (var part in commaSeparatedList.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            if (int.TryParse(part.Trim(), out var id))
            {
                result.Add(id);
            }
        }
        return result;
    }

    /// <summary>
    /// Get active employees for admin creator dropdown
    /// </summary>
    [HttpPut("requests/{torId}/cancel")]
    public async Task<ActionResult<ApiResponse<object>>> CancelRequest(int torId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            await _dataService.CancelTimeOffRequestAsync(torId, UserId);

            stopwatch.Stop();
            await LogAuditAsync("CancelTimeOffRequest", new { torId, userId = UserId }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Time off request cancelled successfully"
            });
        }
        catch (UnauthorizedAccessException ex)
        {
            stopwatch.Stop();
            return StatusCode(403, new ApiResponse<object>
            {
                Success = false,
                Message = ex.Message
            });
        }
        catch (InvalidOperationException ex)
        {
            stopwatch.Stop();
            return BadRequest(new ApiResponse<object>
            {
                Success = false,
                Message = ex.Message
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error cancelling time off request {TorId}", torId);
            await LogAuditErrorAsync("CancelTimeOffRequest", ex);

            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to cancel time off request"
            });
        }
    }

    [HttpGet("active-employees")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<List<TimeOffActiveEmployeeDto>>>> GetActiveEmployees()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var dt = await _dataService.GetActiveEmployeesForTimeOffAsync();
            var employees = dt.AsEnumerable().Select(row => new TimeOffActiveEmployeeDto
            {
                UserId = Convert.ToInt32(row["u_id"]),
                FirstName = row["u_firstname"]?.ToString() ?? string.Empty,
                LastName = row["u_lastname"]?.ToString() ?? string.Empty,
                Username = row["u_username"]?.ToString() ?? string.Empty
            }).ToList();

            stopwatch.Stop();
            await LogAuditAsync("GetActiveEmployeesForTimeOff", new { count = employees.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<List<TimeOffActiveEmployeeDto>>
            {
                Success = true,
                Message = "Active employees retrieved successfully",
                Data = employees,
                Count = employees.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetActiveEmployees");
            await LogAuditErrorAsync("GetActiveEmployeesForTimeOff", ex);

            return StatusCode(500, new ApiResponse<List<TimeOffActiveEmployeeDto>>
            {
                Success = false,
                Message = "Failed to retrieve active employees"
            });
        }
    }

    // ========================================
    // Phase 2: Request Management (Approver) Endpoints
    // ========================================

    /// <summary>
    /// Get all time off requests for approver view (Admin or ZFM)
    /// </summary>
    [HttpGet("all-requests")]
    public async Task<ActionResult<ApiResponse<List<TimeOffRequestDto>>>> GetAllTimeOffRequests()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var dt = await _dataService.GetAllTimeOffRequestsAsync();
            var requests = dt.AsEnumerable().Select(row => new TimeOffRequestDto
            {
                TorId = Convert.ToInt32(row["tor_id"]),
                TorStartdate = Convert.ToDateTime(row["tor_startdate"]),
                TorEnddate = Convert.ToDateTime(row["tor_enddate"]),
                TortdTypedetail = row["tortd_typedetail"]?.ToString() ?? string.Empty,
                TortdId = Convert.ToInt32(row["tortd_id"]),
                TorsStatus = row["tors_status"]?.ToString() ?? string.Empty,
                TorsId = Convert.ToInt32(row["tors_id"]),
                UserId = Convert.ToInt32(row["u_id"]),
                FirstName = row["u_firstname"]?.ToString() ?? string.Empty,
                LastName = row["u_lastname"]?.ToString() ?? string.Empty,
                DaysAvailableVacation = row["u_daysavailablevacation"] != DBNull.Value ? Convert.ToInt32(row["u_daysavailablevacation"]) : 0,
                DaysAvailablePto = row["u_daysavailablepto"] != DBNull.Value ? Convert.ToInt32(row["u_daysavailablepto"]) : 0,
                TorTotalhours = row["tor_totalhours"] != DBNull.Value ? Convert.ToInt32(row["tor_totalhours"]) : 0,
                TorNote = row["tor_note"]?.ToString() ?? string.Empty,
                TorNotereason = row["tor_notereason"]?.ToString() ?? string.Empty,
                ZoneId = row["z_id"] != DBNull.Value ? Convert.ToInt32(row["z_id"]) : 0,
                TorInsertdatetime = row["tor_insertdatetime"] != DBNull.Value ? Convert.ToDateTime(row["tor_insertdatetime"]) : DateTime.MinValue,
                IsTech = row["is_tech"] != DBNull.Value && Convert.ToInt32(row["is_tech"]) == 1
            }).ToList();

            stopwatch.Stop();
            await LogAuditAsync("GetAllTimeOffRequests", new { count = requests.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<List<TimeOffRequestDto>>
            {
                Success = true,
                Message = "All requests retrieved successfully",
                Data = requests,
                Count = requests.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetAllTimeOffRequests");
            await LogAuditErrorAsync("GetAllTimeOffRequests", ex);

            return StatusCode(500, new ApiResponse<List<TimeOffRequestDto>>
            {
                Success = false,
                Message = "Failed to retrieve requests"
            });
        }
    }

    /// <summary>
    /// Get detail rows for a specific time off request
    /// </summary>
    [HttpGet("requests/{torId}/details")]
    public async Task<ActionResult<ApiResponse<List<TimeOffRequestDetailDto>>>> GetTimeOffRequestDetails(int torId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var dt = await _dataService.GetTimeOffRequestDetailAsync(torId);
            var details = dt.AsEnumerable().Select(row => new TimeOffRequestDetailDto
            {
                TordId = Convert.ToInt32(row["tord_id"]),
                TorId = Convert.ToInt32(row["tor_id"]),
                TordDate = Convert.ToDateTime(row["tord_date"]),
                TordStarthour = Convert.ToInt32(row["tord_starthour"]),
                TordEndhour = Convert.ToInt32(row["tord_endhour"])
            }).ToList();

            stopwatch.Stop();
            return Ok(new ApiResponse<List<TimeOffRequestDetailDto>>
            {
                Success = true,
                Message = "Request details retrieved successfully",
                Data = details,
                Count = details.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetTimeOffRequestDetails for torId={TorId}", torId);
            await LogAuditErrorAsync("GetTimeOffRequestDetails", ex);

            return StatusCode(500, new ApiResponse<List<TimeOffRequestDetailDto>>
            {
                Success = false,
                Message = "Failed to retrieve request details"
            });
        }
    }

    /// <summary>
    /// Approve a time off request (sets status to Approved + creates SR/WOs)
    /// </summary>
    [HttpPut("requests/{torId}/approve")]
    public async Task<ActionResult<ApiResponse<object>>> ApproveRequest(int torId, [FromBody] UpdateTimeOffRequestDto request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Get the request info first (needed for email and SR/WO creation)
            var allRequests = await _dataService.GetAllTimeOffRequestsAsync();
            var requestRow = allRequests.AsEnumerable().FirstOrDefault(r => Convert.ToInt32(r["tor_id"]) == torId);
            if (requestRow == null)
            {
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Time off request not found"
                });
            }

            int requestUserId = Convert.ToInt32(requestRow["u_id"]);
            int tortdId = Convert.ToInt32(requestRow["tortd_id"]);

            // Check if this is a ZFM reviewing (status 2) and needs admin escalation
            bool isZFMReview = await _dataService.IsTimeOffWorkflowCurrentlyZFMReviewAsync(torId);

            if (isZFMReview)
            {
                // Check if balance is exceeded
                bool exceedingBalance = await _dataService.IsTimeOffExceedingBalanceAsync(torId);
                string? balanceType = null;
                if (exceedingBalance)
                {
                    balanceType = await _dataService.GetTimeOffBalanceTypeAsync(torId);
                }

                // Check admin escalation needed (exceeding balance OR short-notice threshold)
                bool adminRequired = await _dataService.IsTimeOffWorkflowAdminRequiredAsync(torId);

                if (exceedingBalance || adminRequired)
                {
                    // Escalate to admin review (status 3)
                    await _dataService.UpdateTimeOffRequestStatusAsync(torId, 3, request.TorNotereason ?? "");
                    stopwatch.Stop();
                    await LogAuditAsync("ApproveRequest_EscalatedToAdmin", new { torId, userId = UserId, exceedingBalance, balanceType }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                    // Send admin escalation email
                    _ = Task.Run(async () =>
                    {
                        try { await _timeOffEmailService.SendAdminEscalationNotificationAsync(torId, requestUserId, exceedingBalance, balanceType); }
                        catch (Exception ex) { _logger.LogError(ex, "Error sending admin escalation email for torId={TorId}", torId); }
                    });

                    return Ok(new ApiResponse<object>
                    {
                        Success = true,
                        Message = "Request escalated to admin review"
                    });
                }
            }

            // Create service request and work orders
            await _dataService.InsertTimeOffRequestServiceRequestsAsync(torId, requestUserId, tortdId);

            // Set status to Approved (1)
            await _dataService.UpdateTimeOffRequestStatusAsync(torId, 1, request.TorNotereason ?? "");

            stopwatch.Stop();
            await LogAuditAsync("ApproveRequest", new { torId, userId = UserId, requestUserId }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            // Send approval notification email to the employee
            _ = Task.Run(async () =>
            {
                try { await _timeOffEmailService.SendApprovalNotificationAsync(torId, requestUserId); }
                catch (Exception ex) { _logger.LogError(ex, "Error sending approval email for torId={TorId}", torId); }
            });

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Request approved successfully"
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error approving time off request {TorId}", torId);
            await LogAuditErrorAsync("ApproveRequest", ex);

            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to approve request"
            });
        }
    }

    /// <summary>
    /// Reject a time off request
    /// </summary>
    [HttpPut("requests/{torId}/reject")]
    public async Task<ActionResult<ApiResponse<object>>> RejectRequest(int torId, [FromBody] UpdateTimeOffRequestDto request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // Get the request info for the email notification
            var allRequests = await _dataService.GetAllTimeOffRequestsAsync();
            var requestRow = allRequests.AsEnumerable().FirstOrDefault(r => Convert.ToInt32(r["tor_id"]) == torId);
            int requestUserId = 0;
            if (requestRow != null)
            {
                requestUserId = Convert.ToInt32(requestRow["u_id"]);
            }

            // Status 4 = Rejected
            await _dataService.UpdateTimeOffRequestStatusAsync(torId, 4, request.TorNotereason ?? "");

            stopwatch.Stop();
            await LogAuditAsync("RejectRequest", new { torId, userId = UserId, reason = request.TorNotereason }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            // Send rejection notification email to the employee
            if (requestUserId > 0)
            {
                _ = Task.Run(async () =>
                {
                    try { await _timeOffEmailService.SendRejectionNotificationAsync(torId, requestUserId, request.TorNotereason ?? ""); }
                    catch (Exception ex) { _logger.LogError(ex, "Error sending rejection email for torId={TorId}", torId); }
                });
            }

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Request rejected"
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error rejecting time off request {TorId}", torId);
            await LogAuditErrorAsync("RejectRequest", ex);

            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to reject request"
            });
        }
    }

    /// <summary>
    /// Hard delete a time off request (Admin - Time Off Delete permission)
    /// </summary>
    [HttpDelete("requests/{torId}")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<object>>> DeleteRequest(int torId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            await _dataService.DeleteTimeOffRequestAsync(torId);

            stopwatch.Stop();
            await LogAuditAsync("DeleteTimeOffRequest", new { torId, userId = UserId }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Request deleted successfully"
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error deleting time off request {TorId}", torId);
            await LogAuditErrorAsync("DeleteTimeOffRequest", ex);

            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to delete request"
            });
        }
    }

    /// <summary>
    /// Get all zones for filtering
    /// </summary>
    [HttpGet("zones")]
    public async Task<ActionResult<ApiResponse<List<ZoneDto>>>> GetZones()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var dt = await _dataService.GetZonesAsync();
            var zones = dt.AsEnumerable().Select(row => new ZoneDto
            {
                ZoneId = Convert.ToInt32(row["z_id"]),
                ZoneNumber = row["z_number"]?.ToString() ?? string.Empty,
                ZfmUserId = row["u_id"] != DBNull.Value ? Convert.ToInt32(row["u_id"]) : 0
            }).ToList();

            stopwatch.Stop();
            return Ok(new ApiResponse<List<ZoneDto>>
            {
                Success = true,
                Message = "Zones retrieved successfully",
                Data = zones,
                Count = zones.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetZones");
            await LogAuditErrorAsync("GetZones", ex);

            return StatusCode(500, new ApiResponse<List<ZoneDto>>
            {
                Success = false,
                Message = "Failed to retrieve zones"
            });
        }
    }

    /// <summary>
    /// Get approved time off events for calendar display
    /// </summary>
    [HttpGet("calendar-events")]
    public async Task<ActionResult<ApiResponse<List<TimeOffCalendarEventDto>>>> GetCalendarEvents()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var dt = await _dataService.GetCalendarEventsAsync();
            var events = dt.AsEnumerable().Select(row =>
            {
                var tordDate = Convert.ToDateTime(row["tord_date"]).ToString("yyyy-MM-dd");
                var startHour = Convert.ToInt32(row["tord_starthour"]);
                var endHour = Convert.ToInt32(row["tord_endhour"]);
                var firstName = row["u_firstname"]?.ToString() ?? "";
                var lastName = row["u_lastname"]?.ToString() ?? "";
                var typeDetail = row["tortd_typedetail"]?.ToString() ?? "";

                return new TimeOffCalendarEventDto
                {
                    Title = $"{firstName} {lastName} - {typeDetail}",
                    Start = $"{tordDate}T{startHour:D2}:00:00",
                    End = $"{tordDate}T{endHour:D2}:00:00",
                    UserId = Convert.ToInt32(row["u_id"]),
                    ZoneId = row["z_id"] != DBNull.Value ? Convert.ToInt32(row["z_id"]) : 0,
                    TypeDetail = typeDetail,
                    IsAdmin = row["is_admin"] != DBNull.Value && Convert.ToInt32(row["is_admin"]) == 1
                };
            }).ToList();

            stopwatch.Stop();
            await LogAuditAsync("GetCalendarEvents", new { count = events.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<List<TimeOffCalendarEventDto>>
            {
                Success = true,
                Message = "Calendar events retrieved successfully",
                Data = events,
                Count = events.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetCalendarEvents");
            await LogAuditErrorAsync("GetCalendarEvents", ex);

            return StatusCode(500, new ApiResponse<List<TimeOffCalendarEventDto>>
            {
                Success = false,
                Message = "Failed to retrieve calendar events"
            });
        }
    }
}
