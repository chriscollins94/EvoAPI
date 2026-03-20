using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text.Json;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("EvoApi")]
[Authorize]
public class EvoApiController : BaseController
{
    #region Initialize

    private readonly IDataService _dataService;
        private readonly ILogger<EvoApiController> _logger;
        private readonly HttpClient _httpClient;
        private readonly IConfiguration _configuration;
        private readonly IAuditCriticalService _auditCriticalService;
    
        public EvoApiController(
            IDataService dataService, 
            IAuditService auditService,
            IAuditCriticalService auditCriticalService,
            ILogger<EvoApiController> logger,
            HttpClient httpClient,
            IConfiguration configuration)
        {
            _dataService = dataService;
            _logger = logger;
            _httpClient = httpClient;
            _configuration = configuration;
            _auditCriticalService = auditCriticalService;
            InitializeAuditService(auditService);
        }
        
        /// <summary>
        /// Set user context on the audit critical service for proper logging
        /// </summary>
        private void SetAuditCriticalUserContext()
        {
            _auditCriticalService.Username = Username;
            _auditCriticalService.UserFullName = UserFullName;
            _auditCriticalService.IPAddress = ClientIPAddress;
            _auditCriticalService.UserAgent = UserAgent;
        }

        private static bool IsValidEmail(string email)
        {
            try
            {
                var addr = new System.Net.Mail.MailAddress(email);
                return addr.Address == email;
            }
            catch
            {
                return false;
            }
        }
    #endregion

    #region Get
        [HttpGet("workorders")]
        public async Task<ActionResult<ApiResponse<List<WorkOrderDto>>>> GetWorkOrders([FromQuery] int numberOfDays = 30)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting work orders for {NumberOfDays} days", numberOfDays);
                
                // Validate input
                if (numberOfDays <= 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "NumberOfDays must be greater than 0",
                        Count = 0
                    });
                }
    
                // Get data from service
                var dataTable = await _dataService.GetWorkOrdersAsync(numberOfDays);
                var workOrders = ConvertDataTableToWorkOrders(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetWorkOrders", $"Retrieved {workOrders.Count} work orders for {numberOfDays} days", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<WorkOrderDto>>
                {
                    Success = true,
                    Message = "Work orders retrieved successfully",
                    Data = workOrders,
                    Count = workOrders.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetWorkOrders", ex);
                
                _logger.LogError(ex, "Error retrieving work orders for {NumberOfDays} days", numberOfDays);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving work orders",
                    Count = 0
                });
            }
        }

    [HttpGet("workorders/schedule")]
    public async Task<ActionResult<ApiResponse<List<WorkOrderDto>>>> GetWorkOrdersSchedule([FromQuery] int numberOfDays = 30, [FromQuery] int? technicianId = null)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            // _logger.LogInformation("Getting work orders schedule for {NumberOfDays} days", numberOfDays);

            // Validate input
            if (numberOfDays <= 0)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "NumberOfDays must be greater than 0",
                    Count = 0
                });
            }

            // Get data from service
            var dataTable = await _dataService.GetWorkOrdersScheduleAsync(numberOfDays, technicianId);
            var workOrders = ConvertDataTableToWorkOrders(dataTable);

            stopwatch.Stop();

            // Log successful operation
            var technicianInfo = technicianId.HasValue ? $" for technician {technicianId.Value}" : " for all technicians";
            await LogOperationAsync("GetWorkOrdersSchedule", $"Retrieved {workOrders.Count} scheduled work orders for {numberOfDays} days{technicianInfo}", stopwatch.Elapsed);

            return Ok(new ApiResponse<List<WorkOrderDto>>
            {
                Success = true,
                Message = "Work orders schedule retrieved successfully",
                Data = workOrders,
                Count = workOrders.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetWorkOrdersSchedule", ex);
            
            _logger.LogError(ex, "Error retrieving work orders schedule for {NumberOfDays} days", numberOfDays);            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while retrieving work orders schedule",
                Count = 0
            });
        }
    }

        [HttpGet("priorities")]
        public async Task<ActionResult<ApiResponse<List<PriorityDto>>>> GetPriorities()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting all priorities");
                
                // Get data from service
                var dataTable = await _dataService.GetAllPrioritiesAsync();
                var priorities = ConvertDataTableToPriorities(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetPriorities", $"Retrieved {priorities.Count} priorities", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<PriorityDto>>
                {
                    Success = true,
                    Message = "Priorities retrieved successfully",
                    Data = priorities,
                    Count = priorities.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetPriorities", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving priorities");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving priorities",
                    Count = 0
                });
            }
        }

        [HttpGet("statussecondaries")]
        public async Task<ActionResult<ApiResponse<List<StatusSecondaryDto>>>> GetStatusSecondaries()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting all status secondaries");
                
                // Get data from service
                var dataTable = await _dataService.GetAllStatusSecondariesAsync();
                var statusSecondaries = ConvertDataTableToStatusSecondaries(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetStatusSecondaries", $"Retrieved {statusSecondaries.Count} status secondaries", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<StatusSecondaryDto>>
                {
                    Success = true,
                    Message = "Status secondaries retrieved successfully",
                    Data = statusSecondaries,
                    Count = statusSecondaries.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetStatusSecondaries", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving status secondaries");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving status secondaries",
                    Count = 0
                });
            }
        }

        [HttpGet("callcenters")]
        public async Task<ActionResult<ApiResponse<List<CallCenterDto>>>> GetCallCenters()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting all call centers");
                
                // Get data from service
                var dataTable = await _dataService.GetAllCallCentersAsync();
                var callCenters = ConvertDataTableToCallCenters(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetCallCenters", $"Retrieved {callCenters.Count} call centers", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<CallCenterDto>>
                {
                    Success = true,
                    Message = "Call centers retrieved successfully",
                    Data = callCenters,
                    Count = callCenters.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetCallCenters", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving call centers");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving call centers",
                    Count = 0
                });
            }
        }

        [HttpGet("attackpointnotes")]
        public async Task<ActionResult<ApiResponse<List<AttackPointNoteDto>>>> GetAttackPointNotes()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting all attack point notes");
                
                // Get data from service
                var dataTable = await _dataService.GetAllAttackPointNotesAsync();
                var attackPointNotes = ConvertDataTableToAttackPointNotes(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetAttackPointNotes", $"Retrieved {attackPointNotes.Count} attack point notes", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<AttackPointNoteDto>>
                {
                    Success = true,
                    Message = "Attack point notes retrieved successfully",
                    Data = attackPointNotes,
                    Count = attackPointNotes.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetAttackPointNotes", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving attack point notes");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving attack point notes",
                    Count = 0
                });
            }
        }

        [HttpGet("configsettings/{identifier}")]
        public async Task<ActionResult<ApiResponse<ConfigSettingDto>>> GetConfigSetting(string identifier)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting config setting: {Identifier}", identifier);
                
                // Get data from service
                var configSetting = await _dataService.GetConfigSettingAsync(identifier);
    
                stopwatch.Stop();
                
                if (configSetting == null)
                {
                    await LogOperationAsync("GetConfigSetting", $"Config setting not found: {identifier}", stopwatch.Elapsed);
                    
                    return NotFound(new ApiResponse<ConfigSettingDto>
                    {
                        Success = false,
                        Message = $"Config setting '{identifier}' not found",
                        Count = 0
                    });
                }
                
                // Log successful operation
                await LogOperationAsync("GetConfigSetting", $"Retrieved config setting: {identifier}", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<ConfigSettingDto>
                {
                    Success = true,
                    Message = "Config setting retrieved successfully",
                    Data = configSetting,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetConfigSetting", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving config setting: {Identifier}", identifier);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving config setting",
                    Count = 0
                });
            }
        }

        [HttpGet("attackpointstatus")]
        public async Task<ActionResult<ApiResponse<List<AttackPointStatusDto>>>> GetAttackPointStatus()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting all attack point status records");
                
                // Get data from service
                var dataTable = await _dataService.GetAllAttackPointStatusAsync();
                var attackPointStatus = ConvertDataTableToAttackPointStatus(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetAttackPointStatus", $"Retrieved {attackPointStatus.Count} attack point status records", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<AttackPointStatusDto>>
                {
                    Success = true,
                    Message = "Attack point status records retrieved successfully",
                    Data = attackPointStatus,
                    Count = attackPointStatus.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetAttackPointStatus", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving attack point status records");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving attack point status records",
                    Count = 0
                });
            }
        }

        [HttpGet("zones/legacy")]
        public async Task<ActionResult<ApiResponse<List<ZoneDto>>>> GetZonesLegacy()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting zones (legacy endpoint)");
                
                var dataTable = await _dataService.GetAllZonesAsync();
                var zones = ConvertDataTableToZones(dataTable);
                
                stopwatch.Stop();
                await LogOperationAsync("GetZonesLegacy", $"Retrieved {zones.Count} zones", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<List<ZoneDto>>
                {
                    Success = true,
                    Message = $"Retrieved {zones.Count} zones",
                    Data = zones,
                    Count = zones.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetZones", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving zones");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving zones",
                    Count = 0
                });
            }
        }

        [HttpGet("users/management")]
        [UserAdminOnly]
        public async Task<ActionResult<ApiResponse<List<UserDto>>>> GetUsersForManagement()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting all users for management");
                
                var dataTable = await _dataService.GetAllUsersForManagementAsync();
                var users = ConvertDataTableToUsers(dataTable);
                
                stopwatch.Stop();
                await LogOperationAsync("GetUsersForManagement", $"Retrieved {users.Count} users for management", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<List<UserDto>>
                {
                    Success = true,
                    Message = $"Retrieved {users.Count} users for management",
                    Data = users,
                    Count = users.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetUsersForManagement", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving users for management");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving users for management",
                    Count = 0
                });
            }
        }

        [HttpGet("users/{id:int}")]
        [UserAdminOnly]
        public async Task<ActionResult<ApiResponse<UserDto>>> GetUserById(int id)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting user by ID: {UserId}", id);
                
                if (id <= 0)
                {
                    return BadRequest(new ApiResponse<UserDto>
                    {
                        Success = false,
                        Message = "Valid user ID is required",
                        Count = 0
                    });
                }
                
                var dataTable = await _dataService.GetUserByIdAsync(id);
                
                if (dataTable.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<UserDto>
                    {
                        Success = false,
                        Message = "User not found",
                        Count = 0
                    });
                }
                
                var users = ConvertDataTableToUsers(dataTable);
                var user = users.First();
                
                stopwatch.Stop();
                await LogOperationAsync("GetUserById", $"Retrieved user {id} - {user.Username}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<UserDto>
                {
                    Success = true,
                    Message = "User retrieved successfully",
                    Data = user,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetUserById", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving user {UserId}", id);
                
                return StatusCode(500, new ApiResponse<UserDto>
                {
                    Success = false,
                    Message = "An error occurred while retrieving the user",
                    Count = 0
                });
            }
        }

        [HttpGet("users")]
        public async Task<ActionResult<ApiResponse<List<UserDto>>>> GetUsers()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting active users");
                
                var dataTable = await _dataService.GetAllUsersAsync();
                var users = ConvertDataTableToUsers(dataTable);
                
                stopwatch.Stop();
                await LogOperationAsync("GetUsers", $"Retrieved {users.Count} active users", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<List<UserDto>>
                {
                    Success = true,
                    Message = $"Retrieved {users.Count} active users",
                    Data = users,
                    Count = users.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetUsers", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving users");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving users",
                    Count = 0
                });
            }
        }

        [HttpGet("users/current/dashboard-note")]
        public async Task<ActionResult<ApiResponse<string>>> GetCurrentUserDashboardNote()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting current user dashboard note for user {UserId}", UserId);
                
                var dataTable = await _dataService.GetUserByIdAsync(UserId);
                
                if (dataTable.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<string>
                    {
                        Success = false,
                        Message = "User not found",
                        Data = null,
                        Count = 0
                    });
                }
                
                var row = dataTable.Rows[0];
                var dashboardNote = row["NoteDashboard"]?.ToString() ?? string.Empty;
                
                stopwatch.Stop();
                // await LogOperationAsync("GetCurrentUserDashboardNote", $"Retrieved dashboard note for user {UserId}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<string>
                {
                    Success = true,
                    Message = "Dashboard note retrieved successfully",
                    Data = dashboardNote,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetCurrentUserDashboardNote", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving current user dashboard note");
                
                return StatusCode(500, new ApiResponse<string>
                {
                    Success = false,
                    Message = "An error occurred while retrieving dashboard note",
                    Data = null,
                    Count = 0
                });
            }
        }

        [HttpGet("users/current/technician-profile")]
        public async Task<ActionResult<ApiResponse<object>>> GetCurrentUserTechnicianProfile()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting current user technician profile for user {UserId}", UserId);
                
                var dataTable = await _dataService.GetUserByIdAsync(UserId);
                
                if (dataTable.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "User not found",
                        Data = null,
                        Count = 0
                    });
                }

                var userRow = dataTable.Rows[0];
                
                // Build full address from separate components
                var address1 = userRow["Address1"]?.ToString() ?? "";
                var address2 = userRow["Address2"]?.ToString() ?? "";
                var city = userRow["City"]?.ToString() ?? "";
                var state = userRow["State"]?.ToString() ?? "";
                var zip = userRow["Zip"]?.ToString() ?? "";
                
                var fullAddress = "";
                if (!string.IsNullOrEmpty(address1))
                {
                    fullAddress = address1;
                    if (!string.IsNullOrEmpty(address2))
                        fullAddress += ", " + address2;
                    if (!string.IsNullOrEmpty(city))
                        fullAddress += ", " + city;
                    if (!string.IsNullOrEmpty(state))
                        fullAddress += ", " + state;
                    if (!string.IsNullOrEmpty(zip))
                        fullAddress += " " + zip;
                }
                
                var technicianProfile = new
                {
                    u_id = ConvertToInt(userRow["Id"]),
                    id = ConvertToInt(userRow["Id"]),
                    u_firstname = userRow["FirstName"]?.ToString() ?? "",
                    u_lastname = userRow["LastName"]?.ToString() ?? "",
                    u_username = userRow["Username"]?.ToString() ?? "",
                    u_fulladdress = fullAddress,
                    fullAddress = fullAddress,
                    address = fullAddress,
                    u_email = userRow["Email"]?.ToString() ?? "",
                    u_phone = userRow["PhoneMobile"]?.ToString() ?? "",
                    employeeNumber = userRow["EmployeeNumber"]?.ToString() ?? ""
                };
                
                stopwatch.Stop();
                await LogAuditAsync("GetCurrentUserTechnicianProfile", 
                    $"Retrieved technician profile for user {UserId}", 
                    stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Current user technician profile retrieved successfully",
                    Data = technicianProfile,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetCurrentUserTechnicianProfile", ex);
                
                _logger.LogError(ex, "Error retrieving current user technician profile");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving technician profile",
                    Data = null,
                    Count = 0
                });
            }
        }

        [HttpGet("users/{userId}/dashboard-note")]
        public async Task<ActionResult<ApiResponse<string>>> GetUserDashboardNote(int userId)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting dashboard note for user {UserId} by user {CurrentUserId}", userId, UserId);
                
                var dataTable = await _dataService.GetUserByIdAsync(userId);
                
                if (dataTable.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<string>
                    {
                        Success = false,
                        Message = "User not found",
                        Data = null,
                        Count = 0
                    });
                }
                
                var row = dataTable.Rows[0];
                var dashboardNote = row["NoteDashboard"]?.ToString() ?? string.Empty;
                
                stopwatch.Stop();
                // await LogAuditAsync("GetUserDashboardNote", $"Retrieved dashboard note for user {userId}", stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
                
                return Ok(new ApiResponse<string>
                {
                    Success = true,
                    Message = "Dashboard note retrieved successfully",
                    Data = dashboardNote,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetUserDashboardNote", ex);
                
                _logger.LogError(ex, "Error retrieving dashboard note for user {UserId}", userId);
                
                return StatusCode(500, new ApiResponse<string>
                {
                    Success = false,
                    Message = "An error occurred while retrieving dashboard note",
                    Data = null,
                    Count = 0
                });
            }
        }

        [HttpPut("users/{userId}/dashboard-note")]
        [UserAdminOnly]
        public async Task<ActionResult<ApiResponse<string>>> UpdateUserDashboardNote(int userId, [FromBody] UpdateDashboardNoteRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Updating dashboard note for user {UserId} by user {CurrentUserId}", userId, UserId);
                
                // Validate input
                if (userId != request.UserId)
                {
                    return BadRequest(new ApiResponse<string>
                    {
                        Success = false,
                        Message = "User ID in URL does not match User ID in request body",
                        Data = null,
                        Count = 0
                    });
                }
                
                // Update only the dashboard note
                var success = await _dataService.UpdateUserDashboardNoteAsync(userId, request.NoteDashboard);
                
                stopwatch.Stop();
                
                if (success)
                {
                    await LogAuditAsync("UpdateUserDashboardNote", request, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                    
                    return Ok(new ApiResponse<string>
                    {
                        Success = true,
                        Message = "Dashboard note updated successfully",
                        Data = request.NoteDashboard,
                        Count = 1
                    });
                }
                else
                {
                    return NotFound(new ApiResponse<string>
                    {
                        Success = false,
                        Message = "User not found",
                        Data = null,
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateUserDashboardNote", ex);
                
                _logger.LogError(ex, "Error updating dashboard note for user {UserId}", userId);
                
                return StatusCode(500, new ApiResponse<string>
                {
                    Success = false,
                    Message = "An error occurred while updating dashboard note",
                    Data = null,
                    Count = 0
                });
            }
        }

        #region Employee Management

        [HttpGet("employees")]
        public async Task<ActionResult<ApiResponse<EmployeeManagementDto>>> GetEmployees([FromQuery] bool includeTrades = false, [FromQuery] bool excludeSensitiveData = false)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                List<EmployeeDto> employees;
                
                if (includeTrades)
                {
                    _logger.LogInformation("Getting all employees with roles and trade generals in optimized single query");
                    
                    // Get all employee data including roles AND trade generals in a single query (SUPER OPTIMIZED!)
                    var employeesWithRolesAndTradesDataTable = await _dataService.GetAllEmployeesWithRolesAndTradeGeneralsAsync();
                    
                    if (excludeSensitiveData)
                    {
                        employees = ConvertDataTableToEmployeesWithRolesAndTradeGeneralsSecure(employeesWithRolesAndTradesDataTable);
                    }
                    else
                    {
                        employees = ConvertDataTableToEmployeesWithRolesAndTradeGenerals(employeesWithRolesAndTradesDataTable);
                    }
                }
                else
                {
                    _logger.LogInformation("Getting all employees for management with optimized single query (roles only)");
                    
                    // Get all employee data including roles in a single query (OPTIMIZED!)
                    var employeesWithRolesDataTable = await _dataService.GetAllEmployeesWithRolesAsync();
                    
                    if (excludeSensitiveData)
                    {
                        employees = ConvertDataTableToEmployeesWithRolesSecure(employeesWithRolesDataTable);
                    }
                    else
                    {
                        employees = ConvertDataTableToEmployeesWithRoles(employeesWithRolesDataTable);
                    }
                }

                // Get zones
                var zonesDataTable = await _dataService.GetAllZonesAsync();
                var zones = ConvertDataTableToZones(zonesDataTable);

                // Get roles
                var rolesDataTable = await _dataService.GetAllRolesAsync();
                var roles = ConvertDataTableToRoles(rolesDataTable);
                
                stopwatch.Stop();
                var securityLevel = excludeSensitiveData ? " (secure mode - sensitive data excluded)" : "";
                await LogAuditAsync("GetEmployees", 
                    $"Retrieved {employees.Count} employees with {(includeTrades ? "roles and trade generals" : "roles")} in optimized query{securityLevel}", 
                    stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                
                var managementDto = new EmployeeManagementDto
                {
                    Employees = employees,
                    Zones = zones,
                    Roles = roles
                };
                
                return Ok(new ApiResponse<EmployeeManagementDto>
                {
                    Success = true,
                    Message = $"Retrieved {employees.Count} employees for management",
                    Data = managementDto,
                    Count = employees.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetEmployees", ex);
                
                _logger.LogError(ex, "Error retrieving employees for management");
                
                return StatusCode(500, new ApiResponse<EmployeeManagementDto>
                {
                    Success = false,
                    Message = "An error occurred while retrieving employees for management",
                    Count = 0
                });
            }
        }

        /// <summary>
        /// Get employees for employee directory - excludes sensitive data like passwords
        /// </summary>
        [HttpGet("employees/directory")]
        public async Task<ActionResult<ApiResponse<object>>> GetEmployeesForDirectory([FromQuery] bool includeTrades = true)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting employees for directory (secure mode)");
                
                List<EmployeeDto> employees;
                
                if (includeTrades)
                {
                    // Get all employee data including roles AND trade generals in secure mode
                    var employeesWithRolesAndTradesDataTable = await _dataService.GetAllEmployeesWithRolesAndTradeGeneralsAsync();
                    employees = ConvertDataTableToEmployeesWithRolesAndTradeGeneralsSecure(employeesWithRolesAndTradesDataTable);
                }
                else
                {
                    // Get all employee data including roles in secure mode
                    var employeesWithRolesDataTable = await _dataService.GetAllEmployeesWithRolesAsync();
                    employees = ConvertDataTableToEmployeesWithRolesSecure(employeesWithRolesDataTable);
                }

                stopwatch.Stop();
                await LogAuditAsync("GetEmployeesForDirectory", 
                    $"Retrieved {employees.Count} employees for directory (secure mode - sensitive data excluded)", 
                    stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                
                // Return just the employees list for directory compatibility
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = $"Retrieved {employees.Count} employees for directory",
                    Data = new { employees },
                    Count = employees.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetEmployeesForDirectory", ex);
                
                _logger.LogError(ex, "Error retrieving employees for directory");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving employees for directory",
                    Count = 0
                });
            }
        }

        /// <summary>
        /// Get employees for tech directory - minimal data (city/state only, work email/phone only)
        /// Full address only shown for the current authenticated user
        /// </summary>
        [HttpGet("employees/tech-directory")]
        public async Task<ActionResult<ApiResponse<object>>> GetEmployeesForTechDirectory([FromQuery] bool includeTrades = true)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting employees for tech directory (minimal data)");
                
                List<EmployeeDto> employees;
                
                if (includeTrades)
                {
                    // Get all employee data including roles AND trade generals in secure mode
                    var employeesWithRolesAndTradesDataTable = await _dataService.GetAllEmployeesWithRolesAndTradeGeneralsAsync();
                    employees = ConvertDataTableToEmployeesForTechDirectory(employeesWithRolesAndTradesDataTable, UserId);
                }
                else
                {
                    // Get all employee data including roles in secure mode
                    var employeesWithRolesDataTable = await _dataService.GetAllEmployeesWithRolesAsync();
                    employees = ConvertDataTableToEmployeesForTechDirectoryNoTrades(employeesWithRolesDataTable, UserId);
                }

                stopwatch.Stop();
                await LogAuditAsync("GetEmployeesForTechDirectory", 
                    $"Retrieved {employees.Count} employees for tech directory (minimal data - city/state, work contact only)", 
                    stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                
                // Return just the employees list for directory compatibility
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = $"Retrieved {employees.Count} employees for tech directory",
                    Data = new { employees },
                    Count = employees.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetEmployeesForTechDirectory", ex);
                
                _logger.LogError(ex, "Error retrieving employees for tech directory");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving employees for tech directory",
                    Count = 0
                });
            }
        }

        [HttpGet("employees/{id:int}")]
        public async Task<ActionResult<ApiResponse<EmployeeDto>>> GetEmployeeById(int id)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting employee by ID: {EmployeeId}", id);
                
                if (id <= 0)
                {
                    return BadRequest(new ApiResponse<EmployeeDto>
                    {
                        Success = false,
                        Message = "Employee ID must be greater than 0",
                        Count = 0
                    });
                }

                var dataTable = await _dataService.GetEmployeeByIdAsync(id);
                if (dataTable.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<EmployeeDto>
                    {
                        Success = false,
                        Message = "Employee not found",
                        Count = 0
                    });
                }

                var employees = ConvertDataTableToEmployees(dataTable);
                var employee = employees.First();

                // Get roles for this employee
                var userRolesDataTable = await _dataService.GetUserRolesByUserIdAsync(employee.Id);
                employee.Roles = ConvertDataTableToUserRoles(userRolesDataTable);
                
                stopwatch.Stop();
                await LogAuditAsync("GetEmployeeById", $"Retrieved employee {id}", stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                
                return Ok(new ApiResponse<EmployeeDto>
                {
                    Success = true,
                    Message = "Employee retrieved successfully",
                    Data = employee,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetEmployeeById", ex);
                
                _logger.LogError(ex, "Error retrieving employee with ID {EmployeeId}", id);
                
                return StatusCode(500, new ApiResponse<EmployeeDto>
                {
                    Success = false,
                    Message = "An error occurred while retrieving employee",
                    Count = 0
                });
            }
        }

        [HttpPost("employees")]
        [UserAdminOnly]
        public async Task<ActionResult<ApiResponse<EmployeeDto>>> CreateEmployee([FromBody] CreateEmployeeRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Creating new employee: {FirstName} {LastName}", request.FirstName, request.LastName);
                
                // Validate required fields
                if (string.IsNullOrWhiteSpace(request.Username))
                {
                    return BadRequest(new ApiResponse<EmployeeDto>
                    {
                        Success = false,
                        Message = "Username is required",
                        Count = 0
                    });
                }

                if (string.IsNullOrWhiteSpace(request.Password))
                {
                    return BadRequest(new ApiResponse<EmployeeDto>
                    {
                        Success = false,
                        Message = "Password is required",
                        Count = 0
                    });
                }

                var employeeId = await _dataService.CreateEmployeeAsync(request);
                
                stopwatch.Stop();
                
                if (employeeId.HasValue)
                {
                    // Get the created employee to return
                    var dataTable = await _dataService.GetEmployeeByIdAsync(employeeId.Value);
                    if (dataTable.Rows.Count > 0)
                    {
                        var employees = ConvertDataTableToEmployees(dataTable);
                        var createdEmployee = employees.First();
                        
                        // Get roles for this employee
                        var userRolesDataTable = await _dataService.GetUserRolesByUserIdAsync(createdEmployee.Id);
                        createdEmployee.Roles = ConvertDataTableToUserRoles(userRolesDataTable);
                        
                        // Don't return the password in the response
                        createdEmployee.Password = string.Empty;
                        
                        await LogAuditAsync("CreateEmployee", request, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                        
                        return Ok(new ApiResponse<EmployeeDto>
                        {
                            Success = true,
                            Message = "Employee created successfully",
                            Data = createdEmployee,
                            Count = 1
                        });
                    }
                    else
                    {
                        return Ok(new ApiResponse<EmployeeDto>
                        {
                            Success = true,
                            Message = "Employee created successfully",
                            Count = 1
                        });
                    }
                }
                else
                {
                    return StatusCode(500, new ApiResponse<EmployeeDto>
                    {
                        Success = false,
                        Message = "Failed to create employee",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateEmployee", ex);
                
                _logger.LogError(ex, "Error creating employee {FirstName} {LastName}", request.FirstName, request.LastName);
                
                return StatusCode(500, new ApiResponse<EmployeeDto>
                {
                    Success = false,
                    Message = "An error occurred while creating employee",
                    Count = 0
                });
            }
        }

        [HttpPut("employees/{id:int}")]
        [UserAdminOnly]
        public async Task<ActionResult<ApiResponse<EmployeeDto>>> UpdateEmployee(int id, [FromBody] UpdateEmployeeRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Updating employee {EmployeeId}: {FirstName} {LastName}", id, request.FirstName, request.LastName);
                _logger.LogInformation("Phone fields received - PhoneMobile: {PhoneMobile}, PhoneHome: {PhoneHome}, PhoneDesk: {PhoneDesk}, Extension: {Extension}", 
                    request.PhoneMobile, request.PhoneHome, request.PhoneDesk, request.Extension);
                
                // Validate input
                if (id != request.Id)
                {
                    return BadRequest(new ApiResponse<EmployeeDto>
                    {
                        Success = false,
                        Message = "Employee ID in URL does not match Employee ID in request body",
                        Count = 0
                    });
                }

                if (string.IsNullOrWhiteSpace(request.Username))
                {
                    return BadRequest(new ApiResponse<EmployeeDto>
                    {
                        Success = false,
                        Message = "Username is required",
                        Count = 0
                    });
                }

                // Ensure the request ID matches the URL parameter
                request.Id = id;

                var success = await _dataService.UpdateEmployeeAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
                    // Get the updated employee to return
                    var dataTable = await _dataService.GetEmployeeByIdAsync(id);
                    if (dataTable.Rows.Count > 0)
                    {
                        var employees = ConvertDataTableToEmployees(dataTable);
                        var updatedEmployee = employees.First();
                        
                        // Get roles for this employee
                        var userRolesDataTable = await _dataService.GetUserRolesByUserIdAsync(updatedEmployee.Id);
                        updatedEmployee.Roles = ConvertDataTableToUserRoles(userRolesDataTable);
                        
                        // Don't return the password in the response
                        updatedEmployee.Password = string.Empty;
                        
                        await LogAuditAsync("UpdateEmployee", request, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
            
                        return Ok(new ApiResponse<EmployeeDto>
                        {
                            Success = true,
                            Message = "Employee updated successfully",
                            Data = updatedEmployee,
                            Count = 1
                        });
                    }
                    else
                    {
                        return Ok(new ApiResponse<EmployeeDto>
                        {
                            Success = true,
                            Message = "Employee updated successfully",
                            Count = 1
                        });
                    }
                }
                else
                {
                    return NotFound(new ApiResponse<EmployeeDto>
                    {
                        Success = false,
                        Message = "Employee not found",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateEmployee", ex);
                
                _logger.LogError(ex, "Error updating employee {EmployeeId}", id);
                
                return StatusCode(500, new ApiResponse<EmployeeDto>
                {
                    Success = false,
                    Message = "An error occurred while updating employee",
                    Count = 0
                });
            }
        }

        [HttpGet("tradegenerals")]
        public async Task<ActionResult<ApiResponse<List<TradeGeneralDto>>>> GetTradeGenerals()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting all trade generals");
                
                var dataTable = await _dataService.GetAllTradeGeneralsAsync();
                var tradeGenerals = ConvertDataTableToTradeGenerals(dataTable);
                
                stopwatch.Stop();
                await LogAuditAsync("GetTradeGenerals", $"Retrieved {tradeGenerals.Count} trade generals", stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                
                return Ok(new ApiResponse<List<TradeGeneralDto>>
                {
                    Success = true,
                    Message = $"Retrieved {tradeGenerals.Count} trade generals",
                    Data = tradeGenerals,
                    Count = tradeGenerals.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetTradeGenerals", ex);
                
                _logger.LogError(ex, "Error retrieving trade generals");
                
                return StatusCode(500, new ApiResponse<List<TradeGeneralDto>>
                {
                    Success = false,
                    Message = "An error occurred while retrieving trade generals",
                    Count = 0
                });
            }
        }

        [HttpGet("employees/{userId:int}/tradegenerals")]
        public async Task<ActionResult<ApiResponse<List<UserTradeGeneralDto>>>> GetUserTradeGenerals(int userId)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting trade generals for user {UserId}", userId);
                
                if (userId <= 0)
                {
                    return BadRequest(new ApiResponse<List<UserTradeGeneralDto>>
                    {
                        Success = false,
                        Message = "Invalid user ID",
                        Count = 0
                    });
                }
                
                var dataTable = await _dataService.GetUserTradeGeneralsByUserIdAsync(userId);
                var userTradeGenerals = ConvertDataTableToUserTradeGenerals(dataTable);
                
                stopwatch.Stop();
                await LogAuditAsync("GetUserTradeGenerals", $"Retrieved {userTradeGenerals.Count} trade generals for user {userId}", stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                
                return Ok(new ApiResponse<List<UserTradeGeneralDto>>
                {
                    Success = true,
                    Message = $"Retrieved {userTradeGenerals.Count} trade generals for user",
                    Data = userTradeGenerals,
                    Count = userTradeGenerals.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetUserTradeGenerals", ex);
                
                _logger.LogError(ex, "Error retrieving trade generals for user {UserId}", userId);
                
                return StatusCode(500, new ApiResponse<List<UserTradeGeneralDto>>
                {
                    Success = false,
                    Message = "An error occurred while retrieving user trade generals",
                    Count = 0
                });
            }
        }

        [HttpPut("employees/{userId:int}/tradegenerals")]
        public async Task<ActionResult<ApiResponse<object>>> UpdateEmployeeTradeGenerals(int userId, [FromBody] UpdateEmployeeTradeGeneralsRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Updating trade generals for user {UserId}", userId);
                
                if (userId <= 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Invalid user ID",
                        Count = 0
                    });
                }

                if (request.UserId != userId)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "User ID in URL does not match user ID in request body",
                        Count = 0
                    });
                }
                
                var success = await _dataService.UpdateEmployeeTradeGeneralsAsync(userId, request.TradeGeneralIds);
                
                if (success)
                {
                    stopwatch.Stop();
                    await LogAuditAsync("UpdateEmployeeTradeGenerals", $"Updated trade generals for user {userId}, assigned {request.TradeGeneralIds.Count} trade generals", stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                    
                    return Ok(new ApiResponse<object>
                    {
                        Success = true,
                        Message = "Employee trade generals updated successfully",
                        Count = request.TradeGeneralIds.Count
                    });
                }
                else
                {
                    return StatusCode(500, new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Failed to update employee trade generals",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateEmployeeTradeGenerals", ex);
                
                _logger.LogError(ex, "Error updating trade generals for user {UserId}", userId);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while updating employee trade generals",
                    Count = 0
                });
            }
        }

        #endregion

        [HttpGet("adminusers")]
        public async Task<ActionResult<ApiResponse<List<UserDto>>>> GetAdminUsers()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting active admin users");
                
                var dataTable = await _dataService.GetAdminUsersAsync();
                var users = ConvertDataTableToUsers(dataTable);
                
                stopwatch.Stop();
                await LogOperationAsync("GetAdminUsers", $"Retrieved {users.Count} active admin users", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<List<UserDto>>
                {
                    Success = true,
                    Message = $"Retrieved {users.Count} active admin users",
                    Data = users,
                    Count = users.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetAdminUsers", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving admin users");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving admin users",
                    Count = 0
                });
            }
        }

        [HttpGet("technicians")]
        public async Task<ActionResult<ApiResponse<List<TechnicianDto>>>> GetTechnicians()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting active technicians");
                
                // Get data from service
                var dataTable = await _dataService.GetActiveTechniciansAsync();
                var technicians = ConvertDataTableToTechnicians(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetTechnicians", $"Retrieved {technicians.Count} active technicians", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<TechnicianDto>>
                {
                    Success = true,
                    Message = "Active technicians retrieved successfully",
                    Data = technicians,
                    Count = technicians.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetTechnicians", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving active technicians");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving active technicians",
                    Count = 0
                });
            }
        }

        [HttpGet("statusassignments")]
        public async Task<ActionResult<ApiResponse<StatusAssignmentMatrixDto>>> GetStatusAssignments()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting status assignment matrix");
                
                // Get all the data in parallel
                var zonesTask = _dataService.GetAllZonesAsync();
                var usersTask = _dataService.GetAdminUsersAsync();
                var statusSecondariesTask = _dataService.GetAllStatusSecondariesAsync();
                var assignmentsTask = _dataService.GetAdminZoneStatusAssignmentsAsync();
                
                await Task.WhenAll(zonesTask, usersTask, statusSecondariesTask, assignmentsTask);
                
                var zones = ConvertDataTableToZones(await zonesTask);
                var users = ConvertDataTableToUsers(await usersTask);
                var statusSecondaries = ConvertDataTableToStatusSecondaries(await statusSecondariesTask);
                var assignments = ConvertDataTableToAdminZoneStatusAssignments(await assignmentsTask);
                
                var matrix = new StatusAssignmentMatrixDto
                {
                    Zones = zones,
                    Users = users,
                    StatusSecondaries = statusSecondaries,
                    Assignments = assignments
                };
                
                stopwatch.Stop();
                await LogOperationAsync("GetStatusAssignments", 
                    $"Retrieved matrix with {zones.Count} zones, {users.Count} users, {statusSecondaries.Count} status secondaries, {assignments.Count} assignments", 
                    stopwatch.Elapsed);
                
                return Ok(new ApiResponse<StatusAssignmentMatrixDto>
                {
                    Success = true,
                    Message = "Status assignment matrix retrieved successfully",
                    Data = matrix,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetStatusAssignments", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving status assignment matrix");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving status assignment matrix",
                    Count = 0
                });
            }
        }

        [HttpGet("attackpoints")]
        public async Task<ActionResult<ApiResponse<List<AttackPointDto>>>> GetAttackPoints([FromQuery] int topCount = 15)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting attack points with top {TopCount} per admin", topCount);
                
                // Validate input
                if (topCount <= 0 || topCount > 100)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "TopCount must be between 1 and 100",
                        Count = 0
                    });
                }

                // Get data from service
                var dataTable = await _dataService.GetAttackPointsAsync(topCount);
                var attackPoints = ConvertDataTableToAttackPoints(dataTable);

                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetAttackPoints", $"Retrieved {attackPoints.Count} attack points with top {topCount} per admin", stopwatch.Elapsed);

                return Ok(new ApiResponse<List<AttackPointDto>>
                {
                    Success = true,
                    Message = "Attack points retrieved successfully",
                    Data = attackPoints,
                    Count = attackPoints.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetAttackPoints", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving attack points with top {TopCount} per admin", topCount);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving attack points",
                    Count = 0
                });
            }
        }

        [HttpGet("attackpointactionabledates")]
        public async Task<ActionResult<ApiResponse<List<AttackPointActionableDateDto>>>> GetAttackPointActionableDates()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting all attack point actionable dates");
                
                // Get data from service
                var dataTable = await _dataService.GetAllAttackPointActionableDatesAsync();
                var actionableDates = ConvertDataTableToAttackPointActionableDates(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetAttackPointActionableDates", $"Retrieved {actionableDates.Count} attack point actionable dates", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<AttackPointActionableDateDto>>
                {
                    Success = true,
                    Message = "Attack point actionable dates retrieved successfully",
                    Data = actionableDates,
                    Count = actionableDates.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetAttackPointActionableDates", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving attack point actionable dates");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving attack point actionable dates",
                    Count = 0
                });
            }
        }

        [HttpGet("attachments")]
        public async Task<ActionResult<ApiResponse<List<AttachmentDto>>>> GetAttachments([FromQuery] int srId)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting attachments for service request {SrId}", srId);
                
                // Validate input
                if (srId <= 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Valid service request ID is required",
                        Count = 0
                    });
                }
    
                // Get data from service
                var dataTable = await _dataService.GetAttachmentsByServiceRequestAsync(srId);
                var attachments = ConvertDataTableToAttachments(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetAttachments", $"Retrieved {attachments.Count} attachments for service request {srId}", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<AttachmentDto>>
                {
                    Success = true,
                    Message = "Attachments retrieved successfully",
                    Data = attachments,
                    Count = attachments.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetAttachments", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving attachments for service request {SrId}", srId);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving attachments",
                    Count = 0
                });
            }
        }

        [HttpGet("call-center-attachments")]
        public async Task<ActionResult<ApiResponse<List<AttachmentDto>>>> GetCallCenterAttachments([FromQuery] int ccId)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting attachments for call center {CcId}", ccId);
                
                // Validate input
                if (ccId <= 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Valid call center ID is required",
                        Count = 0
                    });
                }
    
                // Get data from service
                var dataTable = await _dataService.GetAttachmentsByCallCenterAsync(ccId);
                var attachments = ConvertDataTableToAttachments(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetCallCenterAttachments", $"Retrieved {attachments.Count} attachments for call center {ccId}", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<AttachmentDto>>
                {
                    Success = true,
                    Message = "Attachments retrieved successfully",
                    Data = attachments,
                    Count = attachments.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetCallCenterAttachments", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving attachments for call center {CcId}", ccId);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving attachments",
                    Count = 0
                });
            }
        }

        [HttpGet("call-center-attachments-all")]
        public async Task<ActionResult<ApiResponse<List<AttachmentDto>>>> GetAllCallCenterAttachments()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting all call center attachments");
                
                // Get data from service - gets one attachment per call center (most recent)
                var dataTable = await _dataService.GetAllCallCenterAttachmentsAsync();
                var attachments = ConvertDataTableToAttachments(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                await LogOperationAsync("GetAllCallCenterAttachments", $"Retrieved {attachments.Count} call center attachments", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<AttachmentDto>>
                {
                    Success = true,
                    Message = "Call center attachments retrieved successfully",
                    Data = attachments,
                    Count = attachments.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetAllCallCenterAttachments", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving all call center attachments");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving attachments",
                    Count = 0
                });
            }
        }

        [HttpPut("call-center-attachment-description")]
        public async Task<ActionResult<ApiResponse<object>>> UpdateCallCenterAttachmentDescription([FromBody] UpdateAttachmentDescriptionRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                if (request == null || request.AttachmentId <= 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Valid attachment ID is required"
                    });
                }

                _logger.LogInformation("Updating attachment {AttId} description", request.AttachmentId);
                
                // Fetch current attachment for change history
                var attachmentsTable = await _dataService.GetAttachmentByIdAsync(request.AttachmentId);
                if (attachmentsTable == null || attachmentsTable.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Attachment not found"
                    });
                }
                
                var currentAttachmentRow = attachmentsTable.Rows[0];
                
                await _dataService.UpdateAttachmentDescriptionAsync(request.AttachmentId, request.Description);
                
                stopwatch.Stop();
                
                // Log critical change
                var oldValues = new Dictionary<string, object?>
                {
                    { "Description", currentAttachmentRow["att_description"] ?? "" }
                };
                
                var newValues = new Dictionary<string, object?>
                {
                    { "Description", request.Description ?? "" }
                };
                
                SetAuditCriticalUserContext();
                var filename = currentAttachmentRow["att_filename"]?.ToString() ?? "Unknown";
                var ccId = currentAttachmentRow["cc_id"];
                var ccName = currentAttachmentRow["cc_name"]?.ToString() ?? "Unknown";
                await _auditCriticalService.LogChangeAsync(
                    $"Attachment Description Updated - {filename} (Call Center: {ccName} - ID: {ccId}, Attachment ID: {request.AttachmentId})",
                    oldValues,
                    newValues,
                    stopwatch.Elapsed.TotalSeconds.ToString("F3")
                );
                
                await LogOperationAsync("UpdateCallCenterAttachmentDescription", $"Updated attachment {request.AttachmentId} description", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Attachment description updated successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("UpdateCallCenterAttachmentDescription", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error updating attachment description");
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while updating attachment description"
                });
            }
        }

        [HttpDelete("call-center-attachment/{attachmentId}")]
        public async Task<ActionResult<ApiResponse<object>>> DeleteCallCenterAttachment(int attachmentId)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                if (attachmentId <= 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Valid attachment ID is required"
                    });
                }

                _logger.LogInformation("Deleting attachment {AttId}", attachmentId);
                
                // Fetch current attachment for change history
                var attachmentsTable = await _dataService.GetAttachmentByIdAsync(attachmentId);
                if (attachmentsTable == null || attachmentsTable.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Attachment not found"
                    });
                }
                
                var attachmentToDelete = attachmentsTable.Rows[0];
                
                await _dataService.DeleteAttachmentAsync(attachmentId);
                
                stopwatch.Stop();
                
                // Log critical change
                var oldValues = new Dictionary<string, object?>
                {
                    { "Filename", attachmentToDelete["att_filename"] ?? "" },
                    { "Description", attachmentToDelete["att_description"] ?? "" },
                    { "InsertDateTime", attachmentToDelete["att_insertdatetime"] }
                };
                
                SetAuditCriticalUserContext();
                var filename = attachmentToDelete["att_filename"]?.ToString() ?? "Unknown";
                var ccId = attachmentToDelete["cc_id"];
                var ccName = attachmentToDelete["cc_name"]?.ToString() ?? "Unknown";
                await _auditCriticalService.LogChangeAsync(
                    $"Attachment Deleted - {filename} (Call Center: {ccName} - ID: {ccId}, Attachment ID: {attachmentId})",
                    oldValues,
                    new Dictionary<string, object?>(), // Empty new values for delete
                    stopwatch.Elapsed.TotalSeconds.ToString("F3")
                );
                
                await LogOperationAsync("DeleteCallCenterAttachment", $"Deleted attachment {attachmentId}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Attachment deleted successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("DeleteCallCenterAttachment", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error deleting attachment {AttId}", attachmentId);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while deleting attachment"
                });
            }
        }

        [HttpGet("pending-tech-info")]
        public async Task<ActionResult<ApiResponse<List<PendingTechInfoDto>>>> GetPendingTechInfo([FromQuery] int? userId = null)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                // Use current user ID if none provided
                var targetUserId = userId ?? UserId;
                
                _logger.LogInformation("Getting pending tech info for user {UserId}", targetUserId);
    
                // Get data from service
                var dataTable = await _dataService.GetPendingTechInfoAsync(targetUserId);
                var pendingTechInfo = ConvertDataTableToPendingTechInfo(dataTable);
    
                stopwatch.Stop();
                
                // Log successful operation
                // await LogOperationAsync("GetPendingTechInfo", $"Retrieved {pendingTechInfo.Count} pending tech info records for user {targetUserId}", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<List<PendingTechInfoDto>>
                {
                    Success = true,
                    Message = "Pending tech info retrieved successfully",
                    Data = pendingTechInfo,
                    Count = pendingTechInfo.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("GetPendingTechInfo", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error retrieving pending tech info for user {UserId}", userId ?? UserId);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while retrieving pending tech info",
                    Count = 0
                });
            }
        }
    #endregion



    #region Post
    [HttpPost("workorders")]
        public async Task<ActionResult<ApiResponse<List<WorkOrderDto>>>> GetWorkOrdersPost([FromBody] WorkOrderRequest request)
        {
            return await GetWorkOrders(request.NumberOfDays);
        }
    
        [HttpPost("workorders/schedule")]
        public async Task<ActionResult<ApiResponse<List<WorkOrderDto>>>> GetWorkOrdersSchedulePost([FromBody] WorkOrderRequest request)
        {
            return await GetWorkOrdersSchedule(request.NumberOfDays);
        }

        [HttpPost("attackpoints")]
        public async Task<ActionResult<ApiResponse<List<AttackPointDto>>>> GetAttackPointsPost([FromBody] AttackPointRequest request)
        {
            return await GetAttackPoints(request.TopCount);
        }

        [HttpPost("users")]
        [UserAdminOnly]
        public async Task<ActionResult<ApiResponse<UserDto>>> CreateUser([FromBody] CreateUserRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Creating new user: {Username}", request.Username);
                
                // Validate the request
                if (string.IsNullOrWhiteSpace(request.Username) || request.Username.Length < 3)
                {
                    return BadRequest(new ApiResponse<UserDto>
                    {
                        Success = false,
                        Message = "Username must be at least 3 characters long",
                        Count = 0
                    });
                }
                
                if (string.IsNullOrWhiteSpace(request.Password) || request.Password.Length < 6)
                {
                    return BadRequest(new ApiResponse<UserDto>
                    {
                        Success = false,
                        Message = "Password must be at least 6 characters long",
                        Count = 0
                    });
                }
                
                if (request.OId <= 0)
                {
                    return BadRequest(new ApiResponse<UserDto>
                    {
                        Success = false,
                        Message = "Valid Organization ID is required",
                        Count = 0
                    });
                }
                
                var newId = await _dataService.CreateUserAsync(request);
                
                if (newId.HasValue)
                {
                    // Get the created user to return
                    var userDataTable = await _dataService.GetUserByIdAsync(newId.Value);
                    var users = ConvertDataTableToUsers(userDataTable);
                    var newUser = users.First();
                    
                    // Don't return the password in the response
                    newUser.Password = string.Empty;
                    
                    stopwatch.Stop();
                    await LogOperationAsync("CreateUser", $"Created user - {request.Username} ({request.FirstName} {request.LastName}) with ID {newId.Value}", stopwatch.Elapsed);
                    
                    return Ok(new ApiResponse<UserDto>
                    {
                        Success = true,
                        Message = "User created successfully",
                        Data = newUser,
                        Count = 1
                    });
                }
                else
                {
                    stopwatch.Stop();
                    await LogOperationAsync("CreateUser", $"Failed to create user - {request.Username}", stopwatch.Elapsed);
                    
                    return BadRequest(new ApiResponse<UserDto>
                    {
                        Success = false,
                        Message = "Failed to create user",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("CreateUser", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error creating user {Username}", request.Username);
                
                return StatusCode(500, new ApiResponse<UserDto>
                {
                    Success = false,
                    Message = "An error occurred while creating the user",
                    Count = 0
                });
            }
        }

        [HttpPost("statusassignments")]
        public async Task<ActionResult<ApiResponse<AdminZoneStatusAssignmentDto>>> CreateStatusAssignment([FromBody] CreateAdminZoneStatusAssignmentRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Creating status assignment for User {UserId}, Zone {ZoneId}, Status {StatusSecondaryId}", 
                    request.UserId, request.ZoneId, request.StatusSecondaryId);
                
                // Validate input
                if (request.UserId <= 0 || request.ZoneId <= 0 || request.StatusSecondaryId <= 0)
                {
                    return BadRequest(new ApiResponse<AdminZoneStatusAssignmentDto>
                    {
                        Success = false,
                        Message = "Valid User ID, Zone ID, and Status Secondary ID are required",
                        Count = 0
                    });
                }

                // Create assignment
                var newId = await _dataService.CreateAdminZoneStatusAssignmentAsync(request);
                
                stopwatch.Stop();
                
                if (newId.HasValue)
                {
                    var newAssignment = new AdminZoneStatusAssignmentDto
                    {
                        Id = newId.Value,
                        UserId = request.UserId,
                        ZoneId = request.ZoneId,
                        StatusSecondaryId = request.StatusSecondaryId,
                        InsertDateTime = DateTime.UtcNow
                    };
                    
                    await LogOperationAsync("CreateStatusAssignment", 
                        $"Created assignment {newId} for User {request.UserId}, Zone {request.ZoneId}, Status {request.StatusSecondaryId}", 
                        stopwatch.Elapsed);
                    
                    return Ok(new ApiResponse<AdminZoneStatusAssignmentDto>
                    {
                        Success = true,
                        Message = "Status assignment created successfully",
                        Data = newAssignment,
                        Count = 1
                    });
                }
                else
                {
                    await LogErrorAsync("CreateStatusAssignment", 
                        new Exception("Failed to create status assignment - no ID returned"), 
                        stopwatch.Elapsed);
                    
                    return BadRequest(new ApiResponse<AdminZoneStatusAssignmentDto>
                    {
                        Success = false,
                        Message = "Failed to create status assignment",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("CreateStatusAssignment", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error creating status assignment for User {UserId}, Zone {ZoneId}, Status {StatusSecondaryId}", 
                    request.UserId, request.ZoneId, request.StatusSecondaryId);
                
                return StatusCode(500, new ApiResponse<AdminZoneStatusAssignmentDto>
                {
                    Success = false,
                    Message = "An error occurred while creating the status assignment",
                    Count = 0
                });
            }
        }

    #endregion



    #region Put
    [HttpPut("users/{id}")]
    [UserAdminOnly]
        public async Task<ActionResult<ApiResponse<UserDto>>> UpdateUser(int id, [FromBody] UpdateUserRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Updating user {Id}", id);
                
                // Validate input
                if (id != request.Id)
                {
                    return BadRequest(new ApiResponse<UserDto>
                    {
                        Success = false,
                        Message = "ID in URL does not match ID in request body",
                        Count = 0
                    });
                }
                
                if (string.IsNullOrWhiteSpace(request.Username) || request.Username.Length < 3)
                {
                    return BadRequest(new ApiResponse<UserDto>
                    {
                        Success = false,
                        Message = "Username must be at least 3 characters long",
                        Count = 0
                    });
                }
                
                // If password is provided, validate it
                if (!string.IsNullOrEmpty(request.Password) && request.Password.Length < 6)
                {
                    return BadRequest(new ApiResponse<UserDto>
                    {
                        Success = false,
                        Message = "Password must be at least 6 characters long",
                        Count = 0
                    });
                }
                
                if (request.OId <= 0)
                {
                    return BadRequest(new ApiResponse<UserDto>
                    {
                        Success = false,
                        Message = "Valid Organization ID is required",
                        Count = 0
                    });
                }

                // Set the ID from the URL parameter
                request.Id = id;

                // Update user
                var success = await _dataService.UpdateUserAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
                    // Get the updated user to return
                    var userDataTable = await _dataService.GetUserByIdAsync(id);
                    if (userDataTable.Rows.Count > 0)
                    {
                        var users = ConvertDataTableToUsers(userDataTable);
                        var updatedUser = users.First();
                        
                        // Don't return the password in the response
                        updatedUser.Password = string.Empty;
                        
                        // Log with JSON payload as detail object for better formatting
                        await LogAuditAsync("UpdateUser", request, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
            
                        return Ok(new ApiResponse<UserDto>
                        {
                            Success = true,
                            Message = "User updated successfully",
                            Data = updatedUser,
                            Count = 1
                        });
                    }
                    else
                    {
                        return Ok(new ApiResponse<UserDto>
                        {
                            Success = true,
                            Message = "User updated successfully",
                            Count = 1
                        });
                    }
                }
                else
                {
                    return NotFound(new ApiResponse<UserDto>
                    {
                        Success = false,
                        Message = "User not found",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("UpdateUser", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error updating user {Id}", id);
                
                return StatusCode(500, new ApiResponse<UserDto>
                {
                    Success = false,
                    Message = "An error occurred while updating the user",
                    Count = 0
                });
            }
        }

        [HttpPut("priorities/{id}")]
        public async Task<ActionResult<ApiResponse<object>>> UpdatePriority(int id, [FromBody] UpdatePriorityRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Updating priority {Id}", id);
                
                // Validate input
                if (id != request.Id)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "ID in URL does not match ID in request body",
                        Count = 0
                    });
                }
                
                if (string.IsNullOrWhiteSpace(request.PriorityName))
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Priority name is required",
                        Count = 0
                    });
                }

                // Update priority
                var success = await _dataService.UpdatePriorityAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
                    // Log successful operation
                    await LogOperationAsync("UpdatePriority", $"Updated priority {id} - {request.PriorityName}", stopwatch.Elapsed);
        
                    return Ok(new ApiResponse<object>
                    {
                        Success = true,
                        Message = "Priority updated successfully",
                        Count = 1
                    });
                }
                else
                {
                    return NotFound(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Priority not found",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("UpdatePriority", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error updating priority {Id}", id);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while updating the priority",
                    Count = 0
                });
            }
        }

        [HttpPut("statussecondaries/{id}")]
        public async Task<ActionResult<ApiResponse<object>>> UpdateStatusSecondary(int id, [FromBody] UpdateStatusSecondaryRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Updating status secondary {Id}", id);
                
                // Validate input
                if (id != request.Id)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "ID in URL does not match ID in request body",
                        Count = 0
                    });
                }
                
                if (string.IsNullOrWhiteSpace(request.StatusSecondary))
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Status secondary name is required",
                        Count = 0
                    });
                }

                // Update status secondary
                var success = await _dataService.UpdateStatusSecondaryAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
                    // Log successful operation
                    await LogOperationAsync("UpdateStatusSecondary", $"Updated status secondary {id} - {request.StatusSecondary}", stopwatch.Elapsed);
        
                    return Ok(new ApiResponse<object>
                    {
                        Success = true,
                        Message = "Status secondary updated successfully",
                        Count = 1
                    });
                }
                else
                {
                    return NotFound(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Status secondary not found",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("UpdateStatusSecondary", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error updating status secondary {Id}", id);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while updating the status secondary",
                    Count = 0
                });
            }
        }

        [HttpPut("callcenters/{id}")]
        public async Task<ActionResult<ApiResponse<object>>> UpdateCallCenter(int id, [FromBody] UpdateCallCenterRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Updating call center {Id}", id);
                
                // Validate input
                if (id != request.Id)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "ID in URL does not match ID in request body",
                        Count = 0
                    });
                }
                
                if (string.IsNullOrWhiteSpace(request.Name))
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Call center name is required",
                        Count = 0
                    });
                }

                // Get current call center data for audit logging
                var currentCallCentersDataTable = await _dataService.GetAllCallCentersAsync();
                var currentCallCenters = ConvertDataTableToCallCenters(currentCallCentersDataTable);
                var currentCallCenter = currentCallCenters?.FirstOrDefault(cc => cc.Id == id);

                // Update call center
                var success = await _dataService.UpdateCallCenterAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
                    // Log critical audit with change details
                    var oldValues = new Dictionary<string, object?>
                    {
                        { "Name", currentCallCenter?.Name },
                        { "Active", currentCallCenter?.Active },
                        { "Note", currentCallCenter?.Note },
                        { "OId", currentCallCenter?.OId },
                        { "Attack", currentCallCenter?.Attack },
                        { "PortalName", currentCallCenter?.PortalName },
                        { "PortalUrl", currentCallCenter?.PortalUrl },
                        { "PortalCredentials", currentCallCenter?.PortalCredentials }
                    };
                    
                    var newValues = new Dictionary<string, object?>
                    {
                        { "Name", request.Name },
                        { "Active", request.Active },
                        { "Note", request.Note },
                        { "OId", request.OId },
                        { "Attack", request.Attack },
                        { "PortalName", request.PortalName },
                        { "PortalUrl", request.PortalUrl },
                        { "PortalCredentials", request.PortalCredentials }
                    };
                    
                    SetAuditCriticalUserContext();
                    await _auditCriticalService.LogChangeAsync(
                        $"Call Center Updated - {request.Name} (ID: {id})",
                        oldValues,
                        newValues,
                        stopwatch.Elapsed.TotalSeconds.ToString("F3")
                    );
                    
                    // Log successful operation
                    await LogOperationAsync("UpdateCallCenter", $"Updated call center {id} - {request.Name}", stopwatch.Elapsed);
        
                    return Ok(new ApiResponse<object>
                    {
                        Success = true,
                        Message = "Call center updated successfully",
                        Count = 1
                    });
                }
                else
                {
                    return NotFound(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Call center not found",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("UpdateCallCenter", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error updating call center {Id}", id);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while updating the call center",
                    Count = 0
                });
            }
        }

        [HttpPost("callcenters")]
        public async Task<ActionResult<ApiResponse<CallCenterDto>>> CreateCallCenter([FromBody] CreateCallCenterRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Creating new call center: {Name}", request.Name);
                
                // Validate the request
                if (string.IsNullOrWhiteSpace(request.Name) || request.Name.Length < 3)
                {
                    return BadRequest(new ApiResponse<CallCenterDto>
                    {
                        Success = false,
                        Message = "Call center name must be at least 3 characters long",
                        Count = 0
                    });
                }
                
                var newId = await _dataService.CreateCallCenterAsync(request);
                
                if (newId.HasValue)
                {
                    // Create the DTO to return
                    var newCallCenter = new CallCenterDto
                    {
                        Id = newId.Value,
                        OId = request.O_id,
                        Name = request.Name,
                        Active = request.Active,
                        TempId = null,
                        Note = request.Note,
                        Attack = request.Attack,
                        InsertDateTime = DateTime.Now,
                        ModifiedDateTime = DateTime.Now
                    };
                    
                    stopwatch.Stop();
                    
                    // Log critical audit for new call center creation
                    var newValues = new Dictionary<string, object?>
                    {
                        { "Name", request.Name },
                        { "Active", request.Active },
                        { "Note", request.Note },
                        { "OId", request.O_id },
                        { "Attack", request.Attack }
                    };
                    
                    SetAuditCriticalUserContext();
                    await _auditCriticalService.LogChangeAsync(
                        $"Call Center Created - {request.Name} (ID: {newId.Value})",
                        null,
                        newValues,
                        stopwatch.Elapsed.TotalSeconds.ToString("F3")
                    );
                    
                    await LogOperationAsync("CreateCallCenter", $"Created call center - {request.Name} with ID {newId.Value}", stopwatch.Elapsed);
                    
                    return Ok(new ApiResponse<CallCenterDto>
                    {
                        Success = true,
                        Message = "Call center created successfully",
                        Data = newCallCenter,
                        Count = 1
                    });
                }
                else
                {
                    stopwatch.Stop();
                    await LogOperationAsync("CreateCallCenter", $"Failed to create call center - {request.Name}", stopwatch.Elapsed);
                    
                    return BadRequest(new ApiResponse<CallCenterDto>
                    {
                        Success = false,
                        Message = "Failed to create call center",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("CreateCallCenter", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error creating call center {Name}", request.Name);
                
                return StatusCode(500, new ApiResponse<CallCenterDto>
                {
                    Success = false,
                    Message = "An error occurred while creating the call center",
                    Count = 0
                });
            }
        }

        // Attack Point Notes endpoints
        [HttpPut("attackpointnotes/{id}")]
        public async Task<ActionResult<ApiResponse<object>>> UpdateAttackPointNote(int id, [FromBody] UpdateAttackPointNoteRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                if (id != request.Id)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "ID mismatch between URL and request body",
                        Count = 0
                    });
                }
                
                if (string.IsNullOrWhiteSpace(request.Description))
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Description is required",
                        Count = 0
                    });
                }
                
                var success = await _dataService.UpdateAttackPointNoteAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
                    await LogOperationAsync("UpdateAttackPointNote", $"Updated attack point note {id} - {request.Description}", stopwatch.Elapsed);
                    
                    return Ok(new ApiResponse<object>
                    {
                        Success = true,
                        Message = "Attack point note updated successfully",
                        Count = 1
                    });
                }
                else
                {
                    await LogOperationAsync("UpdateAttackPointNote", $"Failed to update attack point note {id}", stopwatch.Elapsed);
                    
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Failed to update attack point note",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("UpdateAttackPointNote", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error updating attack point note {Id}", id);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while updating the attack point note",
                    Count = 0
                });
            }
        }

        [HttpPost("attackpointnotes")]
        public async Task<ActionResult<ApiResponse<AttackPointNoteDto>>> CreateAttackPointNote([FromBody] CreateAttackPointNoteRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Creating new attack point note: {Description}", request.Description);
                
                // Validate the request
                if (string.IsNullOrWhiteSpace(request.Description) || request.Description.Length < 3)
                {
                    return BadRequest(new ApiResponse<AttackPointNoteDto>
                    {
                        Success = false,
                        Message = "Description must be at least 3 characters long",
                        Count = 0
                    });
                }
                
                var newId = await _dataService.CreateAttackPointNoteAsync(request);
                
                if (newId.HasValue)
                {
                    // Create the DTO to return
                    var newAttackPointNote = new AttackPointNoteDto
                    {
                        Id = newId.Value,
                        Description = request.Description,
                        Hours = request.Hours,
                        Attack = request.Attack,
                        InsertDateTime = DateTime.Now,
                        ModifiedDateTime = DateTime.Now
                    };
                    
                    stopwatch.Stop();
                    await LogOperationAsync("CreateAttackPointNote", $"Created attack point note - {request.Description} with ID {newId.Value}", stopwatch.Elapsed);
                    
                    return Ok(new ApiResponse<AttackPointNoteDto>
                    {
                        Success = true,
                        Message = "Attack point note created successfully",
                        Data = newAttackPointNote,
                        Count = 1
                    });
                }
                else
                {
                    stopwatch.Stop();
                    await LogOperationAsync("CreateAttackPointNote", $"Failed to create attack point note - {request.Description}", stopwatch.Elapsed);
                    
                    return BadRequest(new ApiResponse<AttackPointNoteDto>
                    {
                        Success = false,
                        Message = "Failed to create attack point note",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("CreateAttackPointNote", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error creating attack point note {Description}", request.Description);
                
                return StatusCode(500, new ApiResponse<AttackPointNoteDto>
                {
                    Success = false,
                    Message = "An error occurred while creating the attack point note",
                    Count = 0
                });
            }
        }

        // Attack Point Actionable Date endpoints
        [HttpPut("attackpointactionabledates/{id}")]
        public async Task<ActionResult<ApiResponse<object>>> UpdateAttackPointActionableDate(int id, [FromBody] UpdateAttackPointActionableDateRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                if (id != request.Id)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "ID mismatch between URL and request body",
                        Count = 0
                    });
                }
                
                if (string.IsNullOrWhiteSpace(request.Description))
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Description is required",
                        Count = 0
                    });
                }
                
                var success = await _dataService.UpdateAttackPointActionableDateAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
                    await LogOperationAsync("UpdateAttackPointActionableDate", $"Updated attack point actionable date {id} - {request.Description}", stopwatch.Elapsed);
                    
                    return Ok(new ApiResponse<object>
                    {
                        Success = true,
                        Message = "Attack point actionable date updated successfully",
                        Count = 1
                    });
                }
                else
                {
                    await LogOperationAsync("UpdateAttackPointActionableDate", $"Failed to update attack point actionable date {id}", stopwatch.Elapsed);
                    
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Failed to update attack point actionable date",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("UpdateAttackPointActionableDate", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error updating attack point actionable date {Id}", id);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while updating the attack point actionable date",
                    Count = 0
                });
            }
        }

        [HttpPost("attackpointactionabledates")]
        public async Task<ActionResult<ApiResponse<AttackPointActionableDateDto>>> CreateAttackPointActionableDate([FromBody] CreateAttackPointActionableDateRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Creating new attack point actionable date: {Description}", request.Description);
                
                // Validate the request
                if (string.IsNullOrWhiteSpace(request.Description) || request.Description.Length < 3)
                {
                    return BadRequest(new ApiResponse<AttackPointActionableDateDto>
                    {
                        Success = false,
                        Message = "Description must be at least 3 characters long",
                        Count = 0
                    });
                }
                
                var newId = await _dataService.CreateAttackPointActionableDateAsync(request);
                
                if (newId.HasValue)
                {
                    // Create the DTO to return
                    var newActionableDate = new AttackPointActionableDateDto
                    {
                        Id = newId.Value,
                        Description = request.Description,
                        Days = request.Days,
                        Attack = request.Attack,
                        InsertDateTime = DateTime.Now,
                        ModifiedDateTime = DateTime.Now
                    };
                    
                    stopwatch.Stop();
                    await LogOperationAsync("CreateAttackPointActionableDate", $"Created attack point actionable date - {request.Description} with ID {newId.Value}", stopwatch.Elapsed);
                    
                    return Ok(new ApiResponse<AttackPointActionableDateDto>
                    {
                        Success = true,
                        Message = "Attack point actionable date created successfully",
                        Data = newActionableDate,
                        Count = 1
                    });
                }
                else
                {
                    stopwatch.Stop();
                    await LogOperationAsync("CreateAttackPointActionableDate", $"Failed to create attack point actionable date - {request.Description}", stopwatch.Elapsed);
                    
                    return BadRequest(new ApiResponse<AttackPointActionableDateDto>
                    {
                        Success = false,
                        Message = "Failed to create attack point actionable date",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("CreateAttackPointActionableDate", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error creating attack point actionable date {Description}", request.Description);
                
                return StatusCode(500, new ApiResponse<AttackPointActionableDateDto>
                {
                    Success = false,
                    Message = "An error occurred while creating the attack point actionable date",
                    Count = 0
                });
            }
        }

        // Attack Point Status endpoints
        [HttpPut("attackpointstatus/{id}")]
        public async Task<ActionResult<ApiResponse<object>>> UpdateAttackPointStatus(int id, [FromBody] UpdateAttackPointStatusRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                if (id != request.Id)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "ID mismatch between URL and request body",
                        Count = 0
                    });
                }
                
                if (request.DaysInStatus < 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Days in status must be non-negative",
                        Count = 0
                    });
                }
                
                var success = await _dataService.UpdateAttackPointStatusAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
                    await LogOperationAsync("UpdateAttackPointStatus", $"Updated attack point status {id} - {request.DaysInStatus} days", stopwatch.Elapsed);
                    
                    return Ok(new ApiResponse<object>
                    {
                        Success = true,
                        Message = "Attack point status updated successfully",
                        Count = 1
                    });
                }
                else
                {
                    await LogOperationAsync("UpdateAttackPointStatus", $"Failed to update attack point status {id}", stopwatch.Elapsed);
                    
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Failed to update attack point status",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("UpdateAttackPointStatus", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error updating attack point status {Id}", id);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while updating the attack point status",
                    Count = 0
                });
            }
        }

        [HttpPost("attackpointstatus")]
        public async Task<ActionResult<ApiResponse<AttackPointStatusDto>>> CreateAttackPointStatus([FromBody] CreateAttackPointStatusRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Creating new attack point status: {DaysInStatus} days", request.DaysInStatus);
                
                // Validate the request
                if (request.DaysInStatus < 0)
                {
                    return BadRequest(new ApiResponse<AttackPointStatusDto>
                    {
                        Success = false,
                        Message = "Days in status must be non-negative",
                        Count = 0
                    });
                }
                
                var newId = await _dataService.CreateAttackPointStatusAsync(request);
                
                if (newId.HasValue)
                {
                    // Create the DTO to return
                    var newAttackPointStatus = new AttackPointStatusDto
                    {
                        Id = newId.Value,
                        DaysInStatus = request.DaysInStatus,
                        Attack = request.Attack,
                        InsertDateTime = DateTime.Now,
                        ModifiedDateTime = DateTime.Now
                    };
                    
                    stopwatch.Stop();
                    await LogOperationAsync("CreateAttackPointStatus", $"Created attack point status - {request.DaysInStatus} days with ID {newId.Value}", stopwatch.Elapsed);
                    
                    return Ok(new ApiResponse<AttackPointStatusDto>
                    {
                        Success = true,
                        Message = "Attack point status created successfully",
                        Data = newAttackPointStatus,
                        Count = 1
                    });
                }
                else
                {
                    stopwatch.Stop();
                    await LogOperationAsync("CreateAttackPointStatus", $"Failed to create attack point status - {request.DaysInStatus} days", stopwatch.Elapsed);
                    
                    return BadRequest(new ApiResponse<AttackPointStatusDto>
                    {
                        Success = false,
                        Message = "Failed to create attack point status",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("CreateAttackPointStatus", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error creating attack point status {DaysInStatus}", request.DaysInStatus);
                
                return StatusCode(500, new ApiResponse<AttackPointStatusDto>
                {
                    Success = false,
                    Message = "An error occurred while creating the attack point status",
                    Count = 0
                });
            }
        }

        [HttpDelete("statusassignments")]
        public async Task<ActionResult<ApiResponse<object>>> DeleteStatusAssignment([FromBody] DeleteAdminZoneStatusAssignmentRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Deleting status assignment for User {UserId}, Zone {ZoneId}, Status {StatusSecondaryId}", 
                    request.UserId, request.ZoneId, request.StatusSecondaryId);
                
                // Validate input
                if (request.UserId <= 0 || request.ZoneId <= 0 || request.StatusSecondaryId <= 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Valid User ID, Zone ID, and Status Secondary ID are required",
                        Count = 0
                    });
                }

                // Delete assignment
                var success = await _dataService.DeleteAdminZoneStatusAssignmentAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
                    await LogOperationAsync("DeleteStatusAssignment", 
                        $"Deleted assignment for User {request.UserId}, Zone {request.ZoneId}, Status {request.StatusSecondaryId}", 
                        stopwatch.Elapsed);
                    
                    return Ok(new ApiResponse<object>
                    {
                        Success = true,
                        Message = "Status assignment deleted successfully",
                        Count = 0
                    });
                }
                else
                {
                    await LogOperationAsync("DeleteStatusAssignment", 
                        $"Failed to delete assignment for User {request.UserId}, Zone {request.ZoneId}, Status {request.StatusSecondaryId}", 
                        stopwatch.Elapsed);
                    
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Failed to delete status assignment",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("DeleteStatusAssignment", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error deleting status assignment for User {UserId}, Zone {ZoneId}, Status {StatusSecondaryId}", 
                    request.UserId, request.ZoneId, request.StatusSecondaryId);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while deleting the status assignment",
                    Count = 0
                });
            }
        }

        [HttpPut("workorders/{id}/escalated")]
        public async Task<ActionResult<ApiResponse<object>>> UpdateWorkOrderEscalated(int id, [FromBody] UpdateWorkOrderEscalatedRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Updating escalated status for work order {Id} to {IsEscalated}", id, request.IsEscalated);
                
                // Validate input
                if (id != request.ServiceRequestId)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "ID in URL does not match ID in request body",
                        Count = 0
                    });
                }

                // Update escalated status
                var success = await _dataService.UpdateWorkOrderEscalatedAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
                    var action = request.IsEscalated ? "escalated" : "un-escalated";
                    await LogOperationAsync("UpdateWorkOrderEscalated", $"Work order {id} {action}", stopwatch.Elapsed);
        
                    return Ok(new ApiResponse<object>
                    {
                        Success = true,
                        Message = $"Work order {action} successfully",
                        Count = 1
                    });
                }
                else
                {
                    return NotFound(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Work order not found",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("UpdateWorkOrderEscalated", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error updating escalated status for work order {Id}", id);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while updating the work order escalated status",
                    Count = 0
                });
            }
        }

        [HttpPut("workorders/{id}/schedulelock")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<object>>> UpdateWorkOrderScheduleLock(int id, [FromBody] UpdateWorkOrderScheduleLockRequest request)
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Updating schedule lock status for work order {Id} to {IsScheduleLocked}", id, request.IsScheduleLocked);
                
                // Validate input
                if (id != request.ServiceRequestId)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "ID in URL does not match ID in request body",
                        Count = 0
                    });
                }

                // Update schedule lock status
                var success = await _dataService.UpdateWorkOrderScheduleLockAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
                    var action = request.IsScheduleLocked ? "locked" : "unlocked";
                    await LogOperationAsync("UpdateWorkOrderScheduleLock", $"Work order {id} schedule {action}", stopwatch.Elapsed);
        
                    return Ok(new ApiResponse<object>
                    {
                        Success = true,
                        Message = $"Work order schedule {action} successfully",
                        Count = 1
                    });
                }
                else
                {
                    return NotFound(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Work order not found",
                        Count = 0
                    });
                }
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogErrorAsync("UpdateWorkOrderScheduleLock", ex, stopwatch.Elapsed);
                
                _logger.LogError(ex, "Error updating schedule lock status for work order {Id}", id);
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "An error occurred while updating the work order schedule lock status",
                    Count = 0
                });
            }
        }
    #endregion


    #region Helper Methods

    private static string CleanString(object value)
    {
        return value?.ToString()?.Trim()?.Replace("\r\n", "").Replace("\n", "").Replace("\r", "") ?? string.Empty;
    }

    private List<WorkOrderDto> ConvertDataTableToWorkOrders(DataTable dataTable)
    {
        var workOrders = new List<WorkOrderDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var workOrder = new WorkOrderDto
            {
                sr_id = Convert.ToInt32(row["sr_id"]),
                wo_id = row["wo_id"] != DBNull.Value ? Convert.ToInt32(row["wo_id"]) : (int?)null,
                CreateDate = row["CreateDate"]?.ToString() ?? string.Empty,
                CallCenter = CleanString(row["CallCenter"]),
                Company = CleanString(row["Company"]),
                Trade = CleanString(row["Trade"]),
                StartDate = row["StartDate"]?.ToString() ?? string.Empty,
                EndDate = row["EndDate"]?.ToString() ?? string.Empty,
                RequestNumber = CleanString(row["RequestNumber"]),
                TotalDue = row["TotalDue"] != DBNull.Value ? Convert.ToDecimal(row["TotalDue"]) : null,
                Priority = CleanString(row["Priority"]),
                PriorityColor = CleanString(row["PriorityColor"]),
                Status = CleanString(row["Status"]),
                SecondaryStatus = CleanString(row["SecondaryStatus"]),
                StatusColor = CleanString(row["StatusColor"]),
                AssignedFirstName = CleanString(row["AssignedFirstName"]),
                AssignedLastName = CleanString(row["AssignedLastName"]),
                Location = CleanString(row["Location"]),
                Address = CleanString(row["Address"]),
                City = CleanString(row["City"]),
                State = CleanString(row["State"]),
                Zip = CleanString(row["Zip"]),
                Zone = CleanString(row["Zone"]),
                CreatedBy = CleanString(row["CreatedBy"]),
                Escalated = row["Escalated"] != DBNull.Value ? Convert.ToDateTime(row["Escalated"]) : null,
                ScheduleLock = row["ScheduleLock"] != DBNull.Value && Convert.ToBoolean(row["ScheduleLock"]),
                ActionableNote = CleanString(row["ActionableNote"])
            };

            workOrders.Add(workOrder);
        }

        return workOrders;
    }

    private static List<PriorityDto> ConvertDataTableToPriorities(DataTable dataTable)
    {
        var priorities = new List<PriorityDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var priority = new PriorityDto
            {
                Id = Convert.ToInt32(row["Id"]),
                InsertDateTime = Convert.ToDateTime(row["InsertDateTime"]),
                ModifiedDateTime = row["ModifiedDateTime"] != DBNull.Value ? Convert.ToDateTime(row["ModifiedDateTime"]) : null,
                PriorityName = CleanString(row["PriorityName"]),
                Order = row["Order"] != DBNull.Value ? Convert.ToInt32(row["Order"]) : null,
                Color = CleanString(row["Color"]),
                ArrivalTimeInHours = row["ArrivalTimeInHours"] != DBNull.Value ? Convert.ToDecimal(row["ArrivalTimeInHours"]) : null,
                Attack = Convert.ToInt32(row["Attack"])
            };

            priorities.Add(priority);
        }

        return priorities;
    }

    private static List<StatusSecondaryDto> ConvertDataTableToStatusSecondaries(DataTable dataTable)
    {
        var statusSecondaries = new List<StatusSecondaryDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var statusSecondary = new StatusSecondaryDto
            {
                Id = Convert.ToInt32(row["Id"]),
                InsertDateTime = Convert.ToDateTime(row["InsertDateTime"]),
                ModifiedDateTime = row["ModifiedDateTime"] != DBNull.Value ? Convert.ToDateTime(row["ModifiedDateTime"]) : null,
                StatusId = Convert.ToInt32(row["StatusId"]),
                StatusSecondary = CleanString(row["StatusSecondary"]),
                Color = CleanString(row["Color"]),
                Code = CleanString(row["Code"]),
                Attack = row["Attack"] != DBNull.Value ? Convert.ToInt32(row["Attack"]) : 0
            };

            statusSecondaries.Add(statusSecondary);
        }

        return statusSecondaries;
    }

    private static List<CallCenterDto> ConvertDataTableToCallCenters(DataTable dataTable)
    {
        var callCenters = new List<CallCenterDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var callCenter = new CallCenterDto
            {
                Id = Convert.ToInt32(row["Id"]),
                OId = Convert.ToInt32(row["OId"]),
                InsertDateTime = Convert.ToDateTime(row["InsertDateTime"]),
                ModifiedDateTime = row["ModifiedDateTime"] != DBNull.Value ? Convert.ToDateTime(row["ModifiedDateTime"]) : null,
                Name = row["Name"]?.ToString() ?? string.Empty,
                Active = Convert.ToBoolean(row["Active"]),
                TempId = row["TempId"]?.ToString(),
                Note = row["Note"]?.ToString(),
                Attack = Convert.ToInt32(row["Attack"]),
                PortalUrl = row["PortalUrl"]?.ToString(),
                PortalName = row["PortalName"]?.ToString(),
                PortalCredentials = row["PortalCredentials"]?.ToString()
            };

            callCenters.Add(callCenter);
        }

        return callCenters;
    }

    private static List<UserAttachmentTypeDto> ConvertDataTableToUserAttachmentTypes(DataTable dataTable)
    {
        var attachmentTypes = new List<UserAttachmentTypeDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var attachmentType = new UserAttachmentTypeDto
            {
                uat_id = Convert.ToInt32(row["uat_id"]),
                uat_insertdatetime = Convert.ToDateTime(row["uat_insertdatetime"]),
                uat_modifieddatetime = row["uat_modifieddatetime"] != DBNull.Value ? Convert.ToDateTime(row["uat_modifieddatetime"]) : null,
                uat_type = row["uat_type"]?.ToString() ?? string.Empty
            };

            attachmentTypes.Add(attachmentType);
        }

        return attachmentTypes;
    }

    private static List<UserClothingSizeDto> ConvertDataTableToUserClothingSizes(DataTable dataTable)
    {
        var clothingSizes = new List<UserClothingSizeDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            clothingSizes.Add(new UserClothingSizeDto
            {
                Id = Convert.ToInt32(row["uc_id"]),
                ClothingSize = row["uc_clothingsize"]?.ToString() ?? string.Empty
            });
        }

        return clothingSizes;
    }

    private static List<UserPantsWaistDto> ConvertDataTableToUserPantsWaist(DataTable dataTable)
    {
        var pantsWaist = new List<UserPantsWaistDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            pantsWaist.Add(new UserPantsWaistDto
            {
                Id = Convert.ToInt32(row["upw_id"]),
                Size = row["upw_size"]?.ToString() ?? string.Empty,
                Sex = row["upw_sex"]?.ToString() ?? string.Empty
            });
        }

        return pantsWaist;
    }

    private static List<UserPantsLengthDto> ConvertDataTableToUserPantsLength(DataTable dataTable)
    {
        var pantsLength = new List<UserPantsLengthDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            pantsLength.Add(new UserPantsLengthDto
            {
                Id = Convert.ToInt32(row["upl_id"]),
                Size = row["upl_size"]?.ToString() ?? string.Empty,
                Sex = row["upl_sex"]?.ToString() ?? string.Empty
            });
        }

        return pantsLength;
    }

    private static List<UserRelationshipDto> ConvertDataTableToUserRelationships(DataTable dataTable)
    {
        var relationships = new List<UserRelationshipDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            relationships.Add(new UserRelationshipDto
            {
                Id = Convert.ToInt32(row["ur_id"]),
                Relationship = row["ur_relationship"]?.ToString() ?? string.Empty
            });
        }

        return relationships;
    }

    private static List<AttackPointNoteDto> ConvertDataTableToAttackPointNotes(DataTable dataTable)
    {
        var attackPointNotes = new List<AttackPointNoteDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var attackPointNote = new AttackPointNoteDto
            {
                Id = Convert.ToInt32(row["Id"]),
                InsertDateTime = Convert.ToDateTime(row["InsertDateTime"]),
                ModifiedDateTime = row["ModifiedDateTime"] != DBNull.Value ? Convert.ToDateTime(row["ModifiedDateTime"]) : null,
                Description = row["Description"]?.ToString() ?? string.Empty,
                Hours = Convert.ToInt32(row["Hours"]),
                Attack = Convert.ToInt32(row["Attack"])
            };

            attackPointNotes.Add(attackPointNote);
        }

        return attackPointNotes;
    }

    private static List<AttackPointActionableDateDto> ConvertDataTableToAttackPointActionableDates(DataTable dataTable)
    {
        var actionableDates = new List<AttackPointActionableDateDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var actionableDate = new AttackPointActionableDateDto
            {
                Id = Convert.ToInt32(row["Id"]),
                InsertDateTime = Convert.ToDateTime(row["InsertDateTime"]),
                ModifiedDateTime = row["ModifiedDateTime"] != DBNull.Value ? Convert.ToDateTime(row["ModifiedDateTime"]) : null,
                Description = row["Description"]?.ToString() ?? string.Empty,
                Days = Convert.ToInt32(row["Days"]),
                Attack = Convert.ToInt32(row["Attack"])
            };

            actionableDates.Add(actionableDate);
        }

        return actionableDates;
    }

    private static List<AttackPointStatusDto> ConvertDataTableToAttackPointStatus(DataTable dataTable)
    {
        var attackPointStatus = new List<AttackPointStatusDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var attackPointStatusItem = new AttackPointStatusDto
            {
                Id = Convert.ToInt32(row["Id"]),
                InsertDateTime = Convert.ToDateTime(row["InsertDateTime"]),
                ModifiedDateTime = row["ModifiedDateTime"] != DBNull.Value ? Convert.ToDateTime(row["ModifiedDateTime"]) : null,
                DaysInStatus = Convert.ToInt32(row["DaysInStatus"]),
                Attack = Convert.ToInt32(row["Attack"])
            };

            attackPointStatus.Add(attackPointStatusItem);
        }

        return attackPointStatus;
    }

    private static List<ZoneDto> ConvertDataTableToZones(DataTable dataTable)
    {
        var zones = new List<ZoneDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var zone = new ZoneDto
            {
                Id = Convert.ToInt32(row["Id"]),
                InsertDateTime = Convert.ToDateTime(row["InsertDateTime"]),
                ModifiedDateTime = row["ModifiedDateTime"] != DBNull.Value ? Convert.ToDateTime(row["ModifiedDateTime"]) : null,
                Number = row["Number"]?.ToString() ?? string.Empty,
                Description = row["Description"]?.ToString(),
                Acronym = row["Acronym"]?.ToString(),
                UserId = row["UserId"] != DBNull.Value ? Convert.ToInt32(row["UserId"]) : 0
            };

            zones.Add(zone);
        }

        return zones;
    }

    private static List<UserDto> ConvertDataTableToUsers(DataTable dataTable)
    {
        var users = new List<UserDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var user = new UserDto
            {
                Id = Convert.ToInt32(row["Id"]),
                OId = Convert.ToInt32(row["OId"]),
                AId = row["AId"] != DBNull.Value ? Convert.ToInt32(row["AId"]) : null,
                VId = row["VId"] != DBNull.Value ? Convert.ToInt32(row["VId"]) : null,
                SupervisorId = row["SupervisorId"] != DBNull.Value ? Convert.ToInt32(row["SupervisorId"]) : null,
                InsertDateTime = Convert.ToDateTime(row["InsertDateTime"]),
                ModifiedDateTime = row["ModifiedDateTime"] != DBNull.Value ? Convert.ToDateTime(row["ModifiedDateTime"]) : null,
                Username = row["Username"]?.ToString() ?? string.Empty,
                Password = dataTable.Columns.Contains("Password") ? row["Password"]?.ToString() ?? string.Empty : string.Empty,
                FirstName = row["FirstName"]?.ToString(),
                LastName = row["LastName"]?.ToString(),
                EmployeeNumber = dataTable.Columns.Contains("EmployeeNumber") ? row["EmployeeNumber"]?.ToString() : null,
                Email = row["Email"]?.ToString(),
                PhoneHome = dataTable.Columns.Contains("PhoneHome") ? row["PhoneHome"]?.ToString() : null,
                PhoneMobile = dataTable.Columns.Contains("PhoneMobile") ? row["PhoneMobile"]?.ToString() : null,
                PhoneDesk = dataTable.Columns.Contains("PhoneDesk") ? row["PhoneDesk"]?.ToString() : null,
                Extension = dataTable.Columns.Contains("Extension") ? row["Extension"]?.ToString() : null,
                Active = Convert.ToBoolean(row["Active"]),
                Picture = dataTable.Columns.Contains("Picture") ? row["Picture"]?.ToString() : null,
                SSN = dataTable.Columns.Contains("SSN") ? row["SSN"]?.ToString() : null,
                DateOfHire = dataTable.Columns.Contains("DateOfHire") && row["DateOfHire"] != DBNull.Value ? Convert.ToDateTime(row["DateOfHire"]) : null,
                DateEligiblePTO = dataTable.Columns.Contains("DateEligiblePTO") && row["DateEligiblePTO"] != DBNull.Value ? Convert.ToDateTime(row["DateEligiblePTO"]) : null,
                DateEligibleVacation = dataTable.Columns.Contains("DateEligibleVacation") && row["DateEligibleVacation"] != DBNull.Value ? Convert.ToDateTime(row["DateEligibleVacation"]) : null,
                DaysAvailablePTO = dataTable.Columns.Contains("DaysAvailablePTO") && row["DaysAvailablePTO"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailablePTO"]) : null,
                DaysAvailableVacation = dataTable.Columns.Contains("DaysAvailableVacation") && row["DaysAvailableVacation"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailableVacation"]) : null,
                ClothingShirt = dataTable.Columns.Contains("ClothingShirt") ? row["ClothingShirt"]?.ToString() : null,
                ClothingJacket = dataTable.Columns.Contains("ClothingJacket") ? row["ClothingJacket"]?.ToString() : null,
                ClothingPants = dataTable.Columns.Contains("ClothingPants") ? row["ClothingPants"]?.ToString() : null,
                WirelessProvider = dataTable.Columns.Contains("WirelessProvider") ? row["WirelessProvider"]?.ToString() : null,
                PreferredNotification = dataTable.Columns.Contains("PreferredNotification") ? row["PreferredNotification"]?.ToString() : null,
                QuickBooksName = dataTable.Columns.Contains("QuickBooksName") ? row["QuickBooksName"]?.ToString() : null,
                PasswordChanged = dataTable.Columns.Contains("PasswordChanged") && row["PasswordChanged"] != DBNull.Value ? Convert.ToDateTime(row["PasswordChanged"]) : null,
                U_2FA = dataTable.Columns.Contains("U_2FA") && row["U_2FA"] != DBNull.Value ? Convert.ToBoolean(row["U_2FA"]) : false,
                ZoneId = row["ZoneId"] != DBNull.Value ? Convert.ToInt32(row["ZoneId"]) : null,
                CovidVaccineDate = dataTable.Columns.Contains("CovidVaccineDate") && row["CovidVaccineDate"] != DBNull.Value ? Convert.ToDateTime(row["CovidVaccineDate"]) : null,
                Note = dataTable.Columns.Contains("Note") ? row["Note"]?.ToString() : null,
                NoteDashboard = dataTable.Columns.Contains("NoteDashboard") ? row["NoteDashboard"]?.ToString() : null
            };

            users.Add(user);
        }

        return users;
    }

    private static List<TechnicianDto> ConvertDataTableToTechnicians(DataTable dataTable)
    {
        var technicians = new List<TechnicianDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var technician = new TechnicianDto
            {
                Id = Convert.ToInt32(row["Id"]),
                EmployeeNumber = row["EmployeeNumber"]?.ToString(),
                FirstName = row["FirstName"]?.ToString(),
                LastName = row["LastName"]?.ToString(),
                Username = row["Username"]?.ToString() ?? string.Empty,
                Email = row["Email"]?.ToString(),
                Picture = row["Picture"]?.ToString(),
                PhoneMobile = row["PhoneMobile"]?.ToString(),
                Address1 = row["Address1"] == DBNull.Value ? null : row["Address1"]?.ToString(),
                Address2 = row["Address2"] == DBNull.Value ? null : row["Address2"]?.ToString(),
                City = row["City"] == DBNull.Value ? null : row["City"]?.ToString(),
                State = row["State"] == DBNull.Value ? null : row["State"]?.ToString(),
                Zip = row["Zip"] == DBNull.Value ? null : row["Zip"]?.ToString()
            };

            technicians.Add(technician);
        }

        return technicians;
    }

    private static List<AdminZoneStatusAssignmentDto> ConvertDataTableToAdminZoneStatusAssignments(DataTable dataTable)
    {
        var assignments = new List<AdminZoneStatusAssignmentDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var assignment = new AdminZoneStatusAssignmentDto
            {
                Id = Convert.ToInt32(row["Id"]),
                InsertDateTime = Convert.ToDateTime(row["InsertDateTime"]),
                ModifiedDateTime = row["ModifiedDateTime"] != DBNull.Value ? Convert.ToDateTime(row["ModifiedDateTime"]) : null,
                UserId = Convert.ToInt32(row["UserId"]),
                ZoneId = Convert.ToInt32(row["ZoneId"]),
                StatusSecondaryId = Convert.ToInt32(row["StatusSecondaryId"]),
                UserDisplayName = row["UserDisplayName"]?.ToString(),
                ZoneName = row["ZoneName"]?.ToString(),
                StatusSecondaryName = row["StatusSecondaryName"]?.ToString()
            };

            assignments.Add(assignment);
        }

        return assignments;
    }

    private static List<AttackPointDto> ConvertDataTableToAttackPoints(DataTable dataTable)
    {
        var attackPoints = new List<AttackPointDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var attackPoint = new AttackPointDto
            {
                sr_id = Convert.ToInt32(row["sr_id"]),
                sr_requestnumber = CleanString(row["sr_requestnumber"]),
                sr_insertdatetime = Convert.ToDateTime(row["sr_insertdatetime"]),
                sr_totaldue = row["sr_totaldue"] != DBNull.Value ? Convert.ToDecimal(row["sr_totaldue"]) : null,
                sr_datenextstep = row["sr_datenextstep"] != DBNull.Value ? Convert.ToDateTime(row["sr_datenextstep"]) : null,
                sr_actionablenote = CleanString(row["sr_actionablenote"]),
                sr_escalated = row["sr_escalated"] != DBNull.Value ? Convert.ToDateTime(row["sr_escalated"]) : null,
                wo_startdatetime = row["wo_startdatetime"] != DBNull.Value ? Convert.ToDateTime(row["wo_startdatetime"]) : null,
                zone = CleanString(row["zone"]),
                admin_u_id = row["admin_u_id"] != DBNull.Value ? Convert.ToInt32(row["admin_u_id"]) : null,
                admin_firstname = CleanString(row["admin_firstname"]),
                admin_lastname = CleanString(row["admin_lastname"]),
                cc_name = CleanString(row["cc_name"]),
                c_name = CleanString(row["c_name"]),
                p_priority = CleanString(row["p_priority"]),
                ss_statussecondary = CleanString(row["ss_statussecondary"]),
                t_trade = CleanString(row["t_trade"]),
                hours_since_last_note = row["hours_since_last_note"] != DBNull.Value ? Convert.ToInt32(row["hours_since_last_note"]) : 0,
                days_in_current_status = row["days_in_current_status"] != DBNull.Value ? Convert.ToInt32(row["days_in_current_status"]) : 0,
                AttackCallCenter = row["AttackCallCenter"] != DBNull.Value ? Convert.ToInt32(row["AttackCallCenter"]) : 0,
                AttackPriority = row["AttackPriority"] != DBNull.Value ? Convert.ToInt32(row["AttackPriority"]) : 0,
                AttackStatusSecondary = row["AttackStatusSecondary"] != DBNull.Value ? Convert.ToInt32(row["AttackStatusSecondary"]) : 0,
                AttackHoursSinceLastNote = row["AttackHoursSinceLastNote"] != DBNull.Value ? Convert.ToInt32(row["AttackHoursSinceLastNote"]) : 0,
                AttackDaysInStatus = row["AttackDaysInStatus"] != DBNull.Value ? Convert.ToInt32(row["AttackDaysInStatus"]) : 0,
                AttackActionableDate = row["AttackActionableDate"] != DBNull.Value ? Convert.ToInt32(row["AttackActionableDate"]) : 0,
                AttackPoints = row["AttackPoints"] != DBNull.Value ? Convert.ToInt32(row["AttackPoints"]) : 0
            };

            attackPoints.Add(attackPoint);
        }

        return attackPoints;
    }

    private async Task LogOperationAsync(string operation, string detail, TimeSpan elapsed)
    {
        await LogAuditAsync(operation, detail, elapsed.TotalSeconds.ToString("F3"));
    }

    private async Task LogErrorAsync(string operation, Exception ex, TimeSpan elapsed)
    {
        await LogAuditErrorAsync(operation, ex, new { ResponseTime = elapsed.TotalSeconds.ToString("F3") });
    }

    private static List<AttachmentDto> ConvertDataTableToAttachments(DataTable dataTable)
    {
        var attachments = new List<AttachmentDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var attachment = new AttachmentDto
            {
                att_id = Convert.ToInt32(row["att_id"]),
                cc_id = row.Table.Columns.Contains("cc_id") && row["cc_id"] != DBNull.Value ? Convert.ToInt32(row["cc_id"]) : 0,
                att_insertdatetime = Convert.ToDateTime(row["att_insertdatetime"]),
                att_filename = CleanString(row["att_filename"]),
                att_description = CleanString(row["att_description"]),
                att_active = Convert.ToBoolean(row["att_active"]),
                att_receipt = Convert.ToBoolean(row["att_receipt"]),
                att_public = Convert.ToBoolean(row["att_public"]),
                att_signoff = Convert.ToBoolean(row["att_signoff"]),
                att_submittedby = CleanString(row["att_submittedby"]),
                att_receiptamount = row["att_receiptamount"] != DBNull.Value ? Convert.ToDecimal(row["att_receiptamount"]) : null,
                sr_id = Convert.ToInt32(row["sr_id"])
            };

            attachments.Add(attachment);
        }

        return attachments;
    }

    private static List<PendingTechInfoDto> ConvertDataTableToPendingTechInfo(DataTable dataTable)
    {
        var pendingTechInfo = new List<PendingTechInfoDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var info = new PendingTechInfoDto
            {
                sr_id = ConvertToInt(row["sr_id"]),
                xwou_id = ConvertToInt(row["xwou_id"]),
                sr_requestnumber = CleanString(row["sr_requestnumber"]),
                u_firstname = CleanString(row["u_firstname"]),
                u_lastname = CleanString(row["u_lastname"]),
                wo_insertdatetime = ConvertToDateTimeString(row["wo_insertdatetime"]),
                t_trade = CleanString(row["t_trade"]),
                c_name = CleanString(row["c_name"]),
                wo_startdatetime = ConvertToDateTimeString(row["wo_startdatetime"])
            };

            pendingTechInfo.Add(info);
        }

        return pendingTechInfo;
    }

    private static int ConvertToInt(object value)
    {
        if (value == null || value == DBNull.Value)
            return 0;
        
        if (int.TryParse(value.ToString(), out var result))
            return result;
            
        return 0;
    }

    private static DateTime ConvertToDateTime(object value)
    {
        if (value == null || value == DBNull.Value)
            return DateTime.MinValue;
        
        if (DateTime.TryParse(value.ToString(), out var result))
            return result;
            
        return DateTime.MinValue;
    }

    private static string ConvertToDateTimeString(object value)
    {
        if (value == null || value == DBNull.Value)
            return string.Empty;
        
        if (DateTime.TryParse(value.ToString(), out var result))
        {
            // Assume database datetimes are stored in UTC and need to be converted to Central Time
            DateTime localTime;
            try
            {
                // Treat the datetime as UTC and convert to Central Time
                var utcTime = DateTime.SpecifyKind(result, DateTimeKind.Utc);
                var centralTimeZone = TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time");
                localTime = TimeZoneInfo.ConvertTimeFromUtc(utcTime, centralTimeZone);
            }
            catch
            {
                localTime = result; // Fallback to original value
            }
            
            // Return in the same format as work orders API: "YYYY-MM-DD HH:mm"
            return localTime.ToString("yyyy-MM-dd HH:mm");
        }
            
        return string.Empty;
    }

    #endregion

    #region Missing Receipts

    [HttpGet("missing-receipts")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<List<MissingReceiptDashboardDto>>>> GetMissingReceipts()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Getting missing receipts for all users");
            
            var receipts = await _dataService.GetMissingReceiptsAsync();
            
            stopwatch.Stop();
            // await LogOperationAsync("GetMissingReceipts", $"Retrieved {receipts.Count} missing receipts", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<MissingReceiptDashboardDto>>
            {
                Success = true,
                Message = "Missing receipts retrieved successfully",
                Data = receipts,
                Count = receipts.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetMissingReceipts", ex);
            
            return StatusCode(500, new ApiResponse<List<MissingReceiptDashboardDto>>
            {
                Success = false,
                Message = "Failed to retrieve missing receipts"
            });
        }
    }

    [HttpGet("missing-receipts/user")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<MissingReceiptDashboardDto>>>> GetMissingReceiptsByUser()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Getting missing receipts for user {UserId}", UserId);
            
            var receipts = await _dataService.GetMissingReceiptsByUserAsync(UserId);
            
            stopwatch.Stop();
            // await LogAuditAsync("GetMissingReceiptsByUser", $"Retrieved {receipts.Count} missing receipts for user {UserId}", stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
            
            return Ok(new ApiResponse<List<MissingReceiptDashboardDto>>
            {
                Success = true,
                Message = "Missing receipts retrieved successfully",
                Data = receipts,
                Count = receipts.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetMissingReceiptsByUser", ex);
            
            return StatusCode(500, new ApiResponse<List<MissingReceiptDashboardDto>>
            {
                Success = false,
                Message = "Failed to retrieve missing receipts"
            });
        }
    }

    [HttpPost("missing-receipts/upload")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<int>>> UploadMissingReceipts([FromBody] List<MissingReceiptUploadDto> receipts)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Uploading {Count} missing receipts for user {UserId}", receipts.Count, UserId);
            
            if (!receipts.Any())
            {
                return BadRequest(new ApiResponse<int>
                {
                    Success = false,
                    Message = "No receipt data provided"
                });
            }

            var uploadedCount = await _dataService.UploadMissingReceiptsAsync(receipts);
            
            stopwatch.Stop();
            await LogOperationAsync("UploadMissingReceipts", $"Uploaded {uploadedCount} missing receipts", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<int>
            {
                Success = true,
                Message = $"Successfully uploaded {uploadedCount} missing receipts",
                Data = uploadedCount,
                Count = uploadedCount
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UploadMissingReceipts", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<int>
            {
                Success = false,
                Message = "Failed to upload missing receipts"
            });
        }
    }

    #endregion

    #region Vehicle Maintenance

    [HttpGet("vehicle-maintenance")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<List<VehicleMaintenanceDto>>>> GetVehicleMaintenanceRecords()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Getting vehicle maintenance records for today");
            
            var records = await _dataService.GetVehicleMaintenanceRecordsAsync();
            
            stopwatch.Stop();
            await LogAuditAsync("GetVehicleMaintenanceRecords", $"Retrieved {records.Count} vehicle maintenance records", stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
            
            return Ok(new ApiResponse<List<VehicleMaintenanceDto>>
            {
                Success = true,
                Message = "Vehicle maintenance records retrieved successfully",
                Data = records,
                Count = records.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetVehicleMaintenanceRecords", ex);
            
            return StatusCode(500, new ApiResponse<List<VehicleMaintenanceDto>>
            {
                Success = false,
                Message = "Failed to retrieve vehicle maintenance records"
            });
        }
    }

    [HttpPost("vehicle-maintenance/upload")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<int>>> UploadVehicleMaintenanceRecords([FromBody] List<VehicleMaintenanceUploadDto> records)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Uploading {Count} vehicle maintenance records for user {UserId}", records.Count, UserId);
            
            if (!records.Any())
            {
                return BadRequest(new ApiResponse<int>
                {
                    Success = false,
                    Message = "No vehicle maintenance data provided"
                });
            }

            var uploadedCount = await _dataService.UploadVehicleMaintenanceRecordsAsync(records);
            
            stopwatch.Stop();
            await LogAuditAsync("UploadVehicleMaintenanceRecords", $"Uploaded {uploadedCount} vehicle maintenance records", stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
            
            return Ok(new ApiResponse<int>
            {
                Success = true,
                Message = $"Successfully uploaded {uploadedCount} vehicle maintenance records",
                Data = uploadedCount,
                Count = uploadedCount
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("UploadVehicleMaintenanceRecords", ex);
            
            return StatusCode(500, new ApiResponse<int>
            {
                Success = false,
                Message = "Failed to upload vehicle maintenance records"
            });
        }
    }

    [HttpGet("vehicle-maintenance/{employeeNumber}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<VehicleMaintenanceDto>>> GetVehicleMaintenanceByTechnician(string employeeNumber)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Getting vehicle maintenance for employee number: {EmployeeNumber}", employeeNumber);
            
            var vehicleData = await _dataService.GetVehicleMaintenanceByEmployeeNumberAsync(employeeNumber);
            
            stopwatch.Stop();
            await LogAuditAsync("GetVehicleMaintenanceByTechnician", $"Retrieved vehicle maintenance for employee {employeeNumber}", stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
            
            return Ok(new ApiResponse<VehicleMaintenanceDto>
            {
                Success = true,
                Message = "Vehicle maintenance data retrieved successfully",
                Data = vehicleData,
                Count = vehicleData != null ? 1 : 0
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetVehicleMaintenanceByTechnician", ex);
            
            return StatusCode(500, new ApiResponse<VehicleMaintenanceDto>
            {
                Success = false,
                Message = "Failed to retrieve vehicle maintenance data"
            });
        }
    }

    #endregion

    #region Driving Scorecard

    /// <summary>
    /// Get driving scorecard data for a specific technician
    /// </summary>
    /// <param name="userId">User ID of the technician</param>
    /// <returns>Driving scorecard data for the past 7 days</returns>
    [HttpGet("driving-scorecard/{userId}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<DrivingScorecard>>> GetDrivingScorecard(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var drivingData = await _dataService.GetDrivingScorecardAsync(userId);
            stopwatch.Stop();
            
            await LogOperationAsync("GetDrivingScorecard", $"Retrieved driving scorecard for user {userId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<DrivingScorecard>
            {
                Success = true,
                Message = "Driving scorecard retrieved successfully",
                Data = drivingData,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetDrivingScorecard", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<DrivingScorecard>
            {
                Success = false,
                Message = "Failed to retrieve driving scorecard"
            });
        }
    }

    [HttpGet("driving-scorecards")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<List<DrivingScorecardWithTechnicianInfo>>>> GetAllDrivingScorecard()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var drivingData = await _dataService.GetAllDrivingScorecardsAsync();
            stopwatch.Stop();
            
            await LogOperationAsync("GetAllDrivingScorecard", $"Retrieved driving scorecards for all technicians. Count: {drivingData.Count}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<DrivingScorecardWithTechnicianInfo>>
            {
                Success = true,
                Message = "All driving scorecards retrieved successfully",
                Data = drivingData,
                Count = drivingData.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetAllDrivingScorecard", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<DrivingScorecardWithTechnicianInfo>>
            {
                Success = false,
                Message = "Failed to retrieve all driving scorecards"
            });
        }
    }

    #region Employee Management Conversion Methods

    private static List<EmployeeDto> ConvertDataTableToEmployees(DataTable dataTable)
    {
        var employees = new List<EmployeeDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var employee = new EmployeeDto
            {
                Id = Convert.ToInt32(row["Id"]),
                FirstName = row["FirstName"]?.ToString(),
                LastName = row["LastName"]?.ToString(),
                EmployeeNumber = row["EmployeeNumber"]?.ToString(),
                Email = row["Email"]?.ToString(),
                PhoneMobile = row["PhoneMobile"]?.ToString(),
                PhoneHome = row["PhoneHome"]?.ToString(),
                PhoneDesk = row["PhoneDesk"]?.ToString(),
                Extension = row["Extension"]?.ToString(),
                Username = row["Username"]?.ToString() ?? string.Empty,
                Password = row["Password"]?.ToString() ?? string.Empty,
                Active = Convert.ToBoolean(row["Active"]),
                DaysAvailablePTO = row["DaysAvailablePTO"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailablePTO"]) : null,
                DaysAvailableVacation = row["DaysAvailableVacation"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailableVacation"]) : null,
                Note = row["Note"]?.ToString(),
                VehicleNumber = row["VehicleNumber"]?.ToString(),
                Picture = row["Picture"]?.ToString(),
                ZoneId = row["ZoneId"] != DBNull.Value ? Convert.ToInt32(row["ZoneId"]) : null,
                ZoneName = row["ZoneName"]?.ToString(),
                AddressId = row["AddressId"] != DBNull.Value ? Convert.ToInt32(row["AddressId"]) : null,
                Address1 = row["Address1"]?.ToString(),
                Address2 = row["Address2"]?.ToString(),
                City = row["City"]?.ToString(),
                State = row["State"]?.ToString(),
                Zip = row["Zip"]?.ToString(),
                LicenseNumber = row["LicenseNumber"]?.ToString(),
                LicenseState = row["LicenseState"]?.ToString(),
                LicenseExpiration = row["LicenseExpiration"] != DBNull.Value ? Convert.ToDateTime(row["LicenseExpiration"]) : null
            };

            employees.Add(employee);
        }

        return employees;
    }

    private static List<RoleDto> ConvertDataTableToRoles(DataTable dataTable)
    {
        var roles = new List<RoleDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var role = new RoleDto
            {
                Id = Convert.ToInt32(row["Id"]),
                Name = row["Name"]?.ToString() ?? string.Empty,
                Description = row["Description"]?.ToString(),
                Active = Convert.ToBoolean(row["Active"])
            };

            roles.Add(role);
        }

        return roles;
    }

    private static List<UserRoleDto> ConvertDataTableToUserRoles(DataTable dataTable)
    {
        var userRoles = new List<UserRoleDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var userRole = new UserRoleDto
            {
                UserId = Convert.ToInt32(row["UserId"]),
                RoleId = Convert.ToInt32(row["RoleId"]),
                RoleName = row["RoleName"]?.ToString() ?? string.Empty,
                RoleDescription = row["RoleDescription"]?.ToString()
            };

            userRoles.Add(userRole);
        }

        return userRoles;
    }

    private static List<AddressDto> ConvertDataTableToAddresses(DataTable dataTable)
    {
        var addresses = new List<AddressDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var address = new AddressDto
            {
                Id = Convert.ToInt32(row["Id"]),
                InsertDateTime = Convert.ToDateTime(row["InsertDateTime"]),
                ModifiedDateTime = row["ModifiedDateTime"] != DBNull.Value ? Convert.ToDateTime(row["ModifiedDateTime"]) : null,
                Address1 = row["Address1"]?.ToString(),
                Address2 = row["Address2"]?.ToString(),
                City = row["City"]?.ToString(),
                State = row["State"]?.ToString(),
                Zip = row["Zip"]?.ToString(),
                Phone = row["Phone"]?.ToString(),
                Email = row["Email"]?.ToString(),
                Notes = row["Notes"]?.ToString(),
                Active = Convert.ToBoolean(row["Active"])
            };

            addresses.Add(address);
        }

        return addresses;
    }

    private static List<EmployeeDto> ConvertDataTableToEmployeesWithRolesAndTradeGenerals(DataTable dataTable)
    {
        var employeeDict = new Dictionary<int, EmployeeDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var employeeId = Convert.ToInt32(row["Id"]);
            
            // If we haven't seen this employee yet, create them
            if (!employeeDict.ContainsKey(employeeId))
            {
                var employee = new EmployeeDto
                {
                    Id = employeeId,
                    FirstName = row["FirstName"]?.ToString(),
                    LastName = row["LastName"]?.ToString(),
                    EmployeeNumber = row["EmployeeNumber"]?.ToString(),
                    Email = row["Email"]?.ToString(),
                    PhoneMobile = row["PhoneMobile"]?.ToString(),
                    PhoneHome = row["PhoneHome"]?.ToString(),
                    PhoneDesk = row["PhoneDesk"]?.ToString(),
                    Extension = row["Extension"]?.ToString(),
                    Username = row["Username"]?.ToString() ?? string.Empty,
                    Password = row["Password"]?.ToString() ?? string.Empty,
                    Active = Convert.ToBoolean(row["Active"]),
                    DirectoryOnly = row["DirectoryOnly"] != DBNull.Value ? Convert.ToBoolean(row["DirectoryOnly"]) : false,
                    DaysAvailablePTO = row["DaysAvailablePTO"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailablePTO"]) : null,
                    DaysAvailableVacation = row["DaysAvailableVacation"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailableVacation"]) : null,
                    Note = row["Note"]?.ToString(),
                    VehicleNumber = row["VehicleNumber"]?.ToString(),
                    Picture = row["Picture"]?.ToString(),
                    ZoneId = row["ZoneId"] != DBNull.Value ? Convert.ToInt32(row["ZoneId"]) : null,
                    ZoneNumber = row["ZoneNumber"] != DBNull.Value ? row["ZoneNumber"].ToString() : null,
                    ZoneName = row["ZoneName"]?.ToString(),
                    AddressId = row["AddressId"] != DBNull.Value ? Convert.ToInt32(row["AddressId"]) : null,
                    Address1 = row["Address1"]?.ToString(),
                    Address2 = row["Address2"]?.ToString(),
                    City = row["City"]?.ToString(),
                    State = row["State"]?.ToString(),
                    Zip = row["Zip"]?.ToString(),
                    // Clothing Size Information
                    ShirtSizeId = row["ShirtSizeId"] != DBNull.Value ? Convert.ToInt32(row["ShirtSizeId"]) : null,
                    PantsWaistId = row["PantsWaistId"] != DBNull.Value ? Convert.ToInt32(row["PantsWaistId"]) : null,
                    PantsLengthId = row["PantsLengthId"] != DBNull.Value ? Convert.ToInt32(row["PantsLengthId"]) : null,
                    JacketSizeId = row["JacketSizeId"] != DBNull.Value ? Convert.ToInt32(row["JacketSizeId"]) : null,
                    ShirtSize = row["ShirtSize"]?.ToString(),
                    PantsWaistSize = row["PantsWaistSize"]?.ToString(),
                    PantsLengthSize = row["PantsLengthSize"]?.ToString(),
                    JacketSize = row["JacketSize"]?.ToString(),
                    LicenseNumber = row["LicenseNumber"]?.ToString(),
                    LicenseState = row["LicenseState"]?.ToString(),
                    LicenseExpiration = row["LicenseExpiration"] != DBNull.Value ? Convert.ToDateTime(row["LicenseExpiration"]) : null,
                    Roles = new List<UserRoleDto>(),
                    TradeGenerals = new List<UserTradeGeneralDto>()
                };

                employeeDict[employeeId] = employee;
            }

            var currentEmployee = employeeDict[employeeId];

            // Add role information if it exists (not null due to LEFT JOIN)
            if (row["RoleId"] != DBNull.Value)
            {
                var roleId = Convert.ToInt32(row["RoleId"]);
                // Check if this role is already added to avoid duplicates
                if (!currentEmployee.Roles.Any(r => r.RoleId == roleId))
                {
                    var userRole = new UserRoleDto
                    {
                        UserId = employeeId,
                        RoleId = roleId,
                        RoleName = row["RoleName"]?.ToString() ?? string.Empty,
                        RoleDescription = row["RoleDescription"]?.ToString()
                    };

                    currentEmployee.Roles.Add(userRole);
                }
            }

            // Add trade general information if it exists (not null due to LEFT JOIN)
            if (row["UserTradeGeneralId"] != DBNull.Value)
            {
                var userTradeGeneralId = Convert.ToInt32(row["UserTradeGeneralId"]);
                // Check if this trade general is already added to avoid duplicates
                if (!currentEmployee.TradeGenerals.Any(tg => tg.Id == userTradeGeneralId))
                {
                    var userTradeGeneral = new UserTradeGeneralDto
                    {
                        Id = userTradeGeneralId,
                        UserId = employeeId,
                        TradeGeneralId = Convert.ToInt32(row["TradeGeneralId"]),
                        Trade = row["Trade"]?.ToString() ?? string.Empty,
                        Type = row["TradeType"]?.ToString() ?? string.Empty
                    };

                    currentEmployee.TradeGenerals.Add(userTradeGeneral);
                }
            }
        }

        return employeeDict.Values.ToList();
    }

    private static List<EmployeeDto> ConvertDataTableToEmployeesWithRoles(DataTable dataTable)
    {
        var employeeDict = new Dictionary<int, EmployeeDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var employeeId = Convert.ToInt32(row["Id"]);
            
            // If we haven't seen this employee yet, create them
            if (!employeeDict.ContainsKey(employeeId))
            {
                var employee = new EmployeeDto
                {
                    Id = employeeId,
                    FirstName = row["FirstName"]?.ToString(),
                    LastName = row["LastName"]?.ToString(),
                    EmployeeNumber = row["EmployeeNumber"]?.ToString(),
                    Email = row["Email"]?.ToString(),
                    PhoneMobile = row["PhoneMobile"]?.ToString(),
                    PhoneHome = row["PhoneHome"]?.ToString(),
                    PhoneDesk = row["PhoneDesk"]?.ToString(),
                    Extension = row["Extension"]?.ToString(),
                    Username = row["Username"]?.ToString() ?? string.Empty,
                    Password = row["Password"]?.ToString() ?? string.Empty,
                    Active = Convert.ToBoolean(row["Active"]),
                    DirectoryOnly = row["DirectoryOnly"] != DBNull.Value ? Convert.ToBoolean(row["DirectoryOnly"]) : false,
                    DaysAvailablePTO = row["DaysAvailablePTO"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailablePTO"]) : null,
                    DaysAvailableVacation = row["DaysAvailableVacation"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailableVacation"]) : null,
                    Note = row["Note"]?.ToString(),
                    VehicleNumber = row["VehicleNumber"]?.ToString(),
                    Picture = row["Picture"]?.ToString(),
                    ZoneId = row["ZoneId"] != DBNull.Value ? Convert.ToInt32(row["ZoneId"]) : null,
                    ZoneNumber = row["ZoneNumber"] != DBNull.Value ? row["ZoneNumber"].ToString() : null,
                    ZoneName = row["ZoneName"]?.ToString(),
                    AddressId = row["AddressId"] != DBNull.Value ? Convert.ToInt32(row["AddressId"]) : null,
                    Address1 = row["Address1"]?.ToString(),
                    Address2 = row["Address2"]?.ToString(),
                    City = row["City"]?.ToString(),
                    State = row["State"]?.ToString(),
                    Zip = row["Zip"]?.ToString(),
                    LicenseNumber = row["LicenseNumber"]?.ToString(),
                    LicenseState = row["LicenseState"]?.ToString(),
                    LicenseExpiration = row["LicenseExpiration"] != DBNull.Value ? Convert.ToDateTime(row["LicenseExpiration"]) : null,
                    Roles = new List<UserRoleDto>()
                };

                employeeDict[employeeId] = employee;
            }

            // Add role information if it exists (not null due to LEFT JOIN)
            if (row["RoleId"] != DBNull.Value)
            {
                var userRole = new UserRoleDto
                {
                    UserId = employeeId,
                    RoleId = Convert.ToInt32(row["RoleId"]),
                    RoleName = row["RoleName"]?.ToString() ?? string.Empty,
                    RoleDescription = row["RoleDescription"]?.ToString()
                };

                employeeDict[employeeId].Roles.Add(userRole);
            }
        }

        return employeeDict.Values.ToList();
    }

    #endregion

    #region TradeGeneral Management Conversion Methods

    private static List<TradeGeneralDto> ConvertDataTableToTradeGenerals(DataTable dataTable)
    {
        var tradeGenerals = new List<TradeGeneralDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var tradeGeneral = new TradeGeneralDto
            {
                Id = Convert.ToInt32(row["Id"]),
                Trade = row["Trade"]?.ToString() ?? string.Empty,
                Type = row["Type"]?.ToString() ?? string.Empty
            };
            tradeGenerals.Add(tradeGeneral);
        }

        return tradeGenerals;
    }

    private static List<UserTradeGeneralDto> ConvertDataTableToUserTradeGenerals(DataTable dataTable)
    {
        var userTradeGenerals = new List<UserTradeGeneralDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var userTradeGeneral = new UserTradeGeneralDto
            {
                Id = Convert.ToInt32(row["Id"]),
                UserId = Convert.ToInt32(row["UserId"]),
                TradeGeneralId = Convert.ToInt32(row["TradeGeneralId"]),
                Trade = row["Trade"]?.ToString() ?? string.Empty,
                Type = row["Type"]?.ToString() ?? string.Empty
            };
            userTradeGenerals.Add(userTradeGeneral);
        }

        return userTradeGenerals;
    }

    /// <summary>
    /// Secure version that excludes sensitive data like passwords for employee directory
    /// </summary>
    private static List<EmployeeDto> ConvertDataTableToEmployeesWithRolesAndTradeGeneralsSecure(DataTable dataTable)
    {
        var employeeDict = new Dictionary<int, EmployeeDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var employeeId = Convert.ToInt32(row["Id"]);
            
            // If we haven't seen this employee yet, create them
            if (!employeeDict.ContainsKey(employeeId))
            {
                var employee = new EmployeeDto
                {
                    Id = employeeId,
                    FirstName = row["FirstName"]?.ToString(),
                    LastName = row["LastName"]?.ToString(),
                    EmployeeNumber = row["EmployeeNumber"]?.ToString(),
                    Email = row["Email"]?.ToString(),
                    PhoneMobile = row["PhoneMobile"]?.ToString(),
                    PhoneHome = row["PhoneHome"]?.ToString(),
                    PhoneDesk = row["PhoneDesk"]?.ToString(),
                    Extension = row["Extension"]?.ToString(),
                    Username = row["Username"]?.ToString() ?? string.Empty,
                    Password = string.Empty, // Excluded for security
                    Active = Convert.ToBoolean(row["Active"]),
                    DirectoryOnly = row["DirectoryOnly"] != DBNull.Value ? Convert.ToBoolean(row["DirectoryOnly"]) : false,
                    DaysAvailablePTO = row["DaysAvailablePTO"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailablePTO"]) : null,
                    DaysAvailableVacation = row["DaysAvailableVacation"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailableVacation"]) : null,
                    Note = row["Note"]?.ToString(),
                    VehicleNumber = row["VehicleNumber"]?.ToString(),
                    Picture = row["Picture"]?.ToString(),
                    ZoneId = row["ZoneId"] != DBNull.Value ? Convert.ToInt32(row["ZoneId"]) : null,
                    ZoneNumber = row["ZoneNumber"] != DBNull.Value ? row["ZoneNumber"].ToString() : null,
                    ZoneName = row["ZoneName"]?.ToString(),
                    AddressId = row["AddressId"] != DBNull.Value ? Convert.ToInt32(row["AddressId"]) : null,
                    Address1 = row["Address1"]?.ToString(),
                    Address2 = row["Address2"]?.ToString(),
                    City = row["City"]?.ToString(),
                    State = row["State"]?.ToString(),
                    Zip = row["Zip"]?.ToString(),
                    IsZoneFacilityManager = row["IsZoneFacilityManager"] != DBNull.Value ? Convert.ToBoolean(row["IsZoneFacilityManager"]) : false,
                    IsRegionFacilityManager = row["IsRegionFacilityManager"] != DBNull.Value ? Convert.ToBoolean(row["IsRegionFacilityManager"]) : false,
                    Roles = new List<UserRoleDto>(),
                    TradeGenerals = new List<UserTradeGeneralDto>()
                };

                employeeDict[employeeId] = employee;
            }

            var currentEmployee = employeeDict[employeeId];

            // Add role information if it exists (not null due to LEFT JOIN)
            if (row["RoleId"] != DBNull.Value)
            {
                var roleId = Convert.ToInt32(row["RoleId"]);
                
                // Check if we already have this role (avoid duplicates)
                if (!currentEmployee.Roles.Any(r => r.RoleId == roleId))
                {
                    var role = new UserRoleDto
                    {
                        UserId = employeeId,
                        RoleId = roleId,
                        RoleName = row["RoleName"]?.ToString() ?? string.Empty,
                        RoleDescription = row["RoleDescription"]?.ToString()
                    };
                    currentEmployee.Roles.Add(role);
                }
            }

            // Add trade general information if it exists (not null due to LEFT JOIN)
            if (row["TradeGeneralId"] != DBNull.Value)
            {
                var tradeGeneralId = Convert.ToInt32(row["TradeGeneralId"]);
                
                // Check if we already have this trade general (avoid duplicates)
                if (!currentEmployee.TradeGenerals.Any(tg => tg.TradeGeneralId == tradeGeneralId))
                {
                    var tradeGeneral = new UserTradeGeneralDto
                    {
                        Id = row["UserTradeGeneralId"] != DBNull.Value ? Convert.ToInt32(row["UserTradeGeneralId"]) : 0,
                        UserId = employeeId,
                        TradeGeneralId = tradeGeneralId,
                        Trade = row["Trade"]?.ToString() ?? string.Empty,
                        Type = row["TradeType"]?.ToString() ?? string.Empty
                    };
                    currentEmployee.TradeGenerals.Add(tradeGeneral);
                }
            }
        }

        return employeeDict.Values.ToList();
    }

    /// <summary>
    /// Secure version that excludes sensitive data like passwords for employee directory
    /// </summary>
    private static List<EmployeeDto> ConvertDataTableToEmployeesWithRolesSecure(DataTable dataTable)
    {
        var employeeDict = new Dictionary<int, EmployeeDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var employeeId = Convert.ToInt32(row["Id"]);
            
            // If we haven't seen this employee yet, create them
            if (!employeeDict.ContainsKey(employeeId))
            {
                var employee = new EmployeeDto
                {
                    Id = employeeId,
                    FirstName = row["FirstName"]?.ToString(),
                    LastName = row["LastName"]?.ToString(),
                    EmployeeNumber = row["EmployeeNumber"]?.ToString(),
                    Email = row["Email"]?.ToString(),
                    PhoneMobile = row["PhoneMobile"]?.ToString(),
                    PhoneHome = row["PhoneHome"]?.ToString(),
                    PhoneDesk = row["PhoneDesk"]?.ToString(),
                    Extension = row["Extension"]?.ToString(),
                    Username = row["Username"]?.ToString() ?? string.Empty,
                    Password = string.Empty, // Excluded for security
                    Active = Convert.ToBoolean(row["Active"]),
                    DirectoryOnly = row["DirectoryOnly"] != DBNull.Value ? Convert.ToBoolean(row["DirectoryOnly"]) : false,
                    DaysAvailablePTO = row["DaysAvailablePTO"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailablePTO"]) : null,
                    DaysAvailableVacation = row["DaysAvailableVacation"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailableVacation"]) : null,
                    Note = row["Note"]?.ToString(),
                    VehicleNumber = row["VehicleNumber"]?.ToString(),
                    Picture = row["Picture"]?.ToString(),
                    ZoneId = row["ZoneId"] != DBNull.Value ? Convert.ToInt32(row["ZoneId"]) : null,
                    ZoneNumber = row["ZoneNumber"] != DBNull.Value ? row["ZoneNumber"].ToString() : null,
                    ZoneName = row["ZoneName"]?.ToString(),
                    AddressId = row["AddressId"] != DBNull.Value ? Convert.ToInt32(row["AddressId"]) : null,
                    Address1 = row["Address1"]?.ToString(),
                    Address2 = row["Address2"]?.ToString(),
                    City = row["City"]?.ToString(),
                    State = row["State"]?.ToString(),
                    Zip = row["Zip"]?.ToString(),
                    Roles = new List<UserRoleDto>()
                };

                employeeDict[employeeId] = employee;
            }

            // Add role information if it exists (not null due to LEFT JOIN)
            if (row["RoleId"] != DBNull.Value)
            {
                var roleId = Convert.ToInt32(row["RoleId"]);
                
                // Check if we already have this role (avoid duplicates)
                if (!employeeDict[employeeId].Roles.Any(r => r.RoleId == roleId))
                {
                    var role = new UserRoleDto
                    {
                        UserId = employeeId,
                        RoleId = roleId,
                        RoleName = row["RoleName"]?.ToString() ?? string.Empty,
                        RoleDescription = row["RoleDescription"]?.ToString()
                    };
                    employeeDict[employeeId].Roles.Add(role);
                }
            }
        }

        return employeeDict.Values.ToList();
    }

    /// <summary>
    /// Minimal data version for tech directory - only city/state, work email/phone, no personal info
    /// Technicians get all phone numbers, non-techs get work phone, desk phone, and extension only
    /// </summary>
    private static List<EmployeeDto> ConvertDataTableToEmployeesForTechDirectory(DataTable dataTable, int? currentUserId = null)
    {
        var employeeDict = new Dictionary<int, EmployeeDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var employeeId = Convert.ToInt32(row["Id"]);
            
            // If we haven't seen this employee yet, create them
            if (!employeeDict.ContainsKey(employeeId))
            {
                var employee = new EmployeeDto
                {
                    Id = employeeId,
                    FirstName = row["FirstName"]?.ToString(),
                    LastName = row["LastName"]?.ToString(),
                    EmployeeNumber = row["EmployeeNumber"]?.ToString(),
                    Email = row["Email"]?.ToString(),
                    PhoneMobile = row["PhoneMobile"]?.ToString(),
                    PhoneHome = row["PhoneHome"]?.ToString(), // Store temporarily, will be cleared for non-techs
                    PhoneDesk = row["PhoneDesk"]?.ToString(),
                    Extension = row["Extension"]?.ToString(),
                    Username = row["Username"]?.ToString() ?? string.Empty,
                    Password = string.Empty, // Excluded for security
                    Active = Convert.ToBoolean(row["Active"]),
                    DirectoryOnly = row["DirectoryOnly"] != DBNull.Value ? Convert.ToBoolean(row["DirectoryOnly"]) : false,
                    DaysAvailablePTO = null, // Excluded for tech directory
                    DaysAvailableVacation = null, // Excluded for tech directory
                    Note = string.Empty, // Excluded for tech directory
                    VehicleNumber = row["VehicleNumber"]?.ToString(),
                    Picture = row["Picture"]?.ToString(),
                    ZoneId = row["ZoneId"] != DBNull.Value ? Convert.ToInt32(row["ZoneId"]) : null,
                    ZoneNumber = row["ZoneNumber"] != DBNull.Value ? row["ZoneNumber"].ToString() : null,
                    ZoneName = row["ZoneName"]?.ToString(),
                    // Only include full address for the current logged-in user
                    AddressId = (currentUserId == employeeId) ? (row["AddressId"] != DBNull.Value ? Convert.ToInt32(row["AddressId"]) : null) : null,
                    Address1 = (currentUserId == employeeId) ? (row["Address1"]?.ToString() ?? string.Empty) : string.Empty,
                    Address2 = (currentUserId == employeeId) ? (row["Address2"]?.ToString() ?? string.Empty) : string.Empty,
                    City = row["City"]?.ToString(),
                    State = row["State"]?.ToString(),
                    Zip = (currentUserId == employeeId) ? (row["Zip"]?.ToString() ?? string.Empty) : string.Empty,
                    IsZoneFacilityManager = row["IsZoneFacilityManager"] != DBNull.Value ? Convert.ToBoolean(row["IsZoneFacilityManager"]) : false,
                    IsRegionFacilityManager = row["IsRegionFacilityManager"] != DBNull.Value ? Convert.ToBoolean(row["IsRegionFacilityManager"]) : false,
                    Roles = new List<UserRoleDto>(),
                    TradeGenerals = new List<UserTradeGeneralDto>()
                };

                employeeDict[employeeId] = employee;
            }

            var currentEmployee = employeeDict[employeeId];

            // Add role information if it exists (not null due to LEFT JOIN)
            if (row["RoleId"] != DBNull.Value)
            {
                var roleId = Convert.ToInt32(row["RoleId"]);
                
                // Check if we already have this role (avoid duplicates)
                if (!currentEmployee.Roles.Any(r => r.RoleId == roleId))
                {
                    var role = new UserRoleDto
                    {
                        UserId = employeeId,
                        RoleId = roleId,
                        RoleName = row["RoleName"]?.ToString() ?? string.Empty,
                        RoleDescription = row["RoleDescription"]?.ToString()
                    };
                    currentEmployee.Roles.Add(role);
                }
            }

            // Add trade general information if it exists (not null due to LEFT JOIN)
            if (row["TradeGeneralId"] != DBNull.Value)
            {
                var tradeGeneralId = Convert.ToInt32(row["TradeGeneralId"]);
                
                // Check if we already have this trade general (avoid duplicates)
                if (!currentEmployee.TradeGenerals.Any(tg => tg.TradeGeneralId == tradeGeneralId))
                {
                    var tradeGeneral = new UserTradeGeneralDto
                    {
                        Id = row["UserTradeGeneralId"] != DBNull.Value ? Convert.ToInt32(row["UserTradeGeneralId"]) : 0,
                        UserId = employeeId,
                        TradeGeneralId = tradeGeneralId,
                        Trade = row["Trade"]?.ToString() ?? string.Empty,
                        Type = row["TradeType"]?.ToString() ?? string.Empty
                    };
                    currentEmployee.TradeGenerals.Add(tradeGeneral);
                }
            }
        }

        // Post-process: Clear PhoneHome for non-technician employees
        foreach (var employee in employeeDict.Values)
        {
            var isTechnician = employee.Roles.Any(r => 
                r.RoleName != null && r.RoleName.Contains("Technician", StringComparison.OrdinalIgnoreCase));
            
            if (!isTechnician)
            {
                employee.PhoneHome = string.Empty;
            }
        }

        return employeeDict.Values.ToList();
    }

    /// <summary>
    /// Minimal data version for tech directory without trades - only city/state, work email/phone, no personal info
    /// </summary>
    private static List<EmployeeDto> ConvertDataTableToEmployeesForTechDirectoryNoTrades(DataTable dataTable, int? currentUserId = null)
    {
        var employeeDict = new Dictionary<int, EmployeeDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var employeeId = Convert.ToInt32(row["Id"]);
            
            // If we haven't seen this employee yet, create them
            if (!employeeDict.ContainsKey(employeeId))
            {
                var employee = new EmployeeDto
                {
                    Id = employeeId,
                    FirstName = row["FirstName"]?.ToString(),
                    LastName = row["LastName"]?.ToString(),
                    EmployeeNumber = row["EmployeeNumber"]?.ToString(),
                    Email = row["Email"]?.ToString(),
                    PhoneMobile = row["PhoneMobile"]?.ToString(),
                    PhoneHome = string.Empty, // Excluded for tech directory
                    PhoneDesk = string.Empty, // Excluded for tech directory
                    Extension = string.Empty, // Excluded for tech directory
                    Username = row["Username"]?.ToString() ?? string.Empty,
                    Password = string.Empty, // Excluded for security
                    Active = Convert.ToBoolean(row["Active"]),
                    DirectoryOnly = row["DirectoryOnly"] != DBNull.Value ? Convert.ToBoolean(row["DirectoryOnly"]) : false,
                    DaysAvailablePTO = null, // Excluded for tech directory
                    DaysAvailableVacation = null, // Excluded for tech directory
                    Note = string.Empty, // Excluded for tech directory
                    VehicleNumber = row["VehicleNumber"]?.ToString(),
                    Picture = row["Picture"]?.ToString(),
                    ZoneId = row["ZoneId"] != DBNull.Value ? Convert.ToInt32(row["ZoneId"]) : null,
                    ZoneNumber = row["ZoneNumber"] != DBNull.Value ? row["ZoneNumber"].ToString() : null,
                    ZoneName = row["ZoneName"]?.ToString(),
                    // Only include full address for the current logged-in user
                    AddressId = (currentUserId == employeeId) ? (row["AddressId"] != DBNull.Value ? Convert.ToInt32(row["AddressId"]) : null) : null,
                    Address1 = (currentUserId == employeeId) ? (row["Address1"]?.ToString() ?? string.Empty) : string.Empty,
                    Address2 = (currentUserId == employeeId) ? (row["Address2"]?.ToString() ?? string.Empty) : string.Empty,
                    City = row["City"]?.ToString(),
                    State = row["State"]?.ToString(),
                    Zip = (currentUserId == employeeId) ? (row["Zip"]?.ToString() ?? string.Empty) : string.Empty,
                    IsZoneFacilityManager = row["IsZoneFacilityManager"] != DBNull.Value ? Convert.ToBoolean(row["IsZoneFacilityManager"]) : false,
                    IsRegionFacilityManager = row["IsRegionFacilityManager"] != DBNull.Value ? Convert.ToBoolean(row["IsRegionFacilityManager"]) : false,
                    Roles = new List<UserRoleDto>()
                };

                employeeDict[employeeId] = employee;
            }

            // Add role information if it exists (not null due to LEFT JOIN)
            if (row["RoleId"] != DBNull.Value)
            {
                var roleId = Convert.ToInt32(row["RoleId"]);
                
                // Check if we already have this role (avoid duplicates)
                if (!employeeDict[employeeId].Roles.Any(r => r.RoleId == roleId))
                {
                    var role = new UserRoleDto
                    {
                        UserId = employeeId,
                        RoleId = roleId,
                        RoleName = row["RoleName"]?.ToString() ?? string.Empty,
                        RoleDescription = row["RoleDescription"]?.ToString()
                    };
                    employeeDict[employeeId].Roles.Add(role);
                }
            }
        }

        return employeeDict.Values.ToList();
    }

    #endregion

    #region TimeTrackingDetail Endpoints

    [HttpPost("timetrackingdetail")]
    [HttpPost("~/api/EvoApi/timetrackingdetail")]
    public async Task<ActionResult<ApiResponse<object>>> CreateTimeTrackingDetail([FromBody] CreateTimeTrackingDetailRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Creating time tracking detail for User {UserId}, TTT_ID {TttId}, WO_ID {WoId}", 
                request.u_id, request.ttt_id, request.wo_id);
            
            // Validate input
            if (request.u_id <= 0 || request.ttt_id <= 0)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Valid User ID and Time Tracking Type ID are required",
                    Count = 0
                });
            }

            // Insert the time tracking detail record
            var success = await _dataService.InsertTimeTrackingDetailAsync(
                request.u_id, 
                request.ttt_id, 
                request.wo_id, 
                request.ttd_lat_browser, 
                request.ttd_lon_browser,
                request.ttd_type);
            stopwatch.Stop();

            if (success)
            {
                await LogAuditAsync("CreateTimeTrackingDetail", request, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Time tracking detail created successfully",
                    Count = 1
                });
            }
            else
            {
                await LogAuditErrorAsync("CreateTimeTrackingDetail", new Exception("Failed to insert time tracking detail"));
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to create time tracking detail"
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error creating time tracking detail for User {UserId}", request.u_id);
            await LogAuditErrorAsync("CreateTimeTrackingDetail", ex);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to create time tracking detail"
            });
        }
    }

    #endregion

    #region Company Administration

    [HttpGet("companies/callcenter/{callCenterId}")]
    public async Task<ActionResult<ApiResponse<List<CompanyListDto>>>> GetCallCenterCompanies(int callCenterId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var companies = await _dataService.GetCallCenterCompaniesAsync(callCenterId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetCallCenterCompanies", $"Retrieved {companies.Count} companies for call center {callCenterId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<CompanyListDto>>
            {
                Success = true,
                Message = "Companies retrieved successfully",
                Data = companies,
                Count = companies.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error retrieving companies for call center {CallCenterId}", callCenterId);
            await LogErrorAsync("GetCallCenterCompanies", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to retrieve companies"
            });
        }
    }

    [HttpGet("companies/detail/{xcccId}")]
    public async Task<ActionResult<ApiResponse<CompanyDetailDto>>> GetCompanyDetail(int xcccId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var companyDetail = await _dataService.GetCompanyDetailAsync(xcccId);
            
            if (companyDetail == null)
            {
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = $"Company with ID {xcccId} not found"
                });
            }
            
            stopwatch.Stop();
            await LogOperationAsync("GetCompanyDetail", $"Retrieved company detail for xccc_id {xcccId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<CompanyDetailDto>
            {
                Success = true,
                Message = "Company detail retrieved successfully",
                Data = companyDetail,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error retrieving company detail for xccc_id {XcccId}", xcccId);
            await LogErrorAsync("GetCompanyDetail", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to retrieve company detail"
            });
        }
    }

    [HttpPut("companies/detail")]
    public async Task<ActionResult<ApiResponse<object>>> UpdateCompanyGeneralInfo([FromBody] UpdateCompanyGeneralInfoRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // Validate request
            if (request?.XcccId <= 0)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Invalid company ID"
                });
            }

            // Get the current values before update for critical audit logging
            var currentCompany = await _dataService.GetCompanyDetailAsync(request.XcccId);
            
            var result = await _dataService.UpdateCompanyGeneralInfoAsync(request);
            
            stopwatch.Stop();
            
            if (result)
            {
                // Log critical audit with change details
                var oldValues = new Dictionary<string, object?>
                {
                    { "TripCharge", currentCompany?.TripCharge },
                    { "BillableRuleId", currentCompany?.BillableRuleId },
                    { "TermsId", currentCompany?.TermsId },
                    { "TaxExempt", currentCompany?.TaxExempt },
                    { "MinimumLaborChargeMinutes", currentCompany?.MinimumLaborChargeMinutes },
                    { "MarkupPercentage", currentCompany?.MarkupPercentage },
                    { "MarkupPercentageSupplier", currentCompany?.MarkupPercentageSupplier },
                    { "Active", currentCompany?.Active },
                    { "FirmQuote", currentCompany?.FirmQuote },
                    { "InvoiceDateShow", currentCompany?.InvoiceDateShow },
                    { "IvrRequestNumber", currentCompany?.IvrRequestNumber },
                    { "ClientRepresentative", currentCompany?.ClientRepresentative },
                    { "LicenseRepresentative", currentCompany?.LicenseRepresentative },
                    { "InvoiceExtraText", currentCompany?.InvoiceExtraText },
                    { "Note", currentCompany?.Note }
                };
                
                var newValues = new Dictionary<string, object?>
                {
                    { "TripCharge", request.TripCharge },
                    { "BillableRuleId", request.BillableRuleId },
                    { "TermsId", request.TermsId },
                    { "TaxExempt", request.TaxExempt },
                    { "MinimumLaborChargeMinutes", request.MinimumLaborChargeMinutes },
                    { "MarkupPercentage", request.MarkupPercentage },
                    { "MarkupPercentageSupplier", request.MarkupPercentageSupplier },
                    { "Active", request.Active },
                    { "FirmQuote", request.FirmQuote },
                    { "InvoiceDateShow", request.InvoiceDateShow },
                    { "IvrRequestNumber", request.IvrRequestNumber },
                    { "ClientRepresentative", request.ClientRepresentative },
                    { "LicenseRepresentative", request.LicenseRepresentative },
                    { "InvoiceExtraText", request.InvoiceExtraText },
                    { "Note", request.Note }
                };
                
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync(
                    $"Company Updated - {currentCompany?.CompanyName} (ID: {request.XcccId})",
                    oldValues,
                    newValues,
                    stopwatch.Elapsed.TotalSeconds.ToString("F3")
                );
                
                await LogOperationAsync("UpdateCompanyGeneralInfo", $"Updated company general info for xccc_id {request.XcccId}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Company general info updated successfully",
                    Count = 1
                });
            }
            else
            {
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to update company general info"
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error updating company general info");
            await LogErrorAsync("UpdateCompanyGeneralInfo", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to update company general info"
            });
        }
    }

    [HttpPost("materials-markup")]
    public async Task<ActionResult<ApiResponse<int>>> CreateMaterialsMarkup([FromBody] CreateMaterialsMarkupRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // Validate request
            if (request?.XcccId <= 0)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Invalid company ID"
                });
            }

            if (request.FromPrice < 0 || request.ToPrice < 0 || request.ToPrice <= request.FromPrice)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Invalid price range: ToPrice must be greater than FromPrice"
                });
            }

            if (request.MarkupPercentage < 0 || request.MarkupPercentage > 100)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Markup percentage must be between 0 and 100"
                });
            }

            var newId = await _dataService.CreateMaterialsMarkupAsync(request);
            
            stopwatch.Stop();
            
            if (newId.HasValue)
            {
                // Log critical audit with new markup values
                var newValues = new Dictionary<string, object?>
                {
                    { "FromPrice", request.FromPrice },
                    { "ToPrice", request.ToPrice },
                    { "MarkupPercentage", request.MarkupPercentage },
                    { "MarkupHighQuantity", request.MarkupHighQuantity }
                };
                
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync(
                    $"Materials Markup Created - Company ID: {request.XcccId}",
                    null,
                    newValues,
                    stopwatch.Elapsed.TotalSeconds.ToString("F3")
                );
                
                await LogOperationAsync("CreateMaterialsMarkup", $"Created materials markup for xccc_id {request.XcccId}, range {request.FromPrice}-{request.ToPrice}, markup {request.MarkupPercentage}%", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<int>
                {
                    Success = true,
                    Message = "Materials markup created successfully",
                    Data = newId.Value,
                    Count = 1
                });
            }
            else
            {
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to create materials markup"
                });
            }
        }
        catch (InvalidOperationException ex)
        {
            stopwatch.Stop();
            _logger.LogWarning(ex, "Validation error creating materials markup");
            
            return BadRequest(new ApiResponse<object>
            {
                Success = false,
                Message = ex.Message
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error creating materials markup for xccc_id {XcccId}", request?.XcccId);
            await LogErrorAsync("CreateMaterialsMarkup", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to create materials markup"
            });
        }
    }

    [HttpPut("materials-markup")]
    public async Task<ActionResult<ApiResponse<object>>> UpdateMaterialsMarkup([FromBody] UpdateMaterialsMarkupRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // Validate request
            if (request?.MmId <= 0)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Invalid materials markup ID"
                });
            }

            if (request.FromPrice < 0 || request.ToPrice < 0 || request.ToPrice <= request.FromPrice)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Invalid price range: ToPrice must be greater than FromPrice"
                });
            }

            if (request.MarkupPercentage < 0 || request.MarkupPercentage > 100)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Markup percentage must be between 0 and 100"
                });
            }

            // Fetch old values before update for audit comparison
            var (oldMarkupData, companyName) = await _dataService.GetMaterialsMarkupWithCompanyByIdAsync(request.MmId);
            if (oldMarkupData == null)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = $"Materials markup record {request.MmId} not found"
                });
            }
            
            var result = await _dataService.UpdateMaterialsMarkupAsync(request);
            
            stopwatch.Stop();
            
            if (result)
            {
                // Log critical audit with change details - show all fields
                var oldValues = new Dictionary<string, object?>
                {
                    { "FromPrice", oldMarkupData.FromPrice },
                    { "ToPrice", oldMarkupData.ToPrice },
                    { "MarkupPercentage", oldMarkupData.MarkupPercentage },
                    { "MarkupHighQuantity", oldMarkupData.MarkupHighQuantity }
                };
                
                var newValues = new Dictionary<string, object?>
                {
                    { "FromPrice", request.FromPrice },
                    { "ToPrice", request.ToPrice },
                    { "MarkupPercentage", request.MarkupPercentage },
                    { "MarkupHighQuantity", request.MarkupHighQuantity }
                };
                
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync(
                    $"Materials Markup Updated - ID: {request.MmId}{(string.IsNullOrEmpty(companyName) ? "" : $" - {companyName}")}",
                    oldValues,
                    newValues,
                    stopwatch.Elapsed.TotalSeconds.ToString("F3")
                );
                
                await LogOperationAsync("UpdateMaterialsMarkup", $"Updated materials markup mm_id {request.MmId}, range {request.FromPrice}-{request.ToPrice}, markup {request.MarkupPercentage}%", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Materials markup updated successfully",
                    Count = 1
                });
            }
            else
            {
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to update materials markup"
                });
            }
        }
        catch (InvalidOperationException ex)
        {
            stopwatch.Stop();
            _logger.LogWarning(ex, "Validation error updating materials markup");
            
            return BadRequest(new ApiResponse<object>
            {
                Success = false,
                Message = ex.Message
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error updating materials markup mm_id {MmId}", request?.MmId);
            await LogErrorAsync("UpdateMaterialsMarkup", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to update materials markup"
            });
        }
    }

    [HttpDelete("materials-markup/{mmId}")]
    public async Task<ActionResult<ApiResponse<object>>> DeleteMaterialsMarkup(int mmId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            if (mmId <= 0)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Invalid materials markup ID"
                });
            }

            var result = await _dataService.DeleteMaterialsMarkupAsync(mmId);
            
            stopwatch.Stop();
            
            if (result)
            {
                // Log critical audit for deletion
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogAsync(
                    $"Materials Markup Deleted - ID: {mmId}",
                    null,
                    stopwatch.Elapsed.TotalSeconds.ToString("F3")
                );
                
                await LogOperationAsync("DeleteMaterialsMarkup", $"Deleted materials markup mm_id {mmId}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Materials markup deleted successfully",
                    Count = 1
                });
            }
            else
            {
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to delete materials markup"
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error deleting materials markup mm_id {MmId}", mmId);
            await LogErrorAsync("DeleteMaterialsMarkup", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to delete materials markup"
            });
        }
    }

    [HttpPost("materials-markup/reset/{xcccId}")]
    public async Task<ActionResult<ApiResponse<object>>> ResetMaterialsMarkupToDefault(int xcccId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            if (xcccId <= 0)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Invalid company call center ID"
                });
            }

            var result = await _dataService.ResetMaterialsMarkupToDefaultAsync(xcccId);
            
            stopwatch.Stop();
            
            if (result)
            {
                await LogOperationAsync("ResetMaterialsMarkupToDefault", $"Reset materials markup to default for xccc_id {xcccId}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Materials markup reset to default successfully",
                    Count = 1
                });
            }
            else
            {
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to reset materials markup"
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error resetting materials markup to default for xccc_id {XcccId}", xcccId);
            await LogErrorAsync("ResetMaterialsMarkupToDefault", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to reset materials markup to default"
            });
        }
    }

    // Company Priority endpoints
    [HttpGet("company-priorities/{companyId}")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<List<CompanyPriorityDto>>>> GetCompanyPriorities(int companyId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            if (companyId <= 0)
            {
                return BadRequest(new ApiResponse<List<CompanyPriorityDto>>
                {
                    Success = false,
                    Message = "Invalid company ID"
                });
            }

            var priorities = await _dataService.GetCompanyPrioritiesAsync(companyId);
            stopwatch.Stop();
            
            await LogOperationAsync("GetCompanyPriorities", $"Retrieved {priorities.Count} priorities for company c_id {companyId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<CompanyPriorityDto>>
            {
                Success = true,
                Message = "Company priorities retrieved successfully",
                Data = priorities,
                Count = priorities.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error retrieving priorities for company c_id {CompanyId}", companyId);
            await LogErrorAsync("GetCompanyPriorities", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<CompanyPriorityDto>>
            {
                Success = false,
                Message = "Failed to retrieve company priorities"
            });
        }
    }

    [HttpPut("company-priorities")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<object>>> UpdateCompanyPriority([FromBody] UpdateCompanyPriorityRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            if (request?.XcpId <= 0)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Invalid priority ID"
                });
            }

            if (string.IsNullOrWhiteSpace(request.CompanySpecificName))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Company specific name is required"
                });
            }

            if (request.ArrivalTimeInHours < 0)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Arrival time must be 0 or greater"
                });
            }

            var result = await _dataService.UpdateCompanyPriorityAsync(request);
            stopwatch.Stop();
            
            if (result)
            {
                await LogOperationAsync("UpdateCompanyPriority", $"Updated company priority xcp_id {request.XcpId}, name '{request.CompanySpecificName}', arrival time {request.ArrivalTimeInHours} hours", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Company priority updated successfully",
                    Count = 1
                });
            }
            else
            {
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to update company priority"
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error updating company priority xcp_id {XcpId}", request?.XcpId);
            await LogErrorAsync("UpdateCompanyPriority", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "Failed to update company priority"
            });
        }
    }

    // User Attachment Type endpoints
    [HttpGet("userattachmenttypes")]
    public async Task<ActionResult<ApiResponse<List<UserAttachmentTypeDto>>>> GetUserAttachmentTypes()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Getting all user attachment types");
            
            // Get data from service
            var dataTable = await _dataService.GetAllUserAttachmentTypesAsync();
            var attachmentTypes = ConvertDataTableToUserAttachmentTypes(dataTable);

            stopwatch.Stop();
            
            // Log successful operation
            await LogOperationAsync("GetUserAttachmentTypes", $"Retrieved {attachmentTypes.Count} user attachment types", stopwatch.Elapsed);

            return Ok(new ApiResponse<List<UserAttachmentTypeDto>>
            {
                Success = true,
                Message = "User attachment types retrieved successfully",
                Data = attachmentTypes,
                Count = attachmentTypes.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetUserAttachmentTypes", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error retrieving user attachment types");
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while retrieving user attachment types",
                Count = 0
            });
        }
    }

    [HttpPost("userattachmenttypes")]
    public async Task<ActionResult<ApiResponse<UserAttachmentTypeDto>>> CreateUserAttachmentType([FromBody] CreateUserAttachmentTypeRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Creating new user attachment type: {UatType}", request.uat_type);
            
            // Validate the request
            if (string.IsNullOrWhiteSpace(request.uat_type) || request.uat_type.Length < 2)
            {
                return BadRequest(new ApiResponse<UserAttachmentTypeDto>
                {
                    Success = false,
                    Message = "Attachment type must be at least 2 characters long",
                    Count = 0
                });
            }

            if (request.uat_type.Length > 100)
            {
                return BadRequest(new ApiResponse<UserAttachmentTypeDto>
                {
                    Success = false,
                    Message = "Attachment type must be no more than 100 characters",
                    Count = 0
                });
            }
            
            var newId = await _dataService.CreateUserAttachmentTypeAsync(request);
            
            if (newId.HasValue)
            {
                // Create the DTO to return
                var newAttachmentType = new UserAttachmentTypeDto
                {
                    uat_id = newId.Value,
                    uat_type = request.uat_type,
                    uat_insertdatetime = DateTime.Now,
                    uat_modifieddatetime = DateTime.Now
                };
                
                stopwatch.Stop();
                await LogOperationAsync("CreateUserAttachmentType", $"Created user attachment type - {request.uat_type} with ID {newId.Value}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<UserAttachmentTypeDto>
                {
                    Success = true,
                    Message = "User attachment type created successfully",
                    Data = newAttachmentType,
                    Count = 1
                });
            }
            else
            {
                stopwatch.Stop();
                await LogOperationAsync("CreateUserAttachmentType", $"Failed to create user attachment type - {request.uat_type}", stopwatch.Elapsed);
                
                return BadRequest(new ApiResponse<UserAttachmentTypeDto>
                {
                    Success = false,
                    Message = "Failed to create user attachment type",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateUserAttachmentType", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error creating user attachment type {UatType}", request.uat_type);
            
            return StatusCode(500, new ApiResponse<UserAttachmentTypeDto>
            {
                Success = false,
                Message = "An error occurred while creating the user attachment type",
                Count = 0
            });
        }
    }

    [HttpPut("userattachmenttypes/{id}")]
    public async Task<ActionResult<ApiResponse<object>>> UpdateUserAttachmentType(int id, [FromBody] UpdateUserAttachmentTypeRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Updating user attachment type {Id}", id);
            
            // Validate input
            if (id != request.uat_id)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "ID in URL does not match ID in request body",
                    Count = 0
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.uat_type) || request.uat_type.Length < 2)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Attachment type must be at least 2 characters long",
                    Count = 0
                });
            }

            if (request.uat_type.Length > 100)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Attachment type must be no more than 100 characters",
                    Count = 0
                });
            }

            // Update user attachment type
            var success = await _dataService.UpdateUserAttachmentTypeAsync(request);
            
            stopwatch.Stop();
            
            if (success)
            {
                // Log successful operation
                await LogOperationAsync("UpdateUserAttachmentType", $"Updated user attachment type {id} - {request.uat_type}", stopwatch.Elapsed);
    
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "User attachment type updated successfully",
                    Count = 1
                });
            }
            else
            {
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = "User attachment type not found",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateUserAttachmentType", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error updating user attachment type {Id}", id);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while updating the user attachment type",
                Count = 0
            });
        }
    }

    // User Clothing Size endpoints
    [HttpGet("userclothing")]
    public async Task<ActionResult<ApiResponse<List<UserClothingSizeDto>>>> GetUserClothingSizes()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Getting all user clothing sizes");
            
            var dataTable = await _dataService.GetAllUserClothingSizesAsync();
            var clothingSizes = ConvertDataTableToUserClothingSizes(dataTable);

            stopwatch.Stop();
            await LogOperationAsync("GetUserClothingSizes", $"Retrieved {clothingSizes.Count} user clothing sizes", stopwatch.Elapsed);

            return Ok(new ApiResponse<List<UserClothingSizeDto>>
            {
                Success = true,
                Message = "User clothing sizes retrieved successfully",
                Data = clothingSizes,
                Count = clothingSizes.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetUserClothingSizes", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error retrieving user clothing sizes");
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while retrieving user clothing sizes",
                Count = 0
            });
        }
    }

    [HttpPost("userclothing")]
    public async Task<ActionResult<ApiResponse<UserClothingSizeDto>>> CreateUserClothingSize([FromBody] CreateUserClothingSizeRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Creating new user clothing size: {ClothingSize}", request.ClothingSize);
            
            if (string.IsNullOrWhiteSpace(request.ClothingSize) || request.ClothingSize.Length < 2)
            {
                return BadRequest(new ApiResponse<UserClothingSizeDto>
                {
                    Success = false,
                    Message = "Clothing size must be at least 2 characters long",
                    Count = 0
                });
            }

            if (request.ClothingSize.Length > 50)
            {
                return BadRequest(new ApiResponse<UserClothingSizeDto>
                {
                    Success = false,
                    Message = "Clothing size must be no more than 50 characters",
                    Count = 0
                });
            }

            // Check for duplicate
            var existingDataTable = await _dataService.GetAllUserClothingSizesAsync();
            var existingSizes = ConvertDataTableToUserClothingSizes(existingDataTable);
            if (existingSizes.Any(s => s.ClothingSize.Equals(request.ClothingSize.Trim(), StringComparison.OrdinalIgnoreCase)))
            {
                return BadRequest(new ApiResponse<UserClothingSizeDto>
                {
                    Success = false,
                    Message = "This clothing size already exists",
                    Count = 0
                });
            }
            
            var newId = await _dataService.CreateUserClothingSizeAsync(request);
            
            if (newId.HasValue)
            {
                var newClothingSize = new UserClothingSizeDto
                {
                    Id = newId.Value,
                    ClothingSize = request.ClothingSize.Trim()
                };
                
                stopwatch.Stop();
                await LogOperationAsync("CreateUserClothingSize", $"Created user clothing size - {request.ClothingSize} with ID {newId.Value}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<UserClothingSizeDto>
                {
                    Success = true,
                    Message = "User clothing size created successfully",
                    Data = newClothingSize,
                    Count = 1
                });
            }
            else
            {
                stopwatch.Stop();
                await LogOperationAsync("CreateUserClothingSize", $"Failed to create user clothing size - {request.ClothingSize}", stopwatch.Elapsed);
                
                return BadRequest(new ApiResponse<UserClothingSizeDto>
                {
                    Success = false,
                    Message = "Failed to create user clothing size",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateUserClothingSize", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error creating user clothing size {ClothingSize}", request.ClothingSize);
            
            return StatusCode(500, new ApiResponse<UserClothingSizeDto>
            {
                Success = false,
                Message = "An error occurred while creating the user clothing size",
                Count = 0
            });
        }
    }

    [HttpPut("userclothing/{id}")]
    public async Task<ActionResult<ApiResponse<object>>> UpdateUserClothingSize(int id, [FromBody] UpdateUserClothingSizeRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Updating user clothing size {Id}", id);
            
            if (id != request.Id)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "ID in URL does not match ID in request body",
                    Count = 0
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.ClothingSize) || request.ClothingSize.Length < 2)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Clothing size must be at least 2 characters long",
                    Count = 0
                });
            }

            if (request.ClothingSize.Length > 50)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Clothing size must be no more than 50 characters",
                    Count = 0
                });
            }

            // Check for duplicate (excluding current record)
            var existingDataTable = await _dataService.GetAllUserClothingSizesAsync();
            var existingSizes = ConvertDataTableToUserClothingSizes(existingDataTable);
            if (existingSizes.Any(s => s.Id != id && s.ClothingSize.Equals(request.ClothingSize.Trim(), StringComparison.OrdinalIgnoreCase)))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "This clothing size already exists",
                    Count = 0
                });
            }
            
            var success = await _dataService.UpdateUserClothingSizeAsync(request);
            
            stopwatch.Stop();
            
            if (success)
            {
                await LogOperationAsync("UpdateUserClothingSize", $"Updated user clothing size {id} to {request.ClothingSize}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "User clothing size updated successfully",
                    Count = 1
                });
            }
            else
            {
                await LogOperationAsync("UpdateUserClothingSize", $"Failed to update user clothing size {id}", stopwatch.Elapsed);
                
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = "User clothing size not found or update failed",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateUserClothingSize", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error updating user clothing size {Id}", id);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while updating the user clothing size",
                Count = 0
            });
        }
    }

    // User Pants Waist endpoints
    [HttpGet("userpantswaist")]
    public async Task<ActionResult<ApiResponse<List<UserPantsWaistDto>>>> GetUserPantsWaist()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Getting all user pants waist sizes");
            
            var dataTable = await _dataService.GetAllUserPantsWaistAsync();
            var pantsWaist = ConvertDataTableToUserPantsWaist(dataTable);

            stopwatch.Stop();
            await LogOperationAsync("GetUserPantsWaist", $"Retrieved {pantsWaist.Count} user pants waist sizes", stopwatch.Elapsed);

            return Ok(new ApiResponse<List<UserPantsWaistDto>>
            {
                Success = true,
                Message = "User pants waist sizes retrieved successfully",
                Data = pantsWaist,
                Count = pantsWaist.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetUserPantsWaist", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error retrieving user pants waist sizes");
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while retrieving user pants waist sizes",
                Count = 0
            });
        }
    }

    [HttpPost("userpantswaist")]
    public async Task<ActionResult<ApiResponse<UserPantsWaistDto>>> CreateUserPantsWaist([FromBody] CreateUserPantsWaistRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Creating new user pants waist size: {Size} ({Sex})", request.Size, request.Sex);
            
            if (string.IsNullOrWhiteSpace(request.Size) || !int.TryParse(request.Size, out var _))
            {
                return BadRequest(new ApiResponse<UserPantsWaistDto>
                {
                    Success = false,
                    Message = "Size must be a valid number",
                    Count = 0
                });
            }

            if (request.Size.Length > 2)
            {
                return BadRequest(new ApiResponse<UserPantsWaistDto>
                {
                    Success = false,
                    Message = "Size must be no more than 2 characters",
                    Count = 0
                });
            }

            if (string.IsNullOrWhiteSpace(request.Sex) || !new[] { "Male", "Female" }.Contains(request.Sex))
            {
                return BadRequest(new ApiResponse<UserPantsWaistDto>
                {
                    Success = false,
                    Message = "Sex must be either 'Male' or 'Female'",
                    Count = 0
                });
            }

            // Check for duplicate
            var existingDataTable = await _dataService.GetAllUserPantsWaistAsync();
            var existingSizes = ConvertDataTableToUserPantsWaist(existingDataTable);
            if (existingSizes.Any(s => s.Size == request.Size.Trim() && s.Sex == request.Sex))
            {
                return BadRequest(new ApiResponse<UserPantsWaistDto>
                {
                    Success = false,
                    Message = "This pants waist size already exists",
                    Count = 0
                });
            }
            
            var newId = await _dataService.CreateUserPantsWaistAsync(request);
            
            if (newId.HasValue)
            {
                var newPantsWaist = new UserPantsWaistDto
                {
                    Id = newId.Value,
                    Size = request.Size.Trim(),
                    Sex = request.Sex
                };
                
                stopwatch.Stop();
                await LogOperationAsync("CreateUserPantsWaist", $"Created user pants waist size - Size: {request.Size}, Sex: {request.Sex} with ID {newId.Value}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<UserPantsWaistDto>
                {
                    Success = true,
                    Message = "User pants waist size created successfully",
                    Data = newPantsWaist,
                    Count = 1
                });
            }
            else
            {
                stopwatch.Stop();
                await LogOperationAsync("CreateUserPantsWaist", $"Failed to create user pants waist size - {request.Size}", stopwatch.Elapsed);
                
                return BadRequest(new ApiResponse<UserPantsWaistDto>
                {
                    Success = false,
                    Message = "Failed to create user pants waist size",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateUserPantsWaist", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error creating user pants waist size {Size}", request.Size);
            
            return StatusCode(500, new ApiResponse<UserPantsWaistDto>
            {
                Success = false,
                Message = "An error occurred while creating the user pants waist size",
                Count = 0
            });
        }
    }

    [HttpPut("userpantswaist/{id}")]
    public async Task<ActionResult<ApiResponse<object>>> UpdateUserPantsWaist(int id, [FromBody] UpdateUserPantsWaistRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Updating user pants waist size {Id}", id);
            
            if (id != request.Id)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "ID in URL does not match ID in request body",
                    Count = 0
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.Size) || !int.TryParse(request.Size, out var _))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Size must be a valid number",
                    Count = 0
                });
            }

            if (request.Size.Length > 2)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Size must be no more than 2 characters",
                    Count = 0
                });
            }

            if (string.IsNullOrWhiteSpace(request.Sex) || !new[] { "Male", "Female" }.Contains(request.Sex))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Sex must be either 'Male' or 'Female'",
                    Count = 0
                });
            }

            // Check for duplicate (excluding current record)
            var existingDataTable = await _dataService.GetAllUserPantsWaistAsync();
            var existingSizes = ConvertDataTableToUserPantsWaist(existingDataTable);
            if (existingSizes.Any(s => s.Id != id && s.Size == request.Size.Trim() && s.Sex == request.Sex))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "This pants waist size already exists",
                    Count = 0
                });
            }
            
            var success = await _dataService.UpdateUserPantsWaistAsync(request);
            
            stopwatch.Stop();
            
            if (success)
            {
                await LogOperationAsync("UpdateUserPantsWaist", $"Updated user pants waist size {id} - Size: {request.Size}, Sex: {request.Sex}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "User pants waist size updated successfully",
                    Count = 1
                });
            }
            else
            {
                await LogOperationAsync("UpdateUserPantsWaist", $"Failed to update user pants waist size {id}", stopwatch.Elapsed);
                
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = "User pants waist size not found or update failed",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateUserPantsWaist", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error updating user pants waist size {Id}", id);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while updating the user pants waist size",
                Count = 0
            });
        }
    }

    // User Pants Length endpoints
    [HttpGet("userpantslength")]
    public async Task<ActionResult<ApiResponse<List<UserPantsLengthDto>>>> GetUserPantsLength()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Getting all user pants length sizes");
            
            var dataTable = await _dataService.GetAllUserPantsLengthAsync();
            var pantsLength = ConvertDataTableToUserPantsLength(dataTable);

            stopwatch.Stop();
            await LogOperationAsync("GetUserPantsLength", $"Retrieved {pantsLength.Count} user pants length sizes", stopwatch.Elapsed);

            return Ok(new ApiResponse<List<UserPantsLengthDto>>
            {
                Success = true,
                Message = "User pants length sizes retrieved successfully",
                Data = pantsLength,
                Count = pantsLength.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetUserPantsLength", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error retrieving user pants length sizes");
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while retrieving user pants length sizes",
                Count = 0
            });
        }
    }

    [HttpPost("userpantslength")]
    public async Task<ActionResult<ApiResponse<UserPantsLengthDto>>> CreateUserPantsLength([FromBody] CreateUserPantsLengthRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Creating new user pants length size: {Size} ({Sex})", request.Size, request.Sex);
            
            if (string.IsNullOrWhiteSpace(request.Size) || !int.TryParse(request.Size, out var _))
            {
                return BadRequest(new ApiResponse<UserPantsLengthDto>
                {
                    Success = false,
                    Message = "Size must be a valid number",
                    Count = 0
                });
            }

            if (request.Size.Length > 2)
            {
                return BadRequest(new ApiResponse<UserPantsLengthDto>
                {
                    Success = false,
                    Message = "Size must be no more than 2 characters",
                    Count = 0
                });
            }

            if (string.IsNullOrWhiteSpace(request.Sex) || !new[] { "Male", "Female" }.Contains(request.Sex))
            {
                return BadRequest(new ApiResponse<UserPantsLengthDto>
                {
                    Success = false,
                    Message = "Sex must be either 'Male' or 'Female'",
                    Count = 0
                });
            }

            // Check for duplicate
            var existingDataTable = await _dataService.GetAllUserPantsLengthAsync();
            var existingSizes = ConvertDataTableToUserPantsLength(existingDataTable);
            if (existingSizes.Any(s => s.Size == request.Size.Trim() && s.Sex == request.Sex))
            {
                return BadRequest(new ApiResponse<UserPantsLengthDto>
                {
                    Success = false,
                    Message = "This pants length size already exists",
                    Count = 0
                });
            }
            
            var newId = await _dataService.CreateUserPantsLengthAsync(request);
            
            if (newId.HasValue)
            {
                var newPantsLength = new UserPantsLengthDto
                {
                    Id = newId.Value,
                    Size = request.Size.Trim(),
                    Sex = request.Sex
                };
                
                stopwatch.Stop();
                await LogOperationAsync("CreateUserPantsLength", $"Created user pants length size - Size: {request.Size}, Sex: {request.Sex} with ID {newId.Value}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<UserPantsLengthDto>
                {
                    Success = true,
                    Message = "User pants length size created successfully",
                    Data = newPantsLength,
                    Count = 1
                });
            }
            else
            {
                stopwatch.Stop();
                await LogOperationAsync("CreateUserPantsLength", $"Failed to create user pants length size - {request.Size}", stopwatch.Elapsed);
                
                return BadRequest(new ApiResponse<UserPantsLengthDto>
                {
                    Success = false,
                    Message = "Failed to create user pants length size",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateUserPantsLength", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error creating user pants length size {Size}", request.Size);
            
            return StatusCode(500, new ApiResponse<UserPantsLengthDto>
            {
                Success = false,
                Message = "An error occurred while creating the user pants length size",
                Count = 0
            });
        }
    }

    [HttpPut("userpantslength/{id}")]
    public async Task<ActionResult<ApiResponse<object>>> UpdateUserPantsLength(int id, [FromBody] UpdateUserPantsLengthRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Updating user pants length size {Id}", id);
            
            if (id != request.Id)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "ID in URL does not match ID in request body",
                    Count = 0
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.Size) || !int.TryParse(request.Size, out var _))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Size must be a valid number",
                    Count = 0
                });
            }

            if (request.Size.Length > 2)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Size must be no more than 2 characters",
                    Count = 0
                });
            }

            if (string.IsNullOrWhiteSpace(request.Sex) || !new[] { "Male", "Female" }.Contains(request.Sex))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Sex must be either 'Male' or 'Female'",
                    Count = 0
                });
            }

            // Check for duplicate (excluding current record)
            var existingDataTable = await _dataService.GetAllUserPantsLengthAsync();
            var existingSizes = ConvertDataTableToUserPantsLength(existingDataTable);
            if (existingSizes.Any(s => s.Id != id && s.Size == request.Size.Trim() && s.Sex == request.Sex))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "This pants length size already exists",
                    Count = 0
                });
            }
            
            var success = await _dataService.UpdateUserPantsLengthAsync(request);
            
            stopwatch.Stop();
            
            if (success)
            {
                await LogOperationAsync("UpdateUserPantsLength", $"Updated user pants length size {id} - Size: {request.Size}, Sex: {request.Sex}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "User pants length size updated successfully",
                    Count = 1
                });
            }
            else
            {
                await LogOperationAsync("UpdateUserPantsLength", $"Failed to update user pants length size {id}", stopwatch.Elapsed);
                
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = "User pants length size not found or update failed",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateUserPantsLength", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error updating user pants length size {Id}", id);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while updating the user pants length size",
                Count = 0
            });
        }
    }

    // User Relationship endpoints
    [HttpGet("userrelationship")]
    public async Task<ActionResult<ApiResponse<List<UserRelationshipDto>>>> GetUserRelationships()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Getting all user relationships");
            
            var dataTable = await _dataService.GetAllUserRelationshipsAsync();
            var relationships = ConvertDataTableToUserRelationships(dataTable);

            stopwatch.Stop();
            await LogOperationAsync("GetUserRelationships", $"Retrieved {relationships.Count} user relationships", stopwatch.Elapsed);

            return Ok(new ApiResponse<List<UserRelationshipDto>>
            {
                Success = true,
                Message = "User relationships retrieved successfully",
                Data = relationships,
                Count = relationships.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetUserRelationships", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error retrieving user relationships");
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while retrieving user relationships",
                Count = 0
            });
        }
    }

    [HttpPost("userrelationship")]
    public async Task<ActionResult<ApiResponse<UserRelationshipDto>>> CreateUserRelationship([FromBody] CreateUserRelationshipRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Creating new user relationship: {Relationship}", request.Relationship);
            
            if (string.IsNullOrWhiteSpace(request.Relationship) || request.Relationship.Length < 2)
            {
                return BadRequest(new ApiResponse<UserRelationshipDto>
                {
                    Success = false,
                    Message = "Relationship must be at least 2 characters long",
                    Count = 0
                });
            }

            if (request.Relationship.Length > 50)
            {
                return BadRequest(new ApiResponse<UserRelationshipDto>
                {
                    Success = false,
                    Message = "Relationship must be no more than 50 characters",
                    Count = 0
                });
            }

            // Check for duplicate
            var existingDataTable = await _dataService.GetAllUserRelationshipsAsync();
            var existingRelationships = ConvertDataTableToUserRelationships(existingDataTable);
            if (existingRelationships.Any(r => r.Relationship.Equals(request.Relationship.Trim(), StringComparison.OrdinalIgnoreCase)))
            {
                return BadRequest(new ApiResponse<UserRelationshipDto>
                {
                    Success = false,
                    Message = "This relationship already exists",
                    Count = 0
                });
            }
            
            var newId = await _dataService.CreateUserRelationshipAsync(request);
            
            if (newId.HasValue)
            {
                var newRelationship = new UserRelationshipDto
                {
                    Id = newId.Value,
                    Relationship = request.Relationship.Trim()
                };
                
                stopwatch.Stop();
                await LogOperationAsync("CreateUserRelationship", $"Created user relationship - {request.Relationship} with ID {newId.Value}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<UserRelationshipDto>
                {
                    Success = true,
                    Message = "User relationship created successfully",
                    Data = newRelationship,
                    Count = 1
                });
            }
            else
            {
                stopwatch.Stop();
                await LogOperationAsync("CreateUserRelationship", $"Failed to create user relationship - {request.Relationship}", stopwatch.Elapsed);
                
                return BadRequest(new ApiResponse<UserRelationshipDto>
                {
                    Success = false,
                    Message = "Failed to create user relationship",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateUserRelationship", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error creating user relationship {Relationship}", request.Relationship);
            
            return StatusCode(500, new ApiResponse<UserRelationshipDto>
            {
                Success = false,
                Message = "An error occurred while creating the user relationship",
                Count = 0
            });
        }
    }

    [HttpPut("userrelationship/{id}")]
    public async Task<ActionResult<ApiResponse<object>>> UpdateUserRelationship(int id, [FromBody] UpdateUserRelationshipRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Updating user relationship {Id}", id);
            
            if (id != request.Id)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "ID in URL does not match ID in request body",
                    Count = 0
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.Relationship) || request.Relationship.Length < 2)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Relationship must be at least 2 characters long",
                    Count = 0
                });
            }

            if (request.Relationship.Length > 50)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Relationship must be no more than 50 characters",
                    Count = 0
                });
            }

            // Check for duplicate (excluding current record)
            var existingDataTable = await _dataService.GetAllUserRelationshipsAsync();
            var existingRelationships = ConvertDataTableToUserRelationships(existingDataTable);
            if (existingRelationships.Any(r => r.Id != id && r.Relationship.Equals(request.Relationship.Trim(), StringComparison.OrdinalIgnoreCase)))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "This relationship already exists",
                    Count = 0
                });
            }
            
            var success = await _dataService.UpdateUserRelationshipAsync(request);
            
            stopwatch.Stop();
            
            if (success)
            {
                await LogOperationAsync("UpdateUserRelationship", $"Updated user relationship {id} to {request.Relationship}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "User relationship updated successfully",
                    Count = 1
                });
            }
            else
            {
                await LogOperationAsync("UpdateUserRelationship", $"Failed to update user relationship {id}", stopwatch.Elapsed);
                
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = "User relationship not found or update failed",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateUserRelationship", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error updating user relationship {Id}", id);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while updating the user relationship",
                Count = 0
            });
        }
    }

    // User Emergency Contact endpoints
    [HttpGet("employees/emergency-contacts/all")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<Dictionary<int, List<UserEmergencyContactDto>>>>> GetAllEmergencyContacts()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting emergency contacts for all employees");
            
            var contactsByUserId = await _dataService.GetAllEmergencyContactsAsync();
            
            stopwatch.Stop();
            await LogOperationAsync("GetAllEmergencyContacts", $"Retrieved emergency contacts for {contactsByUserId.Count} employees", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<Dictionary<int, List<UserEmergencyContactDto>>>
            {
                Success = true,
                Message = $"Retrieved emergency contacts for {contactsByUserId.Count} employees",
                Data = contactsByUserId,
                Count = contactsByUserId.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetAllEmergencyContacts", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error retrieving emergency contacts for all employees");
            
            return StatusCode(500, new ApiResponse<Dictionary<int, List<UserEmergencyContactDto>>>
            {
                Success = false,
                Message = "An error occurred while retrieving emergency contacts",
                Count = 0
            });
        }
    }

    [HttpGet("employees/{id:int}/emergency-contacts")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<UserEmergencyContactDto>>>> GetUserEmergencyContacts(int id)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting emergency contacts for employee {EmployeeId}", id);
            
            var contacts = await _dataService.GetUserEmergencyContactsAsync(id);
            
            stopwatch.Stop();
            await LogOperationAsync("GetUserEmergencyContacts", $"Retrieved {contacts.Count} emergency contacts for employee {id}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<UserEmergencyContactDto>>
            {
                Success = true,
                Message = $"Retrieved {contacts.Count} emergency contacts",
                Data = contacts,
                Count = contacts.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetUserEmergencyContacts", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error retrieving emergency contacts for employee {EmployeeId}", id);
            
            return StatusCode(500, new ApiResponse<List<UserEmergencyContactDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving emergency contacts",
                Count = 0
            });
        }
    }

    [HttpPost("employees/{id:int}/emergency-contacts")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<UserEmergencyContactDto>>> CreateUserEmergencyContact(int id, [FromBody] CreateUserEmergencyContactRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Creating emergency contact for employee {EmployeeId}", id);
            
            var newId = await _dataService.CreateUserEmergencyContactAsync(id, request);
            
            if (newId.HasValue)
            {
                var newContact = new UserEmergencyContactDto
                {
                    XuecId = newId.Value,
                    UserId = id,
                    RelationshipId = request.RelationshipId,
                    Name = request.Name,
                    Phone = request.Phone,
                    InsertDateTime = DateTime.Now
                };
                
                stopwatch.Stop();
                await LogOperationAsync("CreateUserEmergencyContact", $"Created emergency contact {newId.Value} for employee {id}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<UserEmergencyContactDto>
                {
                    Success = true,
                    Message = "Emergency contact created successfully",
                    Data = newContact,
                    Count = 1
                });
            }
            else
            {
                stopwatch.Stop();
                return BadRequest(new ApiResponse<UserEmergencyContactDto>
                {
                    Success = false,
                    Message = "Failed to create emergency contact",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateUserEmergencyContact", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error creating emergency contact for employee {EmployeeId}", id);
            
            return StatusCode(500, new ApiResponse<UserEmergencyContactDto>
            {
                Success = false,
                Message = "An error occurred while creating emergency contact",
                Count = 0
            });
        }
    }

    [HttpPut("employees/{id:int}/emergency-contacts/{xuecId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<object>>> UpdateUserEmergencyContact(int id, int xuecId, [FromBody] UpdateUserEmergencyContactRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Updating emergency contact {XuecId} for employee {EmployeeId}", xuecId, id);
            
            // Ensure the request xuecId matches the route
            request.XuecId = xuecId;
            
            var success = await _dataService.UpdateUserEmergencyContactAsync(id, request);
            
            if (success)
            {
                stopwatch.Stop();
                await LogOperationAsync("UpdateUserEmergencyContact", $"Updated emergency contact {xuecId} for employee {id}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Emergency contact updated successfully",
                    Count = 1
                });
            }
            else
            {
                stopwatch.Stop();
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Emergency contact not found",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateUserEmergencyContact", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error updating emergency contact {XuecId} for employee {EmployeeId}", xuecId, id);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while updating emergency contact",
                Count = 0
            });
        }
    }

    [HttpDelete("employees/{id:int}/emergency-contacts/{xuecId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<object>>> DeleteUserEmergencyContact(int id, int xuecId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Deleting emergency contact {XuecId} for employee {EmployeeId}", xuecId, id);
            
            var success = await _dataService.DeleteUserEmergencyContactAsync(id, xuecId);
            
            if (success)
            {
                stopwatch.Stop();
                await LogOperationAsync("DeleteUserEmergencyContact", $"Deleted emergency contact {xuecId} for employee {id}", stopwatch.Elapsed);
                
                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Emergency contact deleted successfully",
                    Count = 1
                });
            }
            else
            {
                stopwatch.Stop();
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Emergency contact not found",
                    Count = 0
                });
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("DeleteUserEmergencyContact", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error deleting emergency contact {XuecId} for employee {EmployeeId}", xuecId, id);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while deleting emergency contact",
                Count = 0
            });
        }
    }

    #endregion

    #region Company Trades Management

    [HttpGet("companies/{xcccId:int}/trades")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<LaborRateDto>>>> GetCompanyTrades(int xcccId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting trades for company xcccId {XcccId}", xcccId);
            
            var trades = await _dataService.GetCompanyTradesAsync(xcccId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetCompanyTrades", $"Retrieved {trades.Count} trades for company {xcccId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<LaborRateDto>>
            {
                Success = true,
                Message = $"Retrieved {trades.Count} trades",
                Data = trades,
                Count = trades.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetCompanyTrades", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<LaborRateDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving company trades"
            });
        }
    }

    [HttpGet("companies/{xcccId:int}/available-trades")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<CompanyTradeDto>>>> GetAvailableTrades(int xcccId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting available trades for company xcccId {XcccId}", xcccId);
            
            var trades = await _dataService.GetAvailableTradesForCompanyAsync(xcccId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetAvailableTrades", $"Retrieved {trades.Count} available trades", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<CompanyTradeDto>>
            {
                Success = true,
                Message = $"Retrieved {trades.Count} available trades",
                Data = trades,
                Count = trades.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetAvailableTrades", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<CompanyTradeDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving available trades"
            });
        }
    }

    [HttpGet("companies/{xcccId:int}/checklists")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<CheckListDto>>>> GetCompanyChecklists(int xcccId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting checklists for company xcccId {XcccId}", xcccId);
            
            var checklists = await _dataService.GetCompanyChecklistsAsync(xcccId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetCompanyChecklists", $"Retrieved {checklists.Count} checklists", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<CheckListDto>>
            {
                Success = true,
                Message = $"Retrieved {checklists.Count} checklists",
                Data = checklists,
                Count = checklists.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetCompanyChecklists", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<CheckListDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving checklists"
            });
        }
    }

    [HttpGet("companies/{xcccId:int}/checklists/detail")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<CheckListDto>>>> GetCompanyChecklistsWithQuestions(int xcccId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting checklists with questions for company xcccId {XcccId}", xcccId);
            
            var checklists = await _dataService.GetCompanyChecklistsWithQuestionsAsync(xcccId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetCompanyChecklistsWithQuestions", $"Retrieved {checklists.Count} checklists with questions", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<CheckListDto>>
            {
                Success = true,
                Message = $"Retrieved {checklists.Count} checklists with questions",
                Data = checklists,
                Count = checklists.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetCompanyChecklistsWithQuestions", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<CheckListDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving checklists with questions"
            });
        }
    }

    [HttpGet("checklist-types")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<CheckListTypeDto>>>> GetCheckListTypes()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var types = await _dataService.GetCheckListTypesAsync();
            
            stopwatch.Stop();
            await LogOperationAsync("GetCheckListTypes", $"Retrieved {types.Count} checklist types", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<CheckListTypeDto>>
            {
                Success = true,
                Message = $"Retrieved {types.Count} checklist types",
                Data = types,
                Count = types.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetCheckListTypes", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<CheckListTypeDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving checklist types"
            });
        }
    }

    [HttpGet("checklist-answer-types")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<CheckListAnswerTypeDto>>>> GetCheckListAnswerTypes()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var types = await _dataService.GetCheckListAnswerTypesAsync();
            
            stopwatch.Stop();
            await LogOperationAsync("GetCheckListAnswerTypes", $"Retrieved {types.Count} checklist answer types", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<CheckListAnswerTypeDto>>
            {
                Success = true,
                Message = $"Retrieved {types.Count} checklist answer types",
                Data = types,
                Count = types.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetCheckListAnswerTypes", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<CheckListAnswerTypeDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving checklist answer types"
            });
        }
    }

    [HttpPost("companies/{xcccId:int}/checklists")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<CheckListDto>>> CreateCheckList(int xcccId, [FromBody] CreateCheckListRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Creating checklist '{Name}' for xcccId {XcccId}", request.ClName, xcccId);
            
            var checklist = await _dataService.CreateCheckListAsync(xcccId, request);
            
            stopwatch.Stop();
            await LogOperationAsync("CreateCheckList", $"Created checklist '{request.ClName}' for xcccId {xcccId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<CheckListDto>
            {
                Success = true,
                Message = "Checklist created successfully",
                Data = checklist,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateCheckList", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<CheckListDto>
            {
                Success = false,
                Message = "An error occurred while creating the checklist"
            });
        }
    }

    [HttpPut("checklists/{clId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<CheckListDto>>> UpdateCheckList(int clId, [FromBody] UpdateCheckListRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Updating checklist {ClId}", clId);
            
            var checklist = await _dataService.UpdateCheckListAsync(clId, request);
            
            if (checklist == null)
            {
                return NotFound(new ApiResponse<CheckListDto>
                {
                    Success = false,
                    Message = $"Checklist {clId} not found"
                });
            }
            
            stopwatch.Stop();
            await LogOperationAsync("UpdateCheckList", $"Updated checklist {clId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<CheckListDto>
            {
                Success = true,
                Message = "Checklist updated successfully",
                Data = checklist,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateCheckList", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<CheckListDto>
            {
                Success = false,
                Message = "An error occurred while updating the checklist"
            });
        }
    }

    [HttpPost("checklists/{clId:int}/questions")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<CheckListQuestionDto>>> CreateCheckListQuestion(int clId, [FromBody] CreateCheckListQuestionRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Creating question for checklist {ClId}", clId);
            
            var question = await _dataService.CreateCheckListQuestionAsync(clId, request);
            
            stopwatch.Stop();
            await LogOperationAsync("CreateCheckListQuestion", $"Created question for checklist {clId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<CheckListQuestionDto>
            {
                Success = true,
                Message = "Question created successfully",
                Data = question,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateCheckListQuestion", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<CheckListQuestionDto>
            {
                Success = false,
                Message = "An error occurred while creating the question"
            });
        }
    }

    [HttpPut("checklist-questions/{clqId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<CheckListQuestionDto>>> UpdateCheckListQuestion(int clqId, [FromBody] UpdateCheckListQuestionRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Updating question {ClqId}", clqId);
            
            var question = await _dataService.UpdateCheckListQuestionAsync(clqId, request);
            
            if (question == null)
            {
                return NotFound(new ApiResponse<CheckListQuestionDto>
                {
                    Success = false,
                    Message = $"Question {clqId} not found"
                });
            }
            
            stopwatch.Stop();
            await LogOperationAsync("UpdateCheckListQuestion", $"Updated question {clqId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<CheckListQuestionDto>
            {
                Success = true,
                Message = "Question updated successfully",
                Data = question,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateCheckListQuestion", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<CheckListQuestionDto>
            {
                Success = false,
                Message = "An error occurred while updating the question"
            });
        }
    }

    [HttpPost("companies/{xcccId:int}/checklists/clone")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<object>>> CloneCheckLists(int xcccId, [FromBody] CloneCheckListRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Cloning checklists from xcccId {Source} to {Target}", xcccId, request.TargetXcccId);
            
            await _dataService.CloneCheckListsAsync(xcccId, request.TargetXcccId);
            
            stopwatch.Stop();
            await LogOperationAsync("CloneCheckLists", $"Cloned checklists from xcccId {xcccId} to {request.TargetXcccId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Checklists cloned successfully"
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CloneCheckLists", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while cloning checklists"
            });
        }
    }

    [HttpPost("companies/{xcccId:int}/trades")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<LaborRateDto>>> CreateCompanyTrade(int xcccId, [FromBody] CreateLaborRateRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Creating trade for company xcccId {XcccId}", xcccId);
            
            var laborRate = await _dataService.CreateCompanyTradeAsync(xcccId, request);
            
            stopwatch.Stop();
            
            // Get company and trade info for audit
            var (_, companyName, tradeName) = await _dataService.GetLaborRateWithCompanyByIdAsync(laborRate.LrId);
            
            // Log critical audit with all created values
            var newValues = new Dictionary<string, object?>
            {
                { "LrDescriptionOverride", request.LrDescriptionOverride },
                { "LrNte", request.LrNte },
                { "LrRateRegular", request.LrRateRegular },
                { "LrRateOvertime", request.LrRateOvertime },
                { "LrRateHoliday", request.LrRateHoliday },
                { "LrRateSpecial", request.LrRateSpecial },
                { "LrRateScheduledAfterHours", request.LrRateScheduledAfterHours },
                { "LrRateRegularDiscount", request.LrRateRegularDiscount },
                { "LrRateRegularDiscountHoursLimit", request.LrRateRegularDiscountHoursLimit },
                { "LrRateHelper", request.LrRateHelper },
                { "LrRateHelperOvertime", request.LrRateHelperOvertime },
                { "LrRateFlat", request.LrRateFlat },
                { "LrFlatOrHourly", request.LrFlatOrHourly },
                { "LrTripCharge", request.LrTripCharge },
                { "LrMarkup", request.LrMarkup },
                { "LrNote", request.LrNote }
            };
            
            SetAuditCriticalUserContext();
            await _auditCriticalService.LogChangeAsync(
                $"Trade Created - ID: {laborRate.LrId} - {tradeName ?? "Unknown"}{(string.IsNullOrEmpty(companyName) ? "" : $" - {companyName}")}",
                new Dictionary<string, object?>(), // Empty old values for create
                newValues,
                stopwatch.Elapsed.TotalSeconds.ToString("F3")
            );
            
            await LogOperationAsync("CreateCompanyTrade", $"Created trade {laborRate.LrId} for company {xcccId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<LaborRateDto>
            {
                Success = true,
                Message = "Trade created successfully",
                Data = laborRate,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateCompanyTrade", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<LaborRateDto>
            {
                Success = false,
                Message = "An error occurred while creating trade"
            });
        }
    }

    [HttpPut("companies/{xcccId:int}/trades/{lrId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<LaborRateDto>>> UpdateCompanyTrade(int xcccId, int lrId, [FromBody] UpdateLaborRateRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Updating trade {LrId} for company xcccId {XcccId}", lrId, xcccId);
            
            // Fetch old values before update for audit comparison
            var (oldLaborRate, companyName, tradeName) = await _dataService.GetLaborRateWithCompanyByIdAsync(lrId);
            if (oldLaborRate == null)
            {
                return NotFound(new ApiResponse<LaborRateDto>
                {
                    Success = false,
                    Message = "Trade not found"
                });
            }
            
            var laborRate = await _dataService.UpdateCompanyTradeAsync(xcccId, lrId, request);
            
            if (laborRate == null)
            {
                return NotFound(new ApiResponse<LaborRateDto>
                {
                    Success = false,
                    Message = "Trade not found"
                });
            }
            
            stopwatch.Stop();
            
            // Log critical audit with change details - show all fields
            var oldValues = new Dictionary<string, object?>
            {
                { "LrDescriptionOverride", oldLaborRate.LrDescriptionOverride },
                { "LrNte", oldLaborRate.LrNte },
                { "LrRateRegular", oldLaborRate.LrRateRegular },
                { "LrRateOvertime", oldLaborRate.LrRateOvertime },
                { "LrRateHoliday", oldLaborRate.LrRateHoliday },
                { "LrRateSpecial", oldLaborRate.LrRateSpecial },
                { "LrRateScheduledAfterHours", oldLaborRate.LrRateScheduledAfterHours },
                { "LrRateRegularDiscount", oldLaborRate.LrRateRegularDiscount },
                { "LrRateRegularDiscountHoursLimit", oldLaborRate.LrRateRegularDiscountHoursLimit },
                { "LrRateHelper", oldLaborRate.LrRateHelper },
                { "LrRateHelperOvertime", oldLaborRate.LrRateHelperOvertime },
                { "LrRateFlat", oldLaborRate.LrRateFlat },
                { "LrFlatOrHourly", oldLaborRate.LrFlatOrHourly },
                { "LrTripCharge", oldLaborRate.LrTripCharge },
                { "LrMarkup", oldLaborRate.LrMarkup },
                { "LrNote", oldLaborRate.LrNote }
            };
            
            var newValues = new Dictionary<string, object?>
            {
                { "LrDescriptionOverride", request.LrDescriptionOverride },
                { "LrNte", request.LrNte },
                { "LrRateRegular", request.LrRateRegular },
                { "LrRateOvertime", request.LrRateOvertime },
                { "LrRateHoliday", request.LrRateHoliday },
                { "LrRateSpecial", request.LrRateSpecial },
                { "LrRateScheduledAfterHours", request.LrRateScheduledAfterHours },
                { "LrRateRegularDiscount", request.LrRateRegularDiscount },
                { "LrRateRegularDiscountHoursLimit", request.LrRateRegularDiscountHoursLimit },
                { "LrRateHelper", request.LrRateHelper },
                { "LrRateHelperOvertime", request.LrRateHelperOvertime },
                { "LrRateFlat", request.LrRateFlat },
                { "LrFlatOrHourly", request.LrFlatOrHourly },
                { "LrTripCharge", request.LrTripCharge },
                { "LrMarkup", request.LrMarkup },
                { "LrNote", request.LrNote }
            };
            
            SetAuditCriticalUserContext();
            await _auditCriticalService.LogChangeAsync(
                $"Trade Updated - ID: {lrId} - {tradeName ?? "Unknown"}{(string.IsNullOrEmpty(companyName) ? "" : $" - {companyName}")}",
                oldValues,
                newValues,
                stopwatch.Elapsed.TotalSeconds.ToString("F3")
            );
            
            await LogOperationAsync("UpdateCompanyTrade", $"Updated trade {lrId} for company {xcccId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<LaborRateDto>
            {
                Success = true,
                Message = "Trade updated successfully",
                Data = laborRate,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateCompanyTrade", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<LaborRateDto>
            {
                Success = false,
                Message = "An error occurred while updating trade"
            });
        }
    }

    [HttpGet("companies/{xcccId:int}/trades/{lrId:int}/checklists")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<int>>>> GetTradeChecklists(int xcccId, int lrId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting checklists for trade {LrId}", lrId);
            
            var checklistIds = await _dataService.GetTradeChecklistsAsync(lrId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetTradeChecklists", $"Retrieved {checklistIds.Count} checklists for trade {lrId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<int>>
            {
                Success = true,
                Message = $"Retrieved {checklistIds.Count} checklists",
                Data = checklistIds,
                Count = checklistIds.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetTradeChecklists", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<int>>
            {
                Success = false,
                Message = "An error occurred while retrieving trade checklists"
            });
        }
    }

    [HttpPut("companies/{xcccId:int}/trades/{lrId:int}/checklists")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<object>>> UpdateTradeChecklists(int xcccId, int lrId, [FromBody] List<int> checklistIds)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Updating checklists for trade {LrId}", lrId);
            
            // Get old checklist IDs and names before update
            var oldChecklistIds = await _dataService.GetTradeChecklistsAsync(lrId);
            var oldChecklistNames = await _dataService.GetChecklistNamesByIdsAsync(xcccId, oldChecklistIds);
            var newChecklistNames = await _dataService.GetChecklistNamesByIdsAsync(xcccId, checklistIds ?? new List<int>());
            
            // Get company and trade info for audit
            var (_, companyName, tradeName) = await _dataService.GetLaborRateWithCompanyByIdAsync(lrId);
            
            await _dataService.UpdateTradeChecklistsAsync(lrId, checklistIds ?? new List<int>());
            
            stopwatch.Stop();
            
            // Log critical audit with before/after checklist names
            var oldValues = new Dictionary<string, object?>
            {
                { "Checklists", oldChecklistNames.Any() ? string.Join(", ", oldChecklistNames) : "null" }
            };
            
            var newValues = new Dictionary<string, object?>
            {
                { "Checklists", newChecklistNames.Any() ? string.Join(", ", newChecklistNames) : "null" }
            };
            
            SetAuditCriticalUserContext();
            await _auditCriticalService.LogChangeAsync(
                $"Trade Checklists Updated - ID: {lrId} - {tradeName ?? "Unknown"}{(string.IsNullOrEmpty(companyName) ? "" : $" - {companyName}")}",
                oldValues,
                newValues,
                stopwatch.Elapsed.TotalSeconds.ToString("F3")
            );
            
            await LogOperationAsync("UpdateTradeChecklists", $"Updated checklists for trade {lrId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Checklists updated successfully",
                Count = checklistIds?.Count ?? 0
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateTradeChecklists", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while updating trade checklists"
            });
        }
    }

    #endregion

    #region Company Contacts

    [HttpGet("companies/{cId:int}/contacts")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<ContactDto>>>> GetCompanyContacts(int cId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting contacts for company c_id {CId}", cId);
            
            var contacts = await _dataService.GetCompanyContactsAsync(cId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetCompanyContacts", $"Retrieved {contacts.Count} contacts for company {cId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<ContactDto>>
            {
                Success = true,
                Message = $"Retrieved {contacts.Count} contacts",
                Data = contacts,
                Count = contacts.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetCompanyContacts", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<ContactDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving contacts"
            });
        }
    }

    [HttpGet("contact-titles")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<ContactTitleDto>>>> GetContactTitles()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting contact titles");
            
            var titles = await _dataService.GetContactTitlesAsync();
            
            stopwatch.Stop();
            await LogOperationAsync("GetContactTitles", $"Retrieved {titles.Count} contact titles", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<ContactTitleDto>>
            {
                Success = true,
                Message = $"Retrieved {titles.Count} contact titles",
                Data = titles,
                Count = titles.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetContactTitles", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<ContactTitleDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving contact titles"
            });
        }
    }

    [HttpPost("companies/{cId:int}/contacts")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<ContactDto>>> CreateContact(int cId, [FromBody] CreateContactRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Creating contact for company c_id {CId}", cId);
            
            // Validate required fields
            if (request.CtId <= 0)
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "Title is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.ConFirstname))
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "First name is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.ConLastname))
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "Last name is required"
                });
            }
            
            var contact = await _dataService.CreateContactAsync(cId, request);
            
            stopwatch.Stop();
            
            // Get company name for audit
            var (_, companyName) = await _dataService.GetContactWithCompanyByIdAsync(contact.ConId);
            
            // Log critical audit with all created values
            var newValues = new Dictionary<string, object?>
            {
                { "CtId", request.CtId },
                { "CtTitle", contact.CtTitle },
                { "ConFirstname", request.ConFirstname },
                { "ConLastname", request.ConLastname },
                { "ConEmail", request.ConEmail },
                { "ConPhone", request.ConPhone },
                { "ConMobile", request.ConMobile },
                { "ConFax", request.ConFax }
            };
            
            SetAuditCriticalUserContext();
            await _auditCriticalService.LogChangeAsync(
                $"Contact Created - ID: {contact.ConId} - {request.ConFirstname} {request.ConLastname}{(string.IsNullOrEmpty(companyName) ? "" : $" - {companyName}")}",
                new Dictionary<string, object?>(), // Empty old values for create
                newValues,
                stopwatch.Elapsed.TotalSeconds.ToString("F3")
            );
            
            await LogOperationAsync("CreateContact", $"Created contact {contact.ConId} for company {cId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<ContactDto>
            {
                Success = true,
                Message = "Contact created successfully",
                Data = contact,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateContact", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<ContactDto>
            {
                Success = false,
                Message = "An error occurred while creating contact"
            });
        }
    }

    [HttpPut("contacts/{conId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<ContactDto>>> UpdateContact(int conId, [FromBody] UpdateContactRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Updating contact {ConId}", conId);
            
            // Validate required fields
            if (request.CtId <= 0)
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "Title is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.ConFirstname))
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "First name is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.ConLastname))
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "Last name is required"
                });
            }
            
            // Fetch old values before update for audit comparison
            var (oldContact, companyName) = await _dataService.GetContactWithCompanyByIdAsync(conId);
            if (oldContact == null)
            {
                return NotFound(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "Contact not found"
                });
            }
            
            var contact = await _dataService.UpdateContactAsync(conId, request);
            
            if (contact == null)
            {
                return NotFound(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "Contact not found"
                });
            }
            
            stopwatch.Stop();
            
            // Log critical audit with change details
            var oldValues = new Dictionary<string, object?>
            {
                { "CtId", oldContact.CtId },
                { "CtTitle", oldContact.CtTitle },
                { "ConFirstname", oldContact.ConFirstname },
                { "ConLastname", oldContact.ConLastname },
                { "ConEmail", oldContact.ConEmail },
                { "ConPhone", oldContact.ConPhone },
                { "ConMobile", oldContact.ConMobile },
                { "ConFax", oldContact.ConFax }
            };
            
            var newValues = new Dictionary<string, object?>
            {
                { "CtId", request.CtId },
                { "CtTitle", contact.CtTitle },
                { "ConFirstname", request.ConFirstname },
                { "ConLastname", request.ConLastname },
                { "ConEmail", request.ConEmail },
                { "ConPhone", request.ConPhone },
                { "ConMobile", request.ConMobile },
                { "ConFax", request.ConFax }
            };
            
            SetAuditCriticalUserContext();
            await _auditCriticalService.LogChangeAsync(
                $"Contact Updated - ID: {conId} - {request.ConFirstname} {request.ConLastname}{(string.IsNullOrEmpty(companyName) ? "" : $" - {companyName}")}",
                oldValues,
                newValues,
                stopwatch.Elapsed.TotalSeconds.ToString("F3")
            );
            
            await LogOperationAsync("UpdateContact", $"Updated contact {conId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<ContactDto>
            {
                Success = true,
                Message = "Contact updated successfully",
                Data = contact,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateContact", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<ContactDto>
            {
                Success = false,
                Message = "An error occurred while updating contact"
            });
        }
    }

    #endregion

    #region Company Addresses

    [HttpGet("companies/{cId:int}/addresses")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<AddressDto>>>> GetCompanyAddresses(int cId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting addresses for company {CId}", cId);
            
            var addresses = await _dataService.GetCompanyAddressesAsync(cId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetCompanyAddresses", $"Retrieved {addresses.Count} addresses for company {cId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<AddressDto>>
            {
                Success = true,
                Message = $"Retrieved {addresses.Count} addresses",
                Data = addresses,
                Count = addresses.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetCompanyAddresses", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<AddressDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving addresses"
            });
        }
    }

    [HttpGet("address-titles")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<AddressTitleDto>>>> GetAddressTitles()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting address titles");
            
            var titles = await _dataService.GetAddressTitlesAsync();
            
            stopwatch.Stop();
            await LogOperationAsync("GetAddressTitles", $"Retrieved {titles.Count} address titles", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<AddressTitleDto>>
            {
                Success = true,
                Message = $"Retrieved {titles.Count} address titles",
                Data = titles,
                Count = titles.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetAddressTitles", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<AddressTitleDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving address titles"
            });
        }
    }

    [HttpPost("companies/{cId:int}/addresses")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<AddressDto>>> CreateAddress(int cId, [FromBody] CreateAddressRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Creating address for company {CId}", cId);
            
            // Validate required fields
            if (request.AtId <= 0 || string.IsNullOrWhiteSpace(request.AAddress1) || 
                string.IsNullOrWhiteSpace(request.ACity) || string.IsNullOrWhiteSpace(request.AState) || 
                string.IsNullOrWhiteSpace(request.AZip))
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "Title, Address 1, City, State, and Zip are required"
                });
            }
            
            var address = await _dataService.CreateAddressAsync(cId, request);
            
            // Get company name for audit
            var (addressForAudit, companyName) = await _dataService.GetAddressWithCompanyByIdAsync(address.AId);
            
            // Audit logging
            SetAuditCriticalUserContext();
            var newValues = new Dictionary<string, string>
            {
                ["AtId"] = address.AtId.ToString(),
                ["AtTitle"] = address.AtTitle ?? "",
                ["ADescription"] = address.ADescription ?? "",
                ["AAddress1"] = address.AAddress1 ?? "",
                ["AAddress2"] = address.AAddress2 ?? "",
                ["ACity"] = address.ACity ?? "",
                ["AState"] = address.AState ?? "",
                ["AZip"] = address.AZip ?? "",
                ["ALatitude"] = address.ALatitude ?? "",
                ["ALongitude"] = address.ALongitude ?? ""
            };
            
            var newValuesObj = new Dictionary<string, object?>
            {
                ["AtId"] = address.AtId,
                ["AtTitle"] = address.AtTitle,
                ["ADescription"] = address.ADescription,
                ["AAddress1"] = address.AAddress1,
                ["AAddress2"] = address.AAddress2,
                ["ACity"] = address.ACity,
                ["AState"] = address.AState,
                ["AZip"] = address.AZip,
                ["ALatitude"] = address.ALatitude,
                ["ALongitude"] = address.ALongitude
            };
            
            await _auditCriticalService.LogChangeAsync(
                $"Address Created - ID: {address.AId} - {address.AAddress1}, {address.ACity}, {address.AState} - {companyName}",
                null,
                newValuesObj,
                stopwatch.Elapsed.TotalSeconds.ToString("0.00")
            );
            
            stopwatch.Stop();
            await LogOperationAsync("CreateAddress", $"Created address {address.AId} for company {cId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<AddressDto>
            {
                Success = true,
                Message = "Address created successfully",
                Data = address,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateAddress", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<AddressDto>
            {
                Success = false,
                Message = "An error occurred while creating address"
            });
        }
    }

    [HttpPut("addresses/{aId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<AddressDto>>> UpdateAddress(int aId, [FromBody] UpdateAddressRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Updating address {AId}", aId);
            
            // Validate required fields
            if (request.AtId <= 0 || string.IsNullOrWhiteSpace(request.AAddress1) || 
                string.IsNullOrWhiteSpace(request.ACity) || string.IsNullOrWhiteSpace(request.AState) || 
                string.IsNullOrWhiteSpace(request.AZip))
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "Title, Address 1, City, State, and Zip are required"
                });
            }
            
            // Get old values for audit
            var (oldAddress, companyName) = await _dataService.GetAddressWithCompanyByIdAsync(aId);
            if (oldAddress == null)
            {
                return NotFound(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = $"Address {aId} not found"
                });
            }
            
            var address = await _dataService.UpdateAddressAsync(aId, request);
            if (address == null)
            {
                return NotFound(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = $"Address {aId} not found after update"
                });
            }
            
            // Audit logging with before/after values
            SetAuditCriticalUserContext();
            var oldValues = new Dictionary<string, object?>
            {
                ["AtId"] = oldAddress.AtId,
                ["AtTitle"] = oldAddress.AtTitle,
                ["ADescription"] = oldAddress.ADescription,
                ["AAddress1"] = oldAddress.AAddress1,
                ["AAddress2"] = oldAddress.AAddress2,
                ["ACity"] = oldAddress.ACity,
                ["AState"] = oldAddress.AState,
                ["AZip"] = oldAddress.AZip,
                ["ALatitude"] = oldAddress.ALatitude,
                ["ALongitude"] = oldAddress.ALongitude
            };
            
            var newValues = new Dictionary<string, object?>
            {
                ["AtId"] = address.AtId,
                ["AtTitle"] = address.AtTitle,
                ["ADescription"] = address.ADescription,
                ["AAddress1"] = address.AAddress1,
                ["AAddress2"] = address.AAddress2,
                ["ACity"] = address.ACity,
                ["AState"] = address.AState,
                ["AZip"] = address.AZip,
                ["ALatitude"] = address.ALatitude,
                ["ALongitude"] = address.ALongitude
            };
            
            await _auditCriticalService.LogChangeAsync(
                $"Address Updated - ID: {address.AId} - {address.AAddress1}, {address.ACity}, {address.AState} - {companyName}",
                oldValues,
                newValues,
                stopwatch.Elapsed.TotalSeconds.ToString("0.00")
            );
            
            stopwatch.Stop();
            await LogOperationAsync("UpdateAddress", $"Updated address {aId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<AddressDto>
            {
                Success = true,
                Message = "Address updated successfully",
                Data = address,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateAddress", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<AddressDto>
            {
                Success = false,
                Message = "An error occurred while updating address"
            });
        }
    }

    #endregion

    #region Company Locations

    [HttpGet("companies/{cId:int}/locations")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<LocationDto>>>> GetCompanyLocations(int cId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting locations for company {CId}", cId);
            
            var locations = await _dataService.GetCompanyLocationsAsync(cId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetCompanyLocations", $"Retrieved {locations.Count} locations for company {cId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<LocationDto>>
            {
                Success = true,
                Message = $"Retrieved {locations.Count} locations",
                Data = locations,
                Count = locations.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetCompanyLocations", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error retrieving locations for company {CId}", cId);
            
            return StatusCode(500, new ApiResponse<List<LocationDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving locations"
            });
        }
    }

    [HttpPost("companies/{cId:int}/locations")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<LocationDto>>> CreateLocation(int cId, [FromBody] CreateLocationRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Creating location for company {CId}", cId);
            
            // Validate required fields
            if (string.IsNullOrWhiteSpace(request.LLocation) || 
                string.IsNullOrWhiteSpace(request.AAddress1) ||
                string.IsNullOrWhiteSpace(request.ACity) ||
                string.IsNullOrWhiteSpace(request.AState) ||
                string.IsNullOrWhiteSpace(request.AZip))
            {
                return BadRequest(new ApiResponse<LocationDto>
                {
                    Success = false,
                    Message = "Location, Address 1, City, State, and Zip are required"
                });
            }

            var location = await _dataService.CreateLocationAsync(cId, request);
            
            stopwatch.Stop();
            await LogOperationAsync("CreateLocation", $"Created location {location.LId} for company {cId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<LocationDto>
            {
                Success = true,
                Message = "Location created successfully",
                Data = location,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateLocation", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error creating location for company {CId}", cId);
            
            return StatusCode(500, new ApiResponse<LocationDto>
            {
                Success = false,
                Message = "An error occurred while creating location"
            });
        }
    }

    [HttpPut("locations/{lId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<LocationDto>>> UpdateLocation(int lId, [FromBody] UpdateLocationRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Updating location {LId}", lId);
            
            // Validate required fields
            if (string.IsNullOrWhiteSpace(request.LLocation) || 
                string.IsNullOrWhiteSpace(request.AAddress1) ||
                string.IsNullOrWhiteSpace(request.ACity) ||
                string.IsNullOrWhiteSpace(request.AState) ||
                string.IsNullOrWhiteSpace(request.AZip))
            {
                return BadRequest(new ApiResponse<LocationDto>
                {
                    Success = false,
                    Message = "Location, Address 1, City, State, and Zip are required"
                });
            }

            var location = await _dataService.UpdateLocationAsync(lId, request);
            
            if (location == null)
            {
                return NotFound(new ApiResponse<LocationDto>
                {
                    Success = false,
                    Message = $"Location with ID {lId} not found"
                });
            }
            
            stopwatch.Stop();
            await LogOperationAsync("UpdateLocation", $"Updated location {lId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<LocationDto>
            {
                Success = true,
                Message = "Location updated successfully",
                Data = location,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateLocation", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error updating location {LId}", lId);
            
            return StatusCode(500, new ApiResponse<LocationDto>
            {
                Success = false,
                Message = "An error occurred while updating location"
            });
        }
    }

    #endregion

    #region Employee Attachments

    [HttpGet("employees/{id:int}/attachments")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<EmployeeAttachmentDto>>>> GetEmployeeAttachments(int id)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting attachments for employee {EmployeeId}", id);
            
            // Get attachments
            var attachments = await _dataService.GetEmployeeAttachmentsAsync(id);
            
            stopwatch.Stop();
            await LogOperationAsync("GetEmployeeAttachments", $"Retrieved {attachments.Count} attachments for employee {id}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<EmployeeAttachmentDto>>
            {
                Success = true,
                Message = $"Retrieved {attachments.Count} attachments",
                Data = attachments,
                Count = attachments.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetEmployeeAttachments", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error retrieving attachments for employee {EmployeeId}", id);
            
            return StatusCode(500, new ApiResponse<List<EmployeeAttachmentDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving attachments",
                Count = 0
            });
        }
    }

    [HttpGet("reports/certifications-licensing")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<CertificationsLicensingReportDto>>>> GetCertificationsLicensingReport()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting certifications and licensing report");
            
            // Get all attachments with employee and type information
            var reportData = await _dataService.GetCertificationsLicensingReportAsync();
            
            stopwatch.Stop();
            await LogOperationAsync("GetCertificationsLicensingReport", $"Retrieved {reportData.Count} attachment records", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<CertificationsLicensingReportDto>>
            {
                Success = true,
                Message = $"Retrieved {reportData.Count} certification and licensing records",
                Data = reportData,
                Count = reportData.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetCertificationsLicensingReport", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error retrieving certifications and licensing report");
            
            return StatusCode(500, new ApiResponse<List<CertificationsLicensingReportDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving the report",
                Count = 0
            });
        }
    }

    [HttpGet("reports/certifications-licensing/tech")]
    public async Task<ActionResult<ApiResponse<List<CertificationsLicensingReportDto>>>> GetTechCertificationsLicensingReport()
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting tech certifications and licensing report for user {UserId}", UserId);
            
            // Get certifications and licensing for current user only
            var reportData = await _dataService.GetTechCertificationsLicensingReportAsync(UserId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetTechCertificationsLicensingReport", $"Retrieved {reportData.Count} attachment records", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<CertificationsLicensingReportDto>>
            {
                Success = true,
                Message = $"Retrieved {reportData.Count} certification and licensing records",
                Data = reportData,
                Count = reportData.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetTechCertificationsLicensingReport", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error retrieving tech certifications and licensing report");
            
            return StatusCode(500, new ApiResponse<List<CertificationsLicensingReportDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving the report",
                Count = 0
            });
        }
    }

    [HttpGet("reports/change-history")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<dynamic>>> GetChangeHistory(
        [FromQuery] string? fromDate = null,
        [FromQuery] string? toDate = null,
        [FromQuery] string? username = null,
        [FromQuery] string? description = null,
        [FromQuery] string? objectType = null,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting change history report. FromDate: {FromDate}, ToDate: {ToDate}, Page: {Page}", fromDate, toDate, page);
            
            // Parse dates from strings
            DateTime startDate = DateTime.Now.AddDays(-30); // Default 30 days ago
            DateTime endDate = DateTime.Now;
            
            if (!string.IsNullOrEmpty(fromDate) && DateTime.TryParse(fromDate, out var parsedFromDate))
            {
                startDate = parsedFromDate;
            }
            
            if (!string.IsNullOrEmpty(toDate) && DateTime.TryParse(toDate, out var parsedToDate))
            {
                // Include the entire day by setting to end of day
                endDate = parsedToDate.AddDays(1).AddSeconds(-1);
            }
            else
            {
                // Include the entire today
                endDate = endDate.AddDays(1).AddSeconds(-1);
            }
            
            // Validate pagination
            if (page < 1) page = 1;
            if (pageSize < 1) pageSize = 50;
            if (pageSize > 500) pageSize = 500; // Cap at 500 per page
            
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                return StatusCode(500, new ApiResponse<dynamic>
                {
                    Success = false,
                    Message = "Database connection unavailable",
                    Count = 0
                });
            }

            var records = new List<dynamic>();
            int totalRecords = 0;

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                // Get total count first
                const string countSql = @"
                    SELECT COUNT(*) as TotalCount
                    FROM AuditCritical
                    WHERE ac_insertdatetime >= @StartDate
                    AND ac_insertdatetime <= @EndDate
                    AND (@Username IS NULL OR @Username = '' OR ac_username LIKE '%' + @Username + '%')
                    AND (@Description IS NULL OR @Description = '' OR ac_description LIKE '%' + @Description + '%')
                    AND (@ObjectType IS NULL OR @ObjectType = '' OR ac_description LIKE '%' + @ObjectType + '%')";

                using (var countCmd = new SqlCommand(countSql, connection))
                {
                    countCmd.Parameters.AddWithValue("@StartDate", startDate);
                    countCmd.Parameters.AddWithValue("@EndDate", endDate);
                    countCmd.Parameters.AddWithValue("@Username", (object?)username ?? DBNull.Value);
                    countCmd.Parameters.AddWithValue("@Description", (object?)description ?? DBNull.Value);
                    countCmd.Parameters.AddWithValue("@ObjectType", (object?)objectType ?? DBNull.Value);
                    
                    var result = await countCmd.ExecuteScalarAsync();
                    totalRecords = result != null ? Convert.ToInt32(result) : 0;
                }

                // Get paginated records
                const string dataSql = @"
                    SELECT 
                        ac_id,
                        ac_insertdatetime,
                        ac_username,
                        ac_name,
                        ac_description,
                        ac_detail
                    FROM AuditCritical
                    WHERE ac_insertdatetime >= @StartDate
                    AND ac_insertdatetime <= @EndDate
                    AND (@Username IS NULL OR @Username = '' OR ac_username LIKE '%' + @Username + '%')
                    AND (@Description IS NULL OR @Description = '' OR ac_description LIKE '%' + @Description + '%')
                    AND (@ObjectType IS NULL OR @ObjectType = '' OR ac_description LIKE '%' + @ObjectType + '%')
                    ORDER BY ac_insertdatetime DESC
                    OFFSET @Offset ROWS
                    FETCH NEXT @PageSize ROWS ONLY";

                using (var dataCmd = new SqlCommand(dataSql, connection))
                {
                    dataCmd.Parameters.AddWithValue("@StartDate", startDate);
                    dataCmd.Parameters.AddWithValue("@EndDate", endDate);
                    dataCmd.Parameters.AddWithValue("@Username", (object?)username ?? DBNull.Value);
                    dataCmd.Parameters.AddWithValue("@Description", (object?)description ?? DBNull.Value);
                    dataCmd.Parameters.AddWithValue("@ObjectType", (object?)objectType ?? DBNull.Value);
                    dataCmd.Parameters.AddWithValue("@Offset", (page - 1) * pageSize);
                    dataCmd.Parameters.AddWithValue("@PageSize", pageSize);

                    using (var reader = await dataCmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            records.Add(new
                            {
                                ac_id = reader["ac_id"],
                                ac_insertdatetime = reader["ac_insertdatetime"],
                                ac_username = reader["ac_username"]?.ToString() ?? string.Empty,
                                ac_name = reader["ac_name"]?.ToString() ?? string.Empty,
                                ac_description = reader["ac_description"]?.ToString() ?? string.Empty,
                                ac_detail = reader["ac_detail"]?.ToString()
                            });
                        }
                    }
                }
            }

            stopwatch.Stop();

            return Ok(new ApiResponse<dynamic>
            {
                Success = true,
                Message = $"Retrieved {records.Count} change history records",
                Data = new
                {
                    records = records,
                    pagination = new
                    {
                        currentPage = page,
                        pageSize = pageSize,
                        totalRecords = totalRecords,
                        totalPages = (int)Math.Ceiling((double)totalRecords / pageSize)
                    }
                },
                Count = totalRecords
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error retrieving change history report");

            return StatusCode(500, new ApiResponse<dynamic>
            {
                Success = false,
                Message = "An error occurred while retrieving the change history",
                Count = 0
            });
        }
    }

    [HttpPost("employees/{id:int}/attachments")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<EmployeeAttachmentDto>>> CreateEmployeeAttachment(int id)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Creating attachment for employee {EmployeeId}", id);

            // Validate request
            if (!Request.HasFormContentType)
            {
                return BadRequest(new ApiResponse<EmployeeAttachmentDto>
                {
                    Success = false,
                    Message = "Request must include multipart form data",
                    Count = 0
                });
            }

            var form = await Request.ReadFormAsync();
            var file = form.Files.FirstOrDefault();

            if (file == null || file.Length == 0)
            {
                return BadRequest(new ApiResponse<EmployeeAttachmentDto>
                {
                    Success = false,
                    Message = "No file provided",
                    Count = 0
                });
            }

            // Validate file size (5MB)
            if (file.Length > 5 * 1024 * 1024)
            {
                return BadRequest(new ApiResponse<EmployeeAttachmentDto>
                {
                    Success = false,
                    Message = "File size exceeds 5MB limit",
                    Count = 0
                });
            }

            // Validate file type
            var allowedMimes = new[] { "image/jpeg", "image/png", "image/gif", "image/webp", "application/pdf" };
            if (!allowedMimes.Contains(file.ContentType))
            {
                return BadRequest(new ApiResponse<EmployeeAttachmentDto>
                {
                    Success = false,
                    Message = "File type not allowed. Only images (JPG, PNG, GIF, WEBP) and PDF are allowed.",
                    Count = 0
                });
            }

            // Parse form fields
            string? description = form["description"];
            if (!int.TryParse(form["uat_id"], out var uatId))
            {
                return BadRequest(new ApiResponse<EmployeeAttachmentDto>
                {
                    Success = false,
                    Message = "Invalid or missing attachment type ID",
                    Count = 0
                });
            }

            string? issuingAuthority = form["xua_issuingauthority"];
            string? dateExpires = form["xua_dateexpires"];

            // TODO: In production, upload file to backend storage (Azure blob, EvoWS, etc)
            // For now, we'll use the existing EvoWS uploadAttachment flow
            // This would require integration with the file upload service

            // Create attachment (placeholder - integration point with file upload)
            int? attachmentId = await UploadFileToAttachmentServiceAsync(file, description);

            if (!attachmentId.HasValue)
            {
                stopwatch.Stop();
                // Log detailed error info to audit table for investigation
                await LogAuditAsync("CreateEmployeeAttachment", new { 
                    employeeId = id, 
                    fileName = file.FileName, 
                    fileSize = file.Length, 
                    error = "Failed to upload file to EvoWS attachment service",
                    detail = "Check application logs for UploadFileToAttachmentServiceAsync entries for detailed EvoWS response"
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
                    
                return StatusCode(500, new ApiResponse<EmployeeAttachmentDto>
                {
                    Success = false,
                    Message = "Failed to upload file to attachment service. Please check the server logs for details.",
                    Count = 0
                });
            }

            // Create xrefUserAttachment record
            var createRequest = new CreateEmployeeAttachmentRequest
            {
                description = description ?? string.Empty,
                uat_id = uatId,
                xua_issuingauthority = issuingAuthority ?? string.Empty,
                xua_dateexpires = dateExpires ?? "Not Applicable"
            };

            var xuaId = await _dataService.CreateEmployeeAttachmentAsync(id, createRequest, attachmentId.Value);

            if (!xuaId.HasValue)
            {
                stopwatch.Stop();
                await LogAuditAsync("CreateEmployeeAttachment", new { employeeId = id, attachmentId = attachmentId, error = "Failed to create employee attachment record in database" }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
                
                return StatusCode(500, new ApiResponse<EmployeeAttachmentDto>
                {
                    Success = false,
                    Message = "Failed to create employee attachment record",
                    Count = 0
                });
            }

            // Retrieve the created attachment
            var attachments = await _dataService.GetEmployeeAttachmentsAsync(id);
            var createdAttachment = attachments.FirstOrDefault(a => a.xua_id == xuaId.Value);

            stopwatch.Stop();
            await LogOperationAsync("CreateEmployeeAttachment", $"Created attachment {xuaId} for employee {id}", stopwatch.Elapsed);

            return Ok(new ApiResponse<EmployeeAttachmentDto>
            {
                Success = true,
                Message = "Attachment created successfully",
                Data = createdAttachment,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateEmployeeAttachment", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "CreateEmployeeAttachment: Error creating attachment for employee {EmployeeId} - Message: {Message}, Type: {ExceptionType}", 
                id, ex.Message, ex.GetType().Name);
            
            return StatusCode(500, new ApiResponse<EmployeeAttachmentDto>
            {
                Success = false,
                Message = $"An error occurred while creating the attachment: {ex.Message}",
                Count = 0
            });
        }
    }

    [HttpPut("employees/{id:int}/attachments/{xuaId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<EmployeeAttachmentDto>>> UpdateEmployeeAttachment(int id, int xuaId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Updating attachment {XuaId} for employee {EmployeeId}", xuaId, id);

            if (!Request.HasFormContentType)
            {
                return BadRequest(new ApiResponse<EmployeeAttachmentDto>
                {
                    Success = false,
                    Message = "Request must include multipart form data",
                    Count = 0
                });
            }

            var form = await Request.ReadFormAsync();
            var file = form.Files.FirstOrDefault();

            // Parse form fields
            string? description = form["description"];
            if (!int.TryParse(form["uat_id"], out var uatId))
            {
                return BadRequest(new ApiResponse<EmployeeAttachmentDto>
                {
                    Success = false,
                    Message = "Invalid or missing attachment type ID",
                    Count = 0
                });
            }

            string? issuingAuthority = form["xua_issuingauthority"];
            string? dateExpires = form["xua_dateexpires"];

            int? newAttachmentId = null;

            // If file provided, upload it and replace
            if (file != null && file.Length > 0)
            {
                // Validate file size (5MB)
                if (file.Length > 5 * 1024 * 1024)
                {
                    return BadRequest(new ApiResponse<EmployeeAttachmentDto>
                    {
                        Success = false,
                        Message = "File size exceeds 5MB limit",
                        Count = 0
                    });
                }

                // Validate file type
                var allowedMimes = new[] { "image/jpeg", "image/png", "image/gif", "image/webp", "application/pdf" };
                if (!allowedMimes.Contains(file.ContentType))
                {
                    return BadRequest(new ApiResponse<EmployeeAttachmentDto>
                    {
                        Success = false,
                        Message = "File type not allowed. Only images (JPG, PNG, GIF, WEBP) and PDF are allowed.",
                        Count = 0
                    });
                }

                // Upload new file
                newAttachmentId = await UploadFileToAttachmentServiceAsync(file, description);

                if (!newAttachmentId.HasValue)
                {
                    return StatusCode(500, new ApiResponse<EmployeeAttachmentDto>
                    {
                        Success = false,
                        Message = "Failed to upload file",
                        Count = 0
                    });
                }
            }

            // Update the xrefUserAttachment record
            var updateRequest = new UpdateEmployeeAttachmentRequest
            {
                description = description ?? string.Empty,
                uat_id = uatId,
                xua_issuingauthority = issuingAuthority ?? string.Empty,
                xua_dateexpires = dateExpires ?? "Not Applicable"
            };

            var success = await _dataService.UpdateEmployeeAttachmentAsync(id, xuaId, updateRequest, newAttachmentId);

            if (!success)
            {
                return NotFound(new ApiResponse<EmployeeAttachmentDto>
                {
                    Success = false,
                    Message = "Attachment not found or update failed",
                    Count = 0
                });
            }

            // Retrieve the updated attachment
            var attachments = await _dataService.GetEmployeeAttachmentsAsync(id);
            var updatedAttachment = attachments.FirstOrDefault(a => a.xua_id == xuaId);

            stopwatch.Stop();
            await LogOperationAsync("UpdateEmployeeAttachment", $"Updated attachment {xuaId} for employee {id}", stopwatch.Elapsed);

            return Ok(new ApiResponse<EmployeeAttachmentDto>
            {
                Success = true,
                Message = "Attachment updated successfully",
                Data = updatedAttachment,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateEmployeeAttachment", ex, stopwatch.Elapsed);
            
            _logger.LogError(ex, "Error updating attachment {XuaId} for employee {EmployeeId}", xuaId, id);
            
            return StatusCode(500, new ApiResponse<EmployeeAttachmentDto>
            {
                Success = false,
                Message = "An error occurred while updating the attachment",
                Count = 0
            });
        }
    }

    // TODO: Implement file upload integration with EvoWS or Azure blob storage
    private async Task<int?> UploadFileToAttachmentServiceAsync(IFormFile file, string? description)
    {
        if (file == null || file.Length == 0)
        {
            var emptyFileError = "File is null or empty";
            _logger.LogWarning("UploadFileToAttachmentServiceAsync: {Message}", emptyFileError);
            await LogAuditAsync("UploadFileToAttachmentServiceAsync", new { 
                stage = "validation",
                fileName = file?.FileName,
                fileSize = file?.Length ?? 0,
                error = emptyFileError
            });
            return null;
        }

        try
        {
            var uploadStartTime = DateTime.UtcNow;
            _logger.LogInformation("UploadFileToAttachmentServiceAsync: Starting upload - FileName: {FileName}, ContentType: {ContentType}, Size: {FileSize} bytes", 
                file.FileName, file.ContentType, file.Length);

            // Create multipart form data to send to EvoWS ProcessAttachments endpoint
            using (var form = new MultipartFormDataContent())
            {
                // Add file to form
                var fileContent = new StreamContent(file.OpenReadStream());
                fileContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(file.ContentType);
                form.Add(fileContent, "file1", file.FileName);

                // Add metadata
                form.Add(new StringContent(UserId.ToString()), "u_id_submittedby");
                form.Add(new StringContent(UserFullName), "submittedby");
                form.Add(new StringContent(description ?? file.FileName), "Description");
                form.Add(new StringContent("0"), "sr_id"); // 0 = employee attachment (not related to service request)

                // Determine EvoWS base URL from configuration or use default
                var evoWSBaseUrl = _configuration["EvoWS:BaseUrl"] ?? "https://localhost:44307";
                var uploadUrl = $"{evoWSBaseUrl}/ws/api/file/ProcessAttachments";

                // Log configuration details for debugging
                _logger.LogInformation("UploadFileToAttachmentServiceAsync: Environment configuration - EvoWS:BaseUrl config value: '{EvoWSBaseUrl}' (empty={IsEmpty}, null={IsNull})", 
                    evoWSBaseUrl ?? "(null)", string.IsNullOrEmpty(evoWSBaseUrl), evoWSBaseUrl == null);
                _logger.LogInformation("UploadFileToAttachmentServiceAsync: Resolved upload URL - {UploadUrl}", uploadUrl);

                _logger.LogInformation("UploadFileToAttachmentServiceAsync: Sending POST request to EvoWS - URL: {UploadUrl}, UserId: {UserId}, UserName: {UserName}", 
                    uploadUrl, UserId, UserFullName);

                // Call EvoWS ProcessAttachments endpoint
                HttpResponseMessage response = null;
                string responseContent = string.Empty;
                
                try
                {
                    response = await _httpClient.PostAsync(uploadUrl, form);
                    responseContent = await response.Content.ReadAsStringAsync();

                    _logger.LogInformation("UploadFileToAttachmentServiceAsync: EvoWS response - StatusCode: {StatusCode}, ReasonPhrase: {ReasonPhrase}, ContentLength: {ContentLength}", 
                        response.StatusCode, response.ReasonPhrase, responseContent?.Length ?? 0);
                    
                    // ALWAYS log the full response content for debugging
                    _logger.LogInformation("UploadFileToAttachmentServiceAsync: Full EvoWS response content: {ResponseContent}", responseContent ?? "(empty)");
                }
                catch (HttpRequestException httpEx)
                {
                    var httpErrorMsg = $"HTTP request failed: {httpEx.Message} | InnerException: {httpEx.InnerException?.Message}";
                    _logger.LogError(httpEx, "UploadFileToAttachmentServiceAsync: {Message}", httpErrorMsg);
                    
                    // Log to audit table
                    await LogAuditAsync("UploadFileToAttachmentServiceAsync", new { 
                        stage = "http_request",
                        fileName = file.FileName,
                        fileSize = file.Length,
                        uploadUrl = uploadUrl,
                        userId = UserId,
                        userName = UserFullName,
                        error = httpErrorMsg,
                        exceptionType = httpEx.GetType().Name
                    });
                    throw;
                }

                if (!response.IsSuccessStatusCode)
                {
                    var failureMsg = $"EvoWS ProcessAttachments failed with status {response.StatusCode} ({response.ReasonPhrase}). Response: {responseContent}";
                    _logger.LogError("UploadFileToAttachmentServiceAsync: {Message}", failureMsg);
                    
                    // Log to audit table with full error details
                    await LogAuditAsync("UploadFileToAttachmentServiceAsync", new { 
                        stage = "evows_error_response",
                        fileName = file.FileName,
                        fileSize = file.Length,
                        uploadUrl = uploadUrl,
                        userId = UserId,
                        userName = UserFullName,
                        httpStatusCode = response.StatusCode.ToString(),
                        reasonPhrase = response.ReasonPhrase,
                        evoWSResponse = responseContent
                    });
                    return null;
                }

                // Parse response from EvoWS
                // Expected response: { att_id: number, FileName: string, ... }
                try
                {
                    if (string.IsNullOrEmpty(responseContent))
                    {
                        var emptyResponseMsg = "EvoWS returned empty response content";
                        _logger.LogWarning("UploadFileToAttachmentServiceAsync: {Message}", emptyResponseMsg);
                        
                        await LogAuditAsync("UploadFileToAttachmentServiceAsync", new { 
                            stage = "empty_response",
                            fileName = file.FileName,
                            fileSize = file.Length,
                            uploadUrl = uploadUrl,
                            userId = UserId,
                            userName = UserFullName,
                            error = emptyResponseMsg
                        });
                        return null;
                    }

                    using (JsonDocument doc = JsonDocument.Parse(responseContent))
                    {
                        var root = doc.RootElement;
                        
                        // Extract att_id from response
                        if (root.TryGetProperty("att_id", out JsonElement attIdElement))
                        {
                            if (int.TryParse(attIdElement.GetRawText(), out int attId))
                            {
                                var durationMs = (DateTime.UtcNow - uploadStartTime).TotalMilliseconds;
                                _logger.LogInformation("UploadFileToAttachmentServiceAsync: File uploaded successfully - att_id: {AttId}, Duration: {DurationMs}ms", attId, durationMs);
                                
                                // Log success to audit table
                                await LogAuditAsync("UploadFileToAttachmentServiceAsync", new { 
                                    stage = "success",
                                    fileName = file.FileName,
                                    fileSize = file.Length,
                                    uploadUrl = uploadUrl,
                                    userId = UserId,
                                    userName = UserFullName,
                                    attId = attId,
                                    durationMs = durationMs
                                });
                                
                                return attId;
                            }
                        }

                        var parseErrorMsg = "att_id not found or not parseable in response";
                        _logger.LogWarning("UploadFileToAttachmentServiceAsync: {Message} - Response: {Response}", parseErrorMsg, responseContent);
                        
                        await LogAuditAsync("UploadFileToAttachmentServiceAsync", new { 
                            stage = "parse_error",
                            fileName = file.FileName,
                            fileSize = file.Length,
                            uploadUrl = uploadUrl,
                            userId = UserId,
                            userName = UserFullName,
                            error = parseErrorMsg,
                            evoWSResponse = responseContent
                        });
                        return null;
                    }
                }
                catch (System.Text.Json.JsonException jsonEx)
                {
                    var jsonErrorMsg = $"Failed to parse EvoWS response as JSON: {jsonEx.Message}";
                    _logger.LogError(jsonEx, "UploadFileToAttachmentServiceAsync: {Message} - Response: {Response}", jsonErrorMsg, responseContent);
                    
                    await LogAuditAsync("UploadFileToAttachmentServiceAsync", new { 
                        stage = "json_parse_error",
                        fileName = file.FileName,
                        fileSize = file.Length,
                        uploadUrl = uploadUrl,
                        userId = UserId,
                        userName = UserFullName,
                        error = jsonErrorMsg,
                        exceptionType = jsonEx.GetType().Name,
                        evoWSResponse = responseContent
                    });
                    return null;
                }
            }
        }
        catch (HttpRequestException httpEx)
        {
            var httpErrorMsg = $"HTTP request error: {httpEx.Message} | InnerException: {httpEx.InnerException?.Message}";
            _logger.LogError(httpEx, "UploadFileToAttachmentServiceAsync: {Message}", httpErrorMsg);
            
            await LogAuditAsync("UploadFileToAttachmentServiceAsync", new { 
                stage = "http_exception",
                fileName = file.FileName,
                fileSize = file.Length,
                userId = UserId,
                userName = UserFullName,
                error = httpErrorMsg,
                exceptionType = httpEx.GetType().Name
            });
            return null;
        }
        catch (Exception ex)
        {
            var unexpectedErrorMsg = $"Unexpected error: {ex.Message} | Type: {ex.GetType().Name}";
            _logger.LogError(ex, "UploadFileToAttachmentServiceAsync: {Message}", unexpectedErrorMsg);
            
            // Log configuration for debugging
            var evoWSBaseUrl = _configuration["EvoWS:BaseUrl"] ?? "https://localhost:44307";
            _logger.LogError("UploadFileToAttachmentServiceAsync: Configuration state at exception - EvoWS:BaseUrl='{EvoWSBaseUrl}' (null={IsNull}, empty={IsEmpty}), Full URL would be: {FullUrl}", 
                evoWSBaseUrl ?? "(null)", evoWSBaseUrl == null, string.IsNullOrEmpty(evoWSBaseUrl), 
                string.IsNullOrEmpty(evoWSBaseUrl) ? "(would be invalid)" : $"{evoWSBaseUrl}/ws/api/file/ProcessAttachments");
            
            await LogAuditAsync("UploadFileToAttachmentServiceAsync", new { 
                stage = "unexpected_exception",
                fileName = file.FileName,
                fileSize = file.Length,
                userId = UserId,
                userName = UserFullName,
                error = unexpectedErrorMsg,
                exceptionType = ex.GetType().Name,
                stackTrace = ex.StackTrace,
                configuredBaseUrl = evoWSBaseUrl,
                baseUrlIsEmpty = string.IsNullOrEmpty(evoWSBaseUrl)
            });
            return null;
        }
    }

    #endregion

    #region Call Center Contacts

    [HttpGet("callcenters/{ccId:int}/contacts")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<ContactDto>>>> GetCallCenterContacts(int ccId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting contacts for call center cc_id {CcId}", ccId);
            
            var contacts = await _dataService.GetCallCenterContactsAsync(ccId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetCallCenterContacts", $"Retrieved {contacts.Count} contacts for call center {ccId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<ContactDto>>
            {
                Success = true,
                Message = $"Retrieved {contacts.Count} contacts",
                Data = contacts,
                Count = contacts.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetCallCenterContacts", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<ContactDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving contacts"
            });
        }
    }

    [HttpPost("callcenters/{ccId:int}/contacts")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<ContactDto>>> CreateCallCenterContact(int ccId, [FromBody] CreateContactRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Creating contact for call center cc_id {CcId}", ccId);
            
            // Validate required fields
            if (request.CtId <= 0)
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "Title is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.ConFirstname) && string.IsNullOrWhiteSpace(request.ConLastname))
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "At least first name or last name is required"
                });
            }
            
            // Email validation if provided
            if (!string.IsNullOrWhiteSpace(request.ConEmail) && !IsValidEmail(request.ConEmail))
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "Invalid email format"
                });
            }
            
            var contact = await _dataService.CreateCallCenterContactAsync(ccId, request);
            
            stopwatch.Stop();
            
            // Log critical audit with new contact details
            var newValues = new Dictionary<string, object?>
            {
                { "CallCenterId", ccId },
                { "Title", contact.CtTitle },
                { "FirstName", contact.ConFirstname },
                { "LastName", contact.ConLastname },
                { "Email", contact.ConEmail },
                { "Phone", contact.ConPhone },
                { "Mobile", contact.ConMobile },
                { "Fax", contact.ConFax }
            };
            
            SetAuditCriticalUserContext();
            await _auditCriticalService.LogChangeAsync(
                $"Contact Created - {contact.ConFirstname} {contact.ConLastname} (ID: {contact.ConId})",
                new Dictionary<string, object?>(), // Empty old values for create
                newValues,
                stopwatch.Elapsed.TotalSeconds.ToString("F3")
            );
            
            await LogOperationAsync("CreateCallCenterContact", $"Created contact {contact.ConId} for call center {ccId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<ContactDto>
            {
                Success = true,
                Message = "Contact created successfully",
                Data = contact,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateCallCenterContact", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<ContactDto>
            {
                Success = false,
                Message = "An error occurred while creating contact"
            });
        }
    }

    [HttpPut("callcenters/contacts/{conId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<ContactDto>>> UpdateCallCenterContact(int conId, [FromBody] UpdateContactRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Updating call center contact {ConId}", conId);
            
            // Validate required fields
            if (request.CtId <= 0)
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "Title is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.ConFirstname) && string.IsNullOrWhiteSpace(request.ConLastname))
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "At least first name or last name is required"
                });
            }
            
            // Email validation if provided
            if (!string.IsNullOrWhiteSpace(request.ConEmail) && !IsValidEmail(request.ConEmail))
            {
                return BadRequest(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "Invalid email format"
                });
            }
            
            // Get current contact data for audit logging (need to get all contacts to find this one)
            // We'll need to fetch from all call centers since we don't have ccId in this endpoint
            var allCallCentersDataTable = await _dataService.GetAllCallCentersAsync();
            var allCallCenters = ConvertDataTableToCallCenters(allCallCentersDataTable);
            ContactDto? currentContact = null;
            
            // Find the contact across all call centers
            foreach (var cc in allCallCenters ?? new List<CallCenterDto>())
            {
                var contacts = await _dataService.GetCallCenterContactsAsync(cc.Id);
                currentContact = contacts.FirstOrDefault(c => c.ConId == conId);
                if (currentContact != null) break;
            }
            
            var contact = await _dataService.UpdateCallCenterContactAsync(conId, request);
            
            if (contact == null)
            {
                return NotFound(new ApiResponse<ContactDto>
                {
                    Success = false,
                    Message = "Contact not found"
                });
            }
            
            stopwatch.Stop();
            
            // Log critical audit with change details
            var oldValues = new Dictionary<string, object?>
            {
                { "Title", currentContact?.CtTitle },
                { "FirstName", currentContact?.ConFirstname },
                { "LastName", currentContact?.ConLastname },
                { "Email", currentContact?.ConEmail },
                { "Phone", currentContact?.ConPhone },
                { "Mobile", currentContact?.ConMobile },
                { "Fax", currentContact?.ConFax }
            };
            
            var newValues = new Dictionary<string, object?>
            {
                { "Title", contact.CtTitle },
                { "FirstName", contact.ConFirstname },
                { "LastName", contact.ConLastname },
                { "Email", contact.ConEmail },
                { "Phone", contact.ConPhone },
                { "Mobile", contact.ConMobile },
                { "Fax", contact.ConFax }
            };
            
            SetAuditCriticalUserContext();
            await _auditCriticalService.LogChangeAsync(
                $"Contact Updated - {contact.ConFirstname} {contact.ConLastname} (ID: {conId})",
                oldValues,
                newValues,
                stopwatch.Elapsed.TotalSeconds.ToString("F3")
            );
            
            await LogOperationAsync("UpdateCallCenterContact", $"Updated contact {conId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<ContactDto>
            {
                Success = true,
                Message = "Contact updated successfully",
                Data = contact,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateCallCenterContact", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<ContactDto>
            {
                Success = false,
                Message = "An error occurred while updating contact"
            });
        }
    }

    [HttpDelete("callcenters/{ccId:int}/contacts/{conId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<object>>> DeleteCallCenterContactXref(int ccId, int conId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Deleting contact xref for call center {CcId} and contact {ConId}", ccId, conId);
            
            // Get contact details before deletion for audit logging
            var contacts = await _dataService.GetCallCenterContactsAsync(ccId);
            var contactToDelete = contacts.FirstOrDefault(c => c.ConId == conId);
            
            var deleted = await _dataService.DeleteCallCenterContactXrefAsync(ccId, conId);
            
            if (!deleted)
            {
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Contact association not found"
                });
            }
            
            stopwatch.Stop();
            
            // Log critical audit with deleted contact details
            if (contactToDelete != null)
            {
                var oldValues = new Dictionary<string, object?>
                {
                    { "CallCenterId", ccId },
                    { "ContactId", conId },
                    { "Title", contactToDelete.CtTitle },
                    { "FirstName", contactToDelete.ConFirstname },
                    { "LastName", contactToDelete.ConLastname },
                    { "Email", contactToDelete.ConEmail },
                    { "Phone", contactToDelete.ConPhone },
                    { "Mobile", contactToDelete.ConMobile },
                    { "Fax", contactToDelete.ConFax }
                };
                
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync(
                    $"Contact Deleted - {contactToDelete.ConFirstname} {contactToDelete.ConLastname} (ID: {conId})",
                    oldValues,
                    new Dictionary<string, object?>(), // Empty new values for delete
                    stopwatch.Elapsed.TotalSeconds.ToString("F3")
                );
            }
            
            await LogOperationAsync("DeleteCallCenterContactXref", $"Deleted contact xref for call center {ccId} and contact {conId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Contact removed from call center successfully"
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("DeleteCallCenterContactXref", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while deleting contact association"
            });
        }
    }

    #endregion

    #region Call Center Addresses

    [HttpGet("callcenters/{ccId:int}/addresses")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<List<AddressDto>>>> GetCallCenterAddresses(int ccId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Getting addresses for call center cc_id {CcId}", ccId);
            
            var addresses = await _dataService.GetCallCenterAddressesAsync(ccId);
            
            stopwatch.Stop();
            await LogOperationAsync("GetCallCenterAddresses", $"Retrieved {addresses.Count} addresses for call center {ccId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<List<AddressDto>>
            {
                Success = true,
                Message = $"Retrieved {addresses.Count} addresses",
                Data = addresses,
                Count = addresses.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("GetCallCenterAddresses", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<List<AddressDto>>
            {
                Success = false,
                Message = "An error occurred while retrieving addresses"
            });
        }
    }

    [HttpPost("callcenters/{ccId:int}/addresses")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<AddressDto>>> CreateCallCenterAddress(int ccId, [FromBody] CreateAddressRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Creating address for call center cc_id {CcId}", ccId);
            
            // Validate required fields
            if (request.AtId <= 0)
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "Title is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.AAddress1))
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "Address 1 is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.ACity))
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "City is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.AState))
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "State is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.AZip))
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "Zip is required"
                });
            }
            
            var address = await _dataService.CreateCallCenterAddressAsync(ccId, request);
            
            stopwatch.Stop();
            
            // Log critical audit with new address details
            var newValues = new Dictionary<string, object?>
            {
                { "CallCenterId", ccId },
                { "Title", address.AtTitle },
                { "Address1", address.AAddress1 },
                { "Address2", address.AAddress2 },
                { "City", address.ACity },
                { "State", address.AState },
                { "Zip", address.AZip },
                { "Latitude", address.ALatitude },
                { "Longitude", address.ALongitude }
            };
            
            SetAuditCriticalUserContext();
            await _auditCriticalService.LogChangeAsync(
                $"Address Created - {address.AAddress1}, {address.ACity}, {address.AState} (ID: {address.AId})",
                new Dictionary<string, object?>(), // Empty old values for create
                newValues,
                stopwatch.Elapsed.TotalSeconds.ToString("F3")
            );
            
            await LogOperationAsync("CreateCallCenterAddress", $"Created address {address.AId} for call center {ccId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<AddressDto>
            {
                Success = true,
                Message = "Address created successfully",
                Data = address,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("CreateCallCenterAddress", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<AddressDto>
            {
                Success = false,
                Message = "An error occurred while creating address"
            });
        }
    }

    [HttpPut("callcenters/addresses/{aId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<AddressDto>>> UpdateCallCenterAddress(int aId, [FromBody] UpdateAddressRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Updating call center address {AId}", aId);
            
            // Validate required fields
            if (request.AtId <= 0)
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "Title is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.AAddress1))
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "Address 1 is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.ACity))
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "City is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.AState))
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "State is required"
                });
            }
            
            if (string.IsNullOrWhiteSpace(request.AZip))
            {
                return BadRequest(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "Zip is required"
                });
            }
            
            // Get current address data for audit logging (need to get all addresses to find this one)
            var allCallCentersDataTable = await _dataService.GetAllCallCentersAsync();
            var allCallCenters = ConvertDataTableToCallCenters(allCallCentersDataTable);
            AddressDto? currentAddress = null;
            
            // Find the address across all call centers
            foreach (var cc in allCallCenters ?? new List<CallCenterDto>())
            {
                var addresses = await _dataService.GetCallCenterAddressesAsync(cc.Id);
                currentAddress = addresses.FirstOrDefault(a => a.AId == aId);
                if (currentAddress != null) break;
            }
            
            var address = await _dataService.UpdateCallCenterAddressAsync(aId, request);
            
            if (address == null)
            {
                return NotFound(new ApiResponse<AddressDto>
                {
                    Success = false,
                    Message = "Address not found"
                });
            }
            
            stopwatch.Stop();
            
            // Log critical audit with change details
            var oldValues = new Dictionary<string, object?>
            {
                { "Title", currentAddress?.AtTitle },
                { "Address1", currentAddress?.AAddress1 },
                { "Address2", currentAddress?.AAddress2 },
                { "City", currentAddress?.ACity },
                { "State", currentAddress?.AState },
                { "Zip", currentAddress?.AZip },
                { "Latitude", currentAddress?.ALatitude },
                { "Longitude", currentAddress?.ALongitude }
            };
            
            var newValues = new Dictionary<string, object?>
            {
                { "Title", address.AtTitle },
                { "Address1", address.AAddress1 },
                { "Address2", address.AAddress2 },
                { "City", address.ACity },
                { "State", address.AState },
                { "Zip", address.AZip },
                { "Latitude", address.ALatitude },
                { "Longitude", address.ALongitude }
            };
            
            SetAuditCriticalUserContext();
            await _auditCriticalService.LogChangeAsync(
                $"Address Updated - {address.AAddress1}, {address.ACity}, {address.AState} (ID: {aId})",
                oldValues,
                newValues,
                stopwatch.Elapsed.TotalSeconds.ToString("F3")
            );
            
            await LogOperationAsync("UpdateCallCenterAddress", $"Updated address {aId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<AddressDto>
            {
                Success = true,
                Message = "Address updated successfully",
                Data = address,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("UpdateCallCenterAddress", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<AddressDto>
            {
                Success = false,
                Message = "An error occurred while updating address"
            });
        }
    }

    [HttpDelete("callcenters/{ccId:int}/addresses/{aId:int}")]
    [EvoAuthorize]
    public async Task<ActionResult<ApiResponse<object>>> DeleteCallCenterAddressXref(int ccId, int aId)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            _logger.LogInformation("Deleting address xref for call center {CcId} and address {AId}", ccId, aId);
            
            // Get address details before deletion for audit logging
            var addresses = await _dataService.GetCallCenterAddressesAsync(ccId);
            var addressToDelete = addresses.FirstOrDefault(a => a.AId == aId);
            
            var deleted = await _dataService.DeleteCallCenterAddressXrefAsync(ccId, aId);
            
            if (!deleted)
            {
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Address association not found"
                });
            }
            
            stopwatch.Stop();
            
            // Log critical audit with deleted address details
            if (addressToDelete != null)
            {
                var oldValues = new Dictionary<string, object?>
                {
                    { "CallCenterId", ccId },
                    { "AddressId", aId },
                    { "Title", addressToDelete.AtTitle },
                    { "Address1", addressToDelete.AAddress1 },
                    { "Address2", addressToDelete.AAddress2 },
                    { "City", addressToDelete.ACity },
                    { "State", addressToDelete.AState },
                    { "Zip", addressToDelete.AZip },
                    { "Latitude", addressToDelete.ALatitude },
                    { "Longitude", addressToDelete.ALongitude }
                };
                
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync(
                    $"Address Deleted - {addressToDelete.AAddress1}, {addressToDelete.ACity}, {addressToDelete.AState} (ID: {aId})",
                    oldValues,
                    new Dictionary<string, object?>(), // Empty new values for delete
                    stopwatch.Elapsed.TotalSeconds.ToString("F3")
                );
            }
            
            await LogOperationAsync("DeleteCallCenterAddressXref", $"Deleted address xref for call center {ccId} and address {aId}", stopwatch.Elapsed);
            
            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Address removed from call center successfully"
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogErrorAsync("DeleteCallCenterAddressXref", ex, stopwatch.Elapsed);
            
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = "An error occurred while deleting address association"
            });
        }
    }

    #endregion

    #endregion
}
