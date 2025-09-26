using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Data;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("EvoApi")]
[Authorize]
public class EvoApiController : BaseController
{
    #region Initialize

    private readonly IDataService _dataService;
        private readonly ILogger<EvoApiController> _logger;
    
        public EvoApiController(
            IDataService dataService, 
            IAuditService auditService,
            ILogger<EvoApiController> logger)
        {
            _dataService = dataService;
            _logger = logger;
            InitializeAuditService(auditService);
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

        [HttpGet("zones")]
        public async Task<ActionResult<ApiResponse<List<ZoneDto>>>> GetZones()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting zones");
                
                var dataTable = await _dataService.GetAllZonesAsync();
                var zones = ConvertDataTableToZones(dataTable);
                
                stopwatch.Stop();
                await LogOperationAsync("GetZones", $"Retrieved {zones.Count} zones", stopwatch.Elapsed);
                
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
        public async Task<ActionResult<ApiResponse<EmployeeManagementDto>>> GetEmployees()
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            
            try
            {
                _logger.LogInformation("Getting all employees for management with optimized single query");
                
                // Get all employee data including roles in a single query (OPTIMIZED!)
                var employeesWithRolesDataTable = await _dataService.GetAllEmployeesWithRolesAsync();
                var employees = ConvertDataTableToEmployeesWithRoles(employeesWithRolesDataTable);

                // Get zones
                var zonesDataTable = await _dataService.GetAllZonesAsync();
                var zones = ConvertDataTableToZones(zonesDataTable);

                // Get roles
                var rolesDataTable = await _dataService.GetAllRolesAsync();
                var roles = ConvertDataTableToRoles(rolesDataTable);
                
                stopwatch.Stop();
                await LogAuditAsync("GetEmployees", $"Retrieved {employees.Count} employees with optimized query", stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                
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

                // Update call center
                var success = await _dataService.UpdateCallCenterAsync(request);
                
                stopwatch.Stop();
                
                if (success)
                {
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
                Attack = Convert.ToInt32(row["Attack"])
            };

            callCenters.Add(callCenter);
        }

        return callCenters;
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
                UserId = Convert.ToInt32(row["UserId"])
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
                Password = row["Password"]?.ToString() ?? string.Empty,
                FirstName = row["FirstName"]?.ToString(),
                LastName = row["LastName"]?.ToString(),
                EmployeeNumber = row["EmployeeNumber"]?.ToString(),
                Email = row["Email"]?.ToString(),
                PhoneHome = row["PhoneHome"]?.ToString(),
                PhoneMobile = row["PhoneMobile"]?.ToString(),
                PhoneDesk = row["PhoneDesk"]?.ToString(),
                Extension = row["Extension"]?.ToString(),
                Active = Convert.ToBoolean(row["Active"]),
                Picture = row["Picture"]?.ToString(),
                SSN = row["SSN"]?.ToString(),
                DateOfHire = row["DateOfHire"] != DBNull.Value ? Convert.ToDateTime(row["DateOfHire"]) : null,
                DateEligiblePTO = row["DateEligiblePTO"] != DBNull.Value ? Convert.ToDateTime(row["DateEligiblePTO"]) : null,
                DateEligibleVacation = row["DateEligibleVacation"] != DBNull.Value ? Convert.ToDateTime(row["DateEligibleVacation"]) : null,
                DaysAvailablePTO = row["DaysAvailablePTO"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailablePTO"]) : null,
                DaysAvailableVacation = row["DaysAvailableVacation"] != DBNull.Value ? Convert.ToDecimal(row["DaysAvailableVacation"]) : null,
                ClothingShirt = row["ClothingShirt"]?.ToString(),
                ClothingJacket = row["ClothingJacket"]?.ToString(),
                ClothingPants = row["ClothingPants"]?.ToString(),
                WirelessProvider = row["WirelessProvider"]?.ToString(),
                PreferredNotification = row["PreferredNotification"]?.ToString(),
                QuickBooksName = row["QuickBooksName"]?.ToString(),
                PasswordChanged = row["PasswordChanged"] != DBNull.Value ? Convert.ToDateTime(row["PasswordChanged"]) : null,
                U_2FA = row["U_2FA"] != DBNull.Value ? Convert.ToBoolean(row["U_2FA"]) : false,
                ZoneId = row["ZoneId"] != DBNull.Value ? Convert.ToInt32(row["ZoneId"]) : null,
                CovidVaccineDate = row["CovidVaccineDate"] != DBNull.Value ? Convert.ToDateTime(row["CovidVaccineDate"]) : null,
                Note = row["Note"]?.ToString(),
                NoteDashboard = row["NoteDashboard"]?.ToString()
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
                wo_insertdatetime = ConvertToDateTime(row["wo_insertdatetime"]),
                t_trade = CleanString(row["t_trade"]),
                c_name = CleanString(row["c_name"]),
                wo_startdatetime = ConvertToDateTime(row["wo_startdatetime"])
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
                Zip = row["Zip"]?.ToString()
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

    #endregion

    #endregion
}
