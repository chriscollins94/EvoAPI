using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Data;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("[controller]")]
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
                await LogErrorAsync("GetWorkOrders", ex, stopwatch.Elapsed);
                
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
    public async Task<ActionResult<ApiResponse<List<WorkOrderDto>>>> GetWorkOrdersSchedule([FromQuery] int numberOfDays = 30)
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
            var dataTable = await _dataService.GetWorkOrdersScheduleAsync(numberOfDays);
            var workOrders = ConvertDataTableToWorkOrders(dataTable);

            stopwatch.Stop();

            // Log successful operation
            await LogOperationAsync("GetWorkOrdersSchedule", $"Retrieved {workOrders.Count} scheduled work orders for {numberOfDays} days", stopwatch.Elapsed);

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
            await LogErrorAsync("GetWorkOrdersSchedule", ex, stopwatch.Elapsed);

            _logger.LogError(ex, "Error retrieving work orders schedule for {NumberOfDays} days", numberOfDays);

            return StatusCode(500, new ApiResponse<object>
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
    #endregion


    #region Helper Methods

    private static string CleanString(object value)
    {
        return value?.ToString()?.Trim()?.Replace("\r\n", "").Replace("\n", "").Replace("\r", "") ?? string.Empty;
    }

    private static List<WorkOrderDto> ConvertDataTableToWorkOrders(DataTable dataTable)
    {
        var workOrders = new List<WorkOrderDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var workOrder = new WorkOrderDto
            {
                sr_id = Convert.ToInt32(row["sr_id"]),
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
                Zone = CleanString(row["Zone"]),
                CreatedBy = CleanString(row["CreatedBy"])
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
                FirstName = row["FirstName"]?.ToString(),
                LastName = row["LastName"]?.ToString(),
                Email = row["Email"]?.ToString(),
                Active = Convert.ToBoolean(row["Active"]),
                ZoneId = row["ZoneId"] != DBNull.Value ? Convert.ToInt32(row["ZoneId"]) : null
            };

            users.Add(user);
        }

        return users;
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

    #endregion
}
