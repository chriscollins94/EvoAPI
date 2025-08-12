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
    #endregion


    #region Helper Methods

    private static List<WorkOrderDto> ConvertDataTableToWorkOrders(DataTable dataTable)
    {
        var workOrders = new List<WorkOrderDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            var workOrder = new WorkOrderDto
            {
                sr_id = Convert.ToInt32(row["sr_id"]),
                CallCenter = row["CallCenter"]?.ToString() ?? string.Empty,
                Company = row["Company"]?.ToString() ?? string.Empty,
                Trade = row["Trade"]?.ToString() ?? string.Empty,
                StartDate = row["StartDate"]?.ToString() ?? string.Empty,
                EndDate = row["EndDate"]?.ToString() ?? string.Empty,
                RequestNumber = row["RequestNumber"]?.ToString() ?? string.Empty,
                TotalDue = row["TotalDue"] != DBNull.Value ? Convert.ToDecimal(row["TotalDue"]) : null,
                Status = row["Status"]?.ToString() ?? string.Empty,
                SecondaryStatus = row["SecondaryStatus"]?.ToString() ?? string.Empty,
                StatusColor = row["StatusColor"]?.ToString() ?? string.Empty,
                AssignedFirstName = row["AssignedFirstName"]?.ToString() ?? string.Empty,
                AssignedLastName = row["AssignedLastName"]?.ToString() ?? string.Empty,
                Location = row["Location"]?.ToString() ?? string.Empty,
                Address = row["Address"]?.ToString() ?? string.Empty,
                City = row["City"]?.ToString() ?? string.Empty,
                Zone = row["Zone"]?.ToString() ?? string.Empty,
                CreatedBy = row["CreatedBy"]?.ToString() ?? string.Empty
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
                PriorityName = row["PriorityName"]?.ToString() ?? string.Empty,
                Order = row["Order"] != DBNull.Value ? Convert.ToInt32(row["Order"]) : null,
                Color = row["Color"]?.ToString(),
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
                StatusSecondary = row["StatusSecondary"]?.ToString() ?? string.Empty,
                Color = row["Color"]?.ToString(),
                Code = row["Code"]?.ToString(),
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
