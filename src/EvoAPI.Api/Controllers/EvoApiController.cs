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
