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


    /// Get work orders with optional number of days filter

    /// <param name="numberOfDays">Number of days to retrieve (default: 30, max: 1500)</param>
    /// <returns>List of work orders</returns>
    [HttpGet("workorders")]
    [ProducesResponseType(typeof(ApiResponse<List<WorkOrderDto>>), 200)]
    [ProducesResponseType(typeof(ApiResponse<object>), 400)]
    [ProducesResponseType(typeof(ApiResponse<object>), 500)]
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


    /// Get work orders schedule with optional number of days filter

    /// <param name="numberOfDays">Number of days to retrieve (default: 30, max: 1500)</param>
    /// <returns>List of scheduled work orders</returns>
    [HttpGet("workorders/schedule")]
    [ProducesResponseType(typeof(ApiResponse<List<WorkOrderDto>>), 200)]
    [ProducesResponseType(typeof(ApiResponse<object>), 400)]
    [ProducesResponseType(typeof(ApiResponse<object>), 500)]
    public async Task<ActionResult<ApiResponse<List<WorkOrderDto>>>> GetWorkOrdersSchedule([FromQuery] int numberOfDays = 30)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            _logger.LogInformation("Getting work orders schedule for {NumberOfDays} days", numberOfDays);
            
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


    /// POST version of GetWorkOrders for complex filters

    /// <param name="request">Work order request parameters</param>
    /// <returns>List of work orders</returns>
    [HttpPost("workorders")]
    [ProducesResponseType(typeof(ApiResponse<List<WorkOrderDto>>), 200)]
    [ProducesResponseType(typeof(ApiResponse<object>), 400)]
    [ProducesResponseType(typeof(ApiResponse<object>), 500)]
    public async Task<ActionResult<ApiResponse<List<WorkOrderDto>>>> GetWorkOrdersPost([FromBody] WorkOrderRequest request)
    {
        return await GetWorkOrders(request.NumberOfDays);
    }


    /// POST version of GetWorkOrdersSchedule for complex filters

    /// <param name="request">Work order request parameters</param>
    /// <returns>List of scheduled work orders</returns>
    [HttpPost("workorders/schedule")]
    [ProducesResponseType(typeof(ApiResponse<List<WorkOrderDto>>), 200)]
    [ProducesResponseType(typeof(ApiResponse<object>), 400)]
    [ProducesResponseType(typeof(ApiResponse<object>), 500)]
    public async Task<ActionResult<ApiResponse<List<WorkOrderDto>>>> GetWorkOrdersSchedulePost([FromBody] WorkOrderRequest request)
    {
        return await GetWorkOrdersSchedule(request.NumberOfDays);
    }

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
