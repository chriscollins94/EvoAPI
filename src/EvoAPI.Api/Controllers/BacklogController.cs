using System.Data;
using System.Diagnostics;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Models;
using Microsoft.AspNetCore.Mvc;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("EvoApi/backlog")]
[AttackPointsOnly]
public class BacklogController : BaseController
{
    private readonly IDataService _dataService;

    public BacklogController(IDataService dataService, IAuditService auditService)
    {
        _dataService = dataService;
        InitializeAuditService(auditService);
    }

    [HttpGet]
    public async Task<ActionResult<ApiResponse<List<BacklogItemDto>>>> GetBacklogItems([FromQuery] bool includeInactive = false)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var dataTable = await _dataService.GetBacklogItemsAsync(includeInactive);
            var items = ConvertDataTableToBacklogItems(dataTable);

            stopwatch.Stop();
            await LogAuditAsync("GetBacklogItems", new { includeInactive, count = items.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<List<BacklogItemDto>>
            {
                Success = true,
                Message = "Backlog items retrieved successfully",
                Data = items,
                Count = items.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetBacklogItems", ex, new { includeInactive });

            return StatusCode(500, new ApiResponse<List<BacklogItemDto>>
            {
                Success = false,
                Message = "Failed to retrieve backlog items"
            });
        }
    }

    [HttpGet("{id:int}")]
    public async Task<ActionResult<ApiResponse<BacklogItemDto>>> GetBacklogItemById(int id)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var dataTable = await _dataService.GetBacklogItemByIdAsync(id);
            var item = ConvertDataTableToBacklogItems(dataTable).FirstOrDefault();

            stopwatch.Stop();

            if (item == null)
            {
                return NotFound(new ApiResponse<BacklogItemDto>
                {
                    Success = false,
                    Message = $"Backlog item with ID {id} not found"
                });
            }

            await LogAuditAsync("GetBacklogItemById", new { id }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<BacklogItemDto>
            {
                Success = true,
                Message = "Backlog item retrieved successfully",
                Data = item,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetBacklogItemById", ex, new { id });

            return StatusCode(500, new ApiResponse<BacklogItemDto>
            {
                Success = false,
                Message = "Failed to retrieve backlog item"
            });
        }
    }

    [HttpPost]
    public async Task<ActionResult<ApiResponse<int>>> CreateBacklogItem([FromBody] CreateBacklogItemRequest request)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            if (string.IsNullOrWhiteSpace(request.Code) || string.IsNullOrWhiteSpace(request.Title))
            {
                return BadRequest(new ApiResponse<int>
                {
                    Success = false,
                    Message = "Code and title are required"
                });
            }

            var newId = await _dataService.CreateBacklogItemAsync(request, UserId, Username);
            stopwatch.Stop();

            if (!newId.HasValue)
            {
                return StatusCode(500, new ApiResponse<int>
                {
                    Success = false,
                    Message = "Failed to create backlog item"
                });
            }

            await LogAuditAsync("CreateBacklogItem", new { newId, request.Code, request.Title }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<int>
            {
                Success = true,
                Message = "Backlog item created successfully",
                Data = newId.Value,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("CreateBacklogItem", ex, request);

            return StatusCode(500, new ApiResponse<int>
            {
                Success = false,
                Message = "Failed to create backlog item"
            });
        }
    }

    [HttpPut("{id:int}")]
    public async Task<ActionResult<ApiResponse<bool>>> UpdateBacklogItem(int id, [FromBody] UpdateBacklogItemRequest request)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            if (id != request.BacklogItemId)
            {
                return BadRequest(new ApiResponse<bool>
                {
                    Success = false,
                    Message = "ID mismatch"
                });
            }

            if (string.IsNullOrWhiteSpace(request.Code) || string.IsNullOrWhiteSpace(request.Title))
            {
                return BadRequest(new ApiResponse<bool>
                {
                    Success = false,
                    Message = "Code and title are required"
                });
            }

            var updated = await _dataService.UpdateBacklogItemAsync(id, request, UserId, Username);
            stopwatch.Stop();

            if (!updated)
            {
                return NotFound(new ApiResponse<bool>
                {
                    Success = false,
                    Message = $"Backlog item with ID {id} not found"
                });
            }

            await LogAuditAsync("UpdateBacklogItem", new { id, request.Code, request.Title }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<bool>
            {
                Success = true,
                Message = "Backlog item updated successfully",
                Data = true,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("UpdateBacklogItem", ex, new { id, request });

            return StatusCode(500, new ApiResponse<bool>
            {
                Success = false,
                Message = "Failed to update backlog item"
            });
        }
    }

    [HttpDelete("{id:int}")]
    public async Task<ActionResult<ApiResponse<bool>>> DeleteBacklogItem(int id)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var deleted = await _dataService.DeleteBacklogItemAsync(id, UserId, Username);
            stopwatch.Stop();

            if (!deleted)
            {
                return NotFound(new ApiResponse<bool>
                {
                    Success = false,
                    Message = $"Backlog item with ID {id} not found"
                });
            }

            await LogAuditAsync("DeleteBacklogItem", new { id }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<bool>
            {
                Success = true,
                Message = "Backlog item archived successfully",
                Data = true,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("DeleteBacklogItem", ex, new { id });

            return StatusCode(500, new ApiResponse<bool>
            {
                Success = false,
                Message = "Failed to archive backlog item"
            });
        }
    }

    private static List<BacklogItemDto> ConvertDataTableToBacklogItems(DataTable dataTable)
    {
        var items = new List<BacklogItemDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            items.Add(new BacklogItemDto
            {
                BacklogItemId = ConvertToInt(row["backlogItemId"]),
                Code = row["code"]?.ToString() ?? string.Empty,
                Version = NullIfEmpty(row["version"]?.ToString()),
                Title = row["title"]?.ToString() ?? string.Empty,
                Source = NullIfEmpty(row["source"]?.ToString()),
                Author = NullIfEmpty(row["author"]?.ToString()),
                Date = NullIfEmpty(row["date"]?.ToString()),
                Category = NullIfEmpty(row["category"]?.ToString()),
                Type = row["type"]?.ToString() ?? string.Empty,
                Priority = row["priority"]?.ToString() ?? string.Empty,
                Status = row["status"]?.ToString() ?? string.Empty,
                Effort = NullIfEmpty(row["effort"]?.ToString()),
                Desc = NullIfEmpty(row["desc"]?.ToString()),
                Notes = NullIfEmpty(row["notes"]?.ToString()),
                DetailMarkdown = NullIfEmpty(row["detailMarkdown"]?.ToString()),
                History = NullIfEmpty(row["history"]?.ToString()),
                Active = ConvertToBool(row["active"]),
                InsertDateTime = NullIfEmpty(row["insertDateTime"]?.ToString()),
                LastUpdated = NullIfEmpty(row["lastUpdated"]?.ToString()),
                LastUpdatedByUserId = ConvertToNullableInt(row["lastUpdatedByUserId"])
            });
        }

        return items;
    }

    private static int ConvertToInt(object value)
    {
        if (value == null || value == DBNull.Value)
        {
            return 0;
        }

        return int.TryParse(value.ToString(), out var result) ? result : 0;
    }

    private static int? ConvertToNullableInt(object value)
    {
        if (value == null || value == DBNull.Value)
        {
            return null;
        }

        return int.TryParse(value.ToString(), out var result) ? result : null;
    }

    private static bool ConvertToBool(object value)
    {
        if (value == null || value == DBNull.Value)
        {
            return false;
        }

        return bool.TryParse(value.ToString(), out var result) ? result : value.ToString() == "1";
    }

    private static string? NullIfEmpty(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
