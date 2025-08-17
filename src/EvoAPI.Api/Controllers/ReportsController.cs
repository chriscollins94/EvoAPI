using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Models;
using EvoAPI.Shared.Attributes;
using Microsoft.AspNetCore.Mvc;
using System.Data;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("EvoApi/reports")]
[EvoAuthorize]
public class ReportsController : BaseController
{
    #region Initialize

    private readonly IDataService _dataService;
    private readonly ILogger<ReportsController> _logger;

    public ReportsController(
        IDataService dataService, 
        IAuditService auditService,
        ILogger<ReportsController> logger)
    {
        _dataService = dataService;
        _logger = logger;
        InitializeAuditService(auditService);
    }

    #endregion

    #region Get

    [HttpGet("high-volume")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<List<HighVolumeReportDto>>>> GetHighVolumeReport()
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            var dataTable = await _dataService.GetHighVolumeDashboardAsync();
            var reportData = ConvertDataTableToHighVolumeReport(dataTable);
            
            stopwatch.Stop();
            
            await LogAuditAsync("GetHighVolumeReport", $"Retrieved {reportData.Count} records", stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
            
            return Ok(new ApiResponse<List<HighVolumeReportDto>>
            {
                Success = true,
                Message = "High volume report data retrieved successfully",
                Data = reportData,
                Count = reportData.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetHighVolumeReport", ex);
            
            return StatusCode(500, new ApiResponse<List<HighVolumeReportDto>>
            {
                Success = false,
                Message = "Failed to retrieve high volume report data"
            });
        }
    }

    #endregion

    #region Helper Methods

    private static List<HighVolumeReportDto> ConvertDataTableToHighVolumeReport(DataTable dataTable)
    {
        var result = new List<HighVolumeReportDto>();
        
        foreach (DataRow row in dataTable.Rows)
        {
            result.Add(new HighVolumeReportDto
            {
                Tech = CleanString(row["Tech"]),
                Today = ConvertToInt(row["Today"]),
                Previous1 = ConvertToInt(row["Previous_1"]),
                Previous2 = ConvertToInt(row["Previous_2"]),
                Previous3 = ConvertToInt(row["Previous_3"]),
                Previous4 = ConvertToInt(row["Previous_4"]),
                TodayName = CleanString(row["Today_Name"]),
                Previous1Name = CleanString(row["Previous_1_Name"]),
                Previous2Name = CleanString(row["Previous_2_Name"]),
                Previous3Name = CleanString(row["Previous_3_Name"]),
                Previous4Name = CleanString(row["Previous_4_Name"]),
                NotCompleted = ConvertToInt(row["NotCompleted"])
            });
        }
        
        return result;
    }

    private static int ConvertToInt(object value)
    {
        if (value == null || value == DBNull.Value)
            return 0;
        
        if (int.TryParse(value.ToString(), out var result))
            return result;
            
        return 0;
    }

    private static string CleanString(object value)
    {
        return value?.ToString()?.Trim() ?? string.Empty;
    }

    #endregion
}
