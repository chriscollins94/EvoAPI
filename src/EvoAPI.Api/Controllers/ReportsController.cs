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

    [HttpGet("receipts")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<List<ReceiptsReportDto>>>> GetReceiptsReport()
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            var dataTable = await _dataService.GetReceiptsDashboardAsync();
            var reportData = ConvertDataTableToReceiptsReport(dataTable);
            
            stopwatch.Stop();
            
            await LogAuditAsync("GetReceiptsReport", $"Retrieved {reportData.Count} records", stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
            
            return Ok(new ApiResponse<List<ReceiptsReportDto>>
            {
                Success = true,
                Message = "Receipts report data retrieved successfully",
                Data = reportData,
                Count = reportData.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetReceiptsReport", ex);
            
            return StatusCode(500, new ApiResponse<List<ReceiptsReportDto>>
            {
                Success = false,
                Message = "Failed to retrieve receipts report data"
            });
        }
    }

    [HttpGet("tech-detail")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<List<TechDetailReportDto>>>> GetTechDetailReport()
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            var dataTable = await _dataService.GetTechDetailDashboardAsync();
            var reportData = ConvertDataTableToTechDetailReport(dataTable);
            
            stopwatch.Stop();
            
            await LogAuditAsync("GetTechDetailReport", $"Retrieved {reportData.Count} records", stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
            
            return Ok(new ApiResponse<List<TechDetailReportDto>>
            {
                Success = true,
                Message = "Tech detail report data retrieved successfully",
                Data = reportData,
                Count = reportData.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetTechDetailReport", ex);
            
            return StatusCode(500, new ApiResponse<List<TechDetailReportDto>>
            {
                Success = false,
                Message = "Failed to retrieve tech detail report data"
            });
        }
    }

    [HttpGet("tech-activity")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<List<TechActivityReportDto>>>> GetTechActivityReport()
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            var dataTable = await _dataService.GetTechActivityDashboardAsync();
            var reportData = ConvertDataTableToTechActivityReport(dataTable);
            
            stopwatch.Stop();
            
            await LogAuditAsync("GetTechActivityReport", $"Retrieved {reportData.Count} records", stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
            
            return Ok(new ApiResponse<List<TechActivityReportDto>>
            {
                Success = true,
                Message = "Tech activity report data retrieved successfully",
                Data = reportData,
                Count = reportData.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetTechActivityReport", ex);
            
            return StatusCode(500, new ApiResponse<List<TechActivityReportDto>>
            {
                Success = false,
                Message = "Failed to retrieve tech activity report data"
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

    private static List<ReceiptsReportDto> ConvertDataTableToReceiptsReport(DataTable dataTable)
    {
        var result = new List<ReceiptsReportDto>();
        
        foreach (DataRow row in dataTable.Rows)
        {
            result.Add(new ReceiptsReportDto
            {
                CallCenter = CleanString(row["cc_name"]),
                Company = CleanString(row["c_name"]),
                ReceiptType = CleanString(row["rt_receipttype"]),
                Supplier = CleanString(row["Supplier"]),
                SupplierEntered = CleanString(row["SupplierEntered"]),
                RequestNumber = CleanString(row["sr_requestnumber"]),
                TechFirstName = CleanString(row["u_firstname"]),
                TechLastName = CleanString(row["u_lastname"]),
                SubmittedBy = CleanString(row["SubmittedBy"]),
                ReceiptAmount = ConvertToDecimal(row["att_receiptamount"]),
                InsertDateTime = ConvertToDateTime(row["att_insertdatetime"]) ?? DateTime.MinValue,
                FileName = CleanString(row["att_filename"]),
                Description = CleanString(row["att_description"]),
                Comment = CleanString(row["att_comment"]),
                Path = CleanString(row["att_path"]),
                ServiceRequestId = ConvertToInt(row["sr_id"]),
                WorkOrderId = ConvertToInt(row["wo_id"]),
                AttachmentId = ConvertToInt(row["att_id"]),
                Extension = CleanString(row["att_extension"]),
                TradeId = ConvertToNullableInt(row["t_id"]),
                Trade = CleanString(row["t_trade"])
            });
        }
        
        return result;
    }

    private static List<TechDetailReportDto> ConvertDataTableToTechDetailReport(DataTable dataTable)
    {
        var result = new List<TechDetailReportDto>();
        
        foreach (DataRow row in dataTable.Rows)
        {
            result.Add(new TechDetailReportDto
            {
                UserId = ConvertToInt(row["u_id"]),
                FirstName = CleanString(row["u_firstname"]),
                LastName = CleanString(row["u_lastname"]),
                PerformanceId = ConvertToInt(row["perf_id"]),
                PerformanceDate = ConvertToDateTime(row["perf_insertdate"]),
                Utilization = CleanString(row["perf_utilization"]),
                Profitability = CleanString(row["perf_profitability"]),
                Attendance = CleanString(row["perf_attendance"]),
                Comment = CleanString(row["perf_comment"]),
                Address1 = CleanString(row["a_address1"]),
                Address2 = CleanString(row["a_address2"]),
                City = CleanString(row["a_city"]),
                State = CleanString(row["a_state"]),
                Zip = CleanString(row["a_zip"]),
                ZoneId = ConvertToNullableInt(row["z_id"]),
                ZoneNumber = CleanString(row["z_number"])
            });
        }
        
        return result;
    }

    private static List<TechActivityReportDto> ConvertDataTableToTechActivityReport(DataTable dataTable)
    {
        var result = new List<TechActivityReportDto>();
        
        foreach (DataRow row in dataTable.Rows)
        {
            result.Add(new TechActivityReportDto
            {
                TtId = ConvertToInt(row["tt_id"]),
                TttId = ConvertToInt(row["ttt_id"]),
                UserId = ConvertToInt(row["u_id"]),
                WorkOrderId = ConvertToNullableInt(row["wo_id"]),
                ServiceRequestId = ConvertToNullableInt(row["sr_id"]),
                TradeId = ConvertToNullableInt(row["t_id"]),
                TimeType = CleanString(row["ttt_timetype"]),
                PaidTime = CleanString(row["ttt_paidtime"]),
                BeginTime = CleanString(row["tt_begin"]),
                EndTime = CleanString(row["tt_end"]),
                InvoicedRate = ConvertToDecimal(row["tt_invoicedrate"]),
                FirstName = CleanString(row["u_firstname"]),
                LastName = CleanString(row["u_lastname"]),
                ServiceRequestNumber = CleanString(row["sr_requestnumber"]),
                Trade = CleanString(row["t_trade"]),
                CallCenter = CleanString(row["cc_name"]),
                Company = CleanString(row["c_name"])
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

    private static int? ConvertToNullableInt(object value)
    {
        if (value == null || value == DBNull.Value)
            return null;
        
        if (int.TryParse(value.ToString(), out var result))
            return result;
            
        return null;
    }

    private static decimal ConvertToDecimal(object value)
    {
        if (value == null || value == DBNull.Value)
            return 0m;
        
        if (decimal.TryParse(value.ToString(), out var result))
            return result;
            
        return 0m;
    }

    private static DateTime? ConvertToDateTime(object value)
    {
        if (value == null || value == DBNull.Value)
            return null;
        
        if (DateTime.TryParse(value.ToString(), out var result))
            return result;
            
        return null;
    }

    private static string CleanString(object value)
    {
        return value?.ToString()?.Trim() ?? string.Empty;
    }

    #endregion
}
