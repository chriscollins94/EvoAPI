using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("EvoApi/quickbooks")]
public class QuickBooksController : BaseController
{
    private readonly IQuickBooksService _quickBooksService;
    private readonly IDataService _dataService;

    public QuickBooksController(
        IQuickBooksService quickBooksService,
        IDataService dataService,
        IAuditService auditService)
    {
        _quickBooksService = quickBooksService;
        _dataService = dataService;
        InitializeAuditService(auditService);
    }

    [HttpGet("servicerequest/{requestNumber}/check")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<QuickBooksInvoiceCheckDto>>> CheckInvoice(string requestNumber)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            if (string.IsNullOrWhiteSpace(requestNumber))
            {
                return BadRequest(new ApiResponse<QuickBooksInvoiceCheckDto>
                {
                    Success = false,
                    Message = "Request number is required"
                });
            }

            var result = await _quickBooksService.CheckInvoiceByRequestNumberAsync(requestNumber.Trim());

            stopwatch.Stop();
            await LogAuditAsync("QuickBooksCheckInvoice", new { requestNumber, result.isMismatch, result.error }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<QuickBooksInvoiceCheckDto>
            {
                Success = true,
                Message = result.error ?? (result.isMismatch ? "Sync token mismatch" : "Sync tokens match"),
                Data = result,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("QuickBooksCheckInvoice", ex, new { requestNumber });
            return StatusCode(500, new ApiResponse<QuickBooksInvoiceCheckDto>
            {
                Success = false,
                Message = $"Failed to check invoice: {ex.Message}"
            });
        }
    }

    [HttpPut("servicerequest/{srId}/synctoken")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<object>>> UpdateSyncToken(int srId, [FromBody] UpdateQuickBooksSyncTokenRequest request)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            if (srId <= 0)
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Valid sr_id is required"
                });
            }

            if (request == null || string.IsNullOrWhiteSpace(request.SyncToken))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "syncToken is required"
                });
            }

            var success = await _dataService.UpdateServiceRequestSyncTokenAsync(srId, request.SyncToken.Trim());

            stopwatch.Stop();
            await LogAuditAsync("QuickBooksUpdateSyncToken", new { srId, request.SyncToken, success }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            if (!success)
            {
                return NotFound(new ApiResponse<object>
                {
                    Success = false,
                    Message = "Service request not found or no rows updated"
                });
            }

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = "Sync token updated"
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("QuickBooksUpdateSyncToken", ex, new { srId, request?.SyncToken });
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = $"Failed to update sync token: {ex.Message}"
            });
        }
    }
}
