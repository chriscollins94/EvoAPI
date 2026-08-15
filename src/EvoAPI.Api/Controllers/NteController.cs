using System.Diagnostics;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using Microsoft.AspNetCore.Mvc;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("EvoApi/nte")]
public class NteController : BaseController
{
    private readonly INteNotificationService _nteService;

    public NteController(INteNotificationService nteService, IAuditService auditService)
    {
        _nteService = nteService;
        InitializeAuditService(auditService);
    }

    // Manually triggers a single NTE notification scan. Used for smoke testing
    // without waiting for the background service interval.
    [HttpPost("run-once")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<NteScanResult>>> RunOnce(CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var result = await _nteService.RunScanAsync(ct);
            stopwatch.Stop();
            await LogAuditAsync("NteRunOnce", result, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<NteScanResult>
            {
                Success = true,
                Message = $"NTE scan complete. Scanned={result.RowsScanned}, Sms={result.SmsSent}, Email={result.EmailsSent}, Skipped={result.Skipped}, Failed={result.Failed}.",
                Data = result,
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("NteRunOnce", ex);
            return StatusCode(500, new ApiResponse<NteScanResult>
            {
                Success = false,
                Message = $"NTE scan failed: {ex.Message}"
            });
        }
    }
}
