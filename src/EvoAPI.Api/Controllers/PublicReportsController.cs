using EvoAPI.Api.Services;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.RateLimiting;
using Microsoft.Extensions.Caching.Memory;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace EvoAPI.Api.Controllers;

/// <summary>
/// Anonymous, key-protected read of the Metro Pipe Program Report for people outside the
/// company who have no login. This is the only anonymous data endpoint in the API, so it
/// lives in its own controller with no class-level [EvoAuthorize] to make that visible.
///
/// Access control is a shared key. The viewer's page URL carries it as ?k=, but the page sends
/// it to this API in the X-Report-Key header rather than the query string, so it never lands in
/// the audit table (the audit middleware records the full URL on error responses) or in any
/// proxy log. The key is an 8-character value in ConfigSetting (cs_type 'MetroPipe',
/// cs_identifier 'PublicReportKey'); rotate it with an UPDATE and the old link stops working
/// within a minute. A wrong or missing key gets a 404, never a 401: the evotech front end treats
/// every 401 as an expired login and bounces to the login page, which an outside visitor must
/// not see. The route is rate limited per client and globally (Program.cs, policy
/// "PublicReport") so the key cannot be brute forced.
///
/// The request carries no other input. The report queries take no parameters, so nothing a
/// caller sends reaches SQL. The report payload is cached briefly so refresh-happy viewers do
/// not hammer the database.
/// </summary>
[ApiController]
[Route("EvoApi/public")]
[AllowAnonymous]
[EnableRateLimiting("PublicReport")]
public class PublicReportsController : BaseController
{
    public const string RateLimitPolicy = "PublicReport";
    public const string KeyHeader = "X-Report-Key";

    private const string KeyCacheEntry = "MetroPipe.PublicReportKey";
    private const string ReportCacheEntry = "MetroPipe.PublicReport";
    private static readonly TimeSpan KeyCacheFor = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan ReportCacheFor = TimeSpan.FromSeconds(30);

    private readonly IDataService _dataService;
    private readonly IMemoryCache _cache;
    private readonly ILogger<PublicReportsController> _logger;

    public PublicReportsController(
        IDataService dataService,
        IMemoryCache cache,
        IAuditService auditService,
        ILogger<PublicReportsController> logger)
    {
        _dataService = dataService;
        _cache = cache;
        _logger = logger;
        InitializeAuditService(auditService);
    }

    /// <summary>
    /// Both halves of the Metro Pipe report in one call: the High Volume tech rows exactly as
    /// GET reports/high-volume returns them, and the XRF / crew / wave summary exactly as
    /// GET reports/metro-pipe returns it.
    /// </summary>
    [HttpGet("metro-pipe")]
    public async Task<ActionResult<ApiResponse<PublicMetroPipeReportDto>>> GetMetroPipeReport([FromHeader(Name = KeyHeader)] string? key)
    {
        var stopwatch = Stopwatch.StartNew();

        if (!await KeyIsValidAsync(key))
        {
            await LogAuditAsync("PublicMetroPipeReport - Rejected", $"ip={ClientIp()} keyLength={key?.Length ?? 0}");
            return NotFound();
        }

        try
        {
            var report = await _cache.GetOrCreateAsync(ReportCacheEntry, async entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = ReportCacheFor;
                var hvTask = _dataService.GetHighVolumeDashboardAsync();
                var metroTask = _dataService.GetMetroPipeSummaryAsync();
                await Task.WhenAll(hvTask, metroTask);
                return new PublicMetroPipeReportDto
                {
                    HighVolume = MetroPipeReportMapper.ToHighVolumeReport(hvTask.Result),
                    MetroPipe = MetroPipeReportMapper.ToMetroPipeReport(metroTask.Result)
                };
            });

            stopwatch.Stop();
            await LogAuditAsync("PublicMetroPipeReport", $"ip={ClientIp()}", stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<PublicMetroPipeReportDto>
            {
                Success = true,
                Message = "Metro Pipe report retrieved successfully",
                Data = report,
                Count = report?.HighVolume.Count ?? 0
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("PublicMetroPipeReport", ex);
            _logger.LogError(ex, "Error retrieving public Metro Pipe report");
            return StatusCode(500, new ApiResponse<PublicMetroPipeReportDto>
            {
                Success = false,
                Message = "Failed to retrieve Metro Pipe report"
            });
        }
    }

    /// Constant-time, case-sensitive comparison against the configured key. An empty or
    /// missing configured key disables the endpoint entirely.
    private async Task<bool> KeyIsValidAsync(string? supplied)
    {
        if (string.IsNullOrWhiteSpace(supplied) || supplied.Length > 64) return false;

        var configured = await _cache.GetOrCreateAsync(KeyCacheEntry, async entry =>
        {
            entry.AbsoluteExpirationRelativeToNow = KeyCacheFor;
            var value = await _dataService.GetConfigSettingValueAsync("MetroPipe", "PublicReportKey");
            return value?.Trim() ?? string.Empty;
        });

        if (string.IsNullOrEmpty(configured)) return false;

        var a = Encoding.UTF8.GetBytes(configured);
        var b = Encoding.UTF8.GetBytes(supplied.Trim());
        return a.Length == b.Length && CryptographicOperations.FixedTimeEquals(a, b);
    }

    private string ClientIp() => ClientAddress.Resolve(HttpContext);
}
