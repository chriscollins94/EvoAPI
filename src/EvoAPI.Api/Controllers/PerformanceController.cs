using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Models;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using EvoAPI.Infrastructure.Repositories;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers
{
    /// <summary>
    /// Admin performance dashboard and uploads. Every endpoint here exposes other people's
    /// data (named technicians, zones, upload history), so the class-level policy is
    /// PerformanceOnly rather than EvoAuthorize: an action added without its own attribute
    /// fails closed instead of falling back to "any logged-in user". The per-action
    /// [PerformanceOnly] attributes are redundant with this and kept as documentation.
    ///
    /// Technician self-service lives in MyPerformanceController — do not add self-scoped
    /// endpoints here, and do not loosen this attribute to share one with technicians.
    /// </summary>
    [ApiController]
    [Route("EvoApi/performance")]
    [PerformanceOnly]
    public class PerformanceController : BaseController
    {
        private readonly IPerformanceRepository _performanceRepository;

        public PerformanceController(IPerformanceRepository performanceRepository, IAuditService auditService)
        {
            _performanceRepository = performanceRepository;
            InitializeAuditService(auditService);
        }

        [HttpPost("upload/employees")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<PerformanceUploadResultDto>>> UploadEmployees([FromBody] UploadPerformanceEmployeesRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (request?.Rows == null || request.Rows.Count == 0)
                    return BadRequest(new ApiResponse<PerformanceUploadResultDto> { Success = false, Message = "No rows provided" });

                if (request.ReportDate == default)
                    return BadRequest(new ApiResponse<PerformanceUploadResultDto> { Success = false, Message = "Report date is required" });

                var userIds = await _performanceRepository.GetUserIdsAsync();
                var employeeNumberMap = await _performanceRepository.GetEmployeeNumberMapAsync();

                var matched = new List<(int UserId, PerformanceEmployeeRowDto Row)>();
                var unmatched = new List<string>();
                var seenUserIds = new HashSet<int>();

                foreach (var row in request.Rows)
                {
                    var label = $"{row.FirstName} {row.LastName}".Trim();
                    if (string.IsNullOrEmpty(label))
                        label = $"emp #{row.EmployeeNumber ?? "?"}";

                    int? userId = null;
                    if (row.UserId.HasValue && userIds.Contains(row.UserId.Value))
                    {
                        userId = row.UserId.Value;
                    }
                    else
                    {
                        var key = PerformanceRepository.NormalizeEmployeeNumber(row.EmployeeNumber);
                        if (key.Length > 0 && employeeNumberMap.TryGetValue(key, out var mappedId))
                            userId = mappedId;
                    }

                    if (userId == null)
                    {
                        unmatched.Add($"{label} (emp #{row.EmployeeNumber ?? "?"}) - no matching user");
                        continue;
                    }

                    if (!seenUserIds.Add(userId.Value))
                    {
                        unmatched.Add($"{label} - duplicate row for same user, first row kept");
                        continue;
                    }

                    matched.Add((userId.Value, row));
                }

                if (matched.Count == 0)
                    return BadRequest(new ApiResponse<PerformanceUploadResultDto> { Success = false, Message = "No rows matched an existing user" });

                var skippedTotal = request.SkippedRows + unmatched.Count;
                var uploadId = await _performanceRepository.CreateEmployeeUploadAsync(
                    request.ReportDate, request.Filename, UserId, skippedTotal, matched);

                stopwatch.Stop();
                await LogAuditAsync("UploadPerformanceEmployees",
                    new { uploadId, inserted = matched.Count, skipped = skippedTotal, request.ReportDate, request.Filename },
                    stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<PerformanceUploadResultDto>
                {
                    Success = true,
                    Message = "Employee performance data uploaded",
                    Data = new PerformanceUploadResultDto
                    {
                        UploadId = uploadId,
                        Inserted = matched.Count,
                        Skipped = skippedTotal,
                        UnmatchedRows = unmatched
                    }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UploadPerformanceEmployees", ex);
                return StatusCode(500, new ApiResponse<PerformanceUploadResultDto> { Success = false, Message = "Failed to upload employee performance data" });
            }
        }

        [HttpPost("upload/zones")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<PerformanceUploadResultDto>>> UploadZones([FromBody] UploadPerformanceZonesRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (request?.Rows == null || request.Rows.Count == 0)
                    return BadRequest(new ApiResponse<PerformanceUploadResultDto> { Success = false, Message = "No rows provided" });

                if (request.ReportDate == default)
                    return BadRequest(new ApiResponse<PerformanceUploadResultDto> { Success = false, Message = "Report date is required" });

                var zoneMap = await _performanceRepository.GetZoneAcronymMapAsync();

                var matched = new List<(int ZoneId, PerformanceZoneRowDto Row)>();
                var unmatched = new List<string>();
                var seenZoneIds = new HashSet<int>();

                foreach (var row in request.Rows)
                {
                    var key = row.ZoneAcronym?.Trim().ToUpperInvariant() ?? string.Empty;
                    if (key.Length == 0 || !zoneMap.TryGetValue(key, out var zoneId))
                    {
                        unmatched.Add($"{row.ZoneAcronym} - no matching zone");
                        continue;
                    }

                    if (!seenZoneIds.Add(zoneId))
                    {
                        unmatched.Add($"{row.ZoneAcronym} - duplicate row for same zone, first row kept");
                        continue;
                    }

                    matched.Add((zoneId, row));
                }

                if (matched.Count == 0)
                    return BadRequest(new ApiResponse<PerformanceUploadResultDto> { Success = false, Message = "No rows matched an existing zone" });

                var skippedTotal = request.SkippedRows + unmatched.Count;
                var uploadId = await _performanceRepository.CreateZoneUploadAsync(
                    request.ReportDate, request.Filename, UserId, skippedTotal, matched);

                stopwatch.Stop();
                await LogAuditAsync("UploadPerformanceZones",
                    new { uploadId, inserted = matched.Count, skipped = skippedTotal, request.ReportDate, request.Filename },
                    stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<PerformanceUploadResultDto>
                {
                    Success = true,
                    Message = "Zone performance data uploaded",
                    Data = new PerformanceUploadResultDto
                    {
                        UploadId = uploadId,
                        Inserted = matched.Count,
                        Skipped = skippedTotal,
                        UnmatchedRows = unmatched
                    }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UploadPerformanceZones", ex);
                return StatusCode(500, new ApiResponse<PerformanceUploadResultDto> { Success = false, Message = "Failed to upload zone performance data" });
            }
        }

        [HttpGet("uploads")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<List<PerformanceUploadDto>>>> GetUploads([FromQuery] string? type = null)
        {
            try
            {
                var uploads = await _performanceRepository.GetUploadsAsync(type);
                return Ok(new ApiResponse<List<PerformanceUploadDto>>
                {
                    Success = true,
                    Message = "Uploads retrieved",
                    Data = uploads,
                    Count = uploads.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetPerformanceUploads", ex);
                return StatusCode(500, new ApiResponse<List<PerformanceUploadDto>> { Success = false, Message = "Failed to retrieve uploads" });
            }
        }

        [HttpDelete("uploads/{id}")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<bool>>> DeleteUpload(int id)
        {
            try
            {
                var deleted = await _performanceRepository.DeleteUploadAsync(id);
                if (!deleted)
                    return NotFound(new ApiResponse<bool> { Success = false, Message = "Upload not found" });

                await LogAuditAsync("DeletePerformanceUpload", new { uploadId = id });
                return Ok(new ApiResponse<bool> { Success = true, Message = "Upload deleted", Data = true });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("DeletePerformanceUpload", ex);
                return StatusCode(500, new ApiResponse<bool> { Success = false, Message = "Failed to delete upload" });
            }
        }

        [HttpGet("employees/latest")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<List<PerformanceEmployeeDto>>>> GetLatestEmployeePerformance()
        {
            try
            {
                var data = await _performanceRepository.GetLatestEmployeePerformanceAsync();
                return Ok(new ApiResponse<List<PerformanceEmployeeDto>>
                {
                    Success = true,
                    Message = "Latest employee performance retrieved",
                    Data = data,
                    Count = data.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetLatestEmployeePerformance", ex);
                return StatusCode(500, new ApiResponse<List<PerformanceEmployeeDto>> { Success = false, Message = "Failed to retrieve employee performance" });
            }
        }

        [HttpGet("employees/{userId}/history")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<List<PerformanceEmployeeDto>>>> GetEmployeeHistory(int userId)
        {
            try
            {
                var data = await _performanceRepository.GetEmployeeHistoryAsync(userId);
                return Ok(new ApiResponse<List<PerformanceEmployeeDto>>
                {
                    Success = true,
                    Message = "Employee performance history retrieved",
                    Data = data,
                    Count = data.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetEmployeePerformanceHistory", ex);
                return StatusCode(500, new ApiResponse<List<PerformanceEmployeeDto>> { Success = false, Message = "Failed to retrieve employee performance history" });
            }
        }

        [HttpGet("employees/{userId}/jobmix")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<PerformanceJobMixDto>>> GetEmployeeJobMix(int userId)
        {
            try
            {
                var data = await _performanceRepository.GetEmployeeJobMixAsync(userId);
                return Ok(new ApiResponse<PerformanceJobMixDto>
                {
                    Success = true,
                    Message = "Employee job mix retrieved",
                    Data = data
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetEmployeeJobMix", ex);
                return StatusCode(500, new ApiResponse<PerformanceJobMixDto> { Success = false, Message = "Failed to retrieve employee job mix" });
            }
        }

        [HttpGet("zones/latest")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<List<PerformanceZoneDto>>>> GetLatestZonePerformance()
        {
            try
            {
                var data = await _performanceRepository.GetLatestZonePerformanceAsync();
                return Ok(new ApiResponse<List<PerformanceZoneDto>>
                {
                    Success = true,
                    Message = "Latest zone performance retrieved",
                    Data = data,
                    Count = data.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetLatestZonePerformance", ex);
                return StatusCode(500, new ApiResponse<List<PerformanceZoneDto>> { Success = false, Message = "Failed to retrieve zone performance" });
            }
        }

        [HttpGet("zones/{zoneId}/history")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<List<PerformanceZoneDto>>>> GetZoneHistory(int zoneId)
        {
            try
            {
                var data = await _performanceRepository.GetZoneHistoryAsync(zoneId);
                return Ok(new ApiResponse<List<PerformanceZoneDto>>
                {
                    Success = true,
                    Message = "Zone performance history retrieved",
                    Data = data,
                    Count = data.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetZonePerformanceHistory", ex);
                return StatusCode(500, new ApiResponse<List<PerformanceZoneDto>> { Success = false, Message = "Failed to retrieve zone performance history" });
            }
        }

        [HttpGet("targets")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<List<PerformanceTargetDto>>>> GetTargets()
        {
            try
            {
                var targets = await _performanceRepository.GetTargetsAsync();
                return Ok(new ApiResponse<List<PerformanceTargetDto>>
                {
                    Success = true,
                    Message = "Performance targets retrieved",
                    Data = targets,
                    Count = targets.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetPerformanceTargets", ex);
                return StatusCode(500, new ApiResponse<List<PerformanceTargetDto>> { Success = false, Message = "Failed to retrieve performance targets" });
            }
        }

        [HttpPut("targets")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<int>>> UpdateTargets([FromBody] List<PerformanceTargetDto> targets)
        {
            try
            {
                if (targets == null || targets.Count == 0)
                    return BadRequest(new ApiResponse<int> { Success = false, Message = "No targets provided" });

                var updated = await _performanceRepository.UpdateTargetsAsync(targets);
                await LogAuditAsync("UpdatePerformanceTargets", new { updated, targets });

                return Ok(new ApiResponse<int>
                {
                    Success = true,
                    Message = $"{updated} target(s) updated",
                    Data = updated
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("UpdatePerformanceTargets", ex);
                return StatusCode(500, new ApiResponse<int> { Success = false, Message = "Failed to update performance targets" });
            }
        }
    }

    public class UploadPerformanceEmployeesRequest
    {
        public DateTime ReportDate { get; set; }
        public string? Filename { get; set; }
        public int SkippedRows { get; set; }
        public List<PerformanceEmployeeRowDto> Rows { get; set; } = new();
    }

    public class UploadPerformanceZonesRequest
    {
        public DateTime ReportDate { get; set; }
        public string? Filename { get; set; }
        public int SkippedRows { get; set; }
        public List<PerformanceZoneRowDto> Rows { get; set; } = new();
    }
}
