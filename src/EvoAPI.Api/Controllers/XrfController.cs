using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers
{
    /// <summary>
    /// XRF revisits: a second trip to High Volume locations so the tech can shoot the
    /// service line with the XRF analyzer. Locations are keyed by premise number back to
    /// HighVolumeBatchDetail; the original visit is shown read-only and the tech records a
    /// required result (Complete / Error / Inaccessible / Dirty/Wet), an optional comment and
    /// presses "Submit XRF" (who / when / device position are stamped server-side).
    ///
    /// Open to any logged-in user, like the High Volume endpoints it sits beside. The
    /// evotech page additionally gates itself with the existing EvoWS isTechHighVolume
    /// check. Completion always stamps the caller's own id from the JWT; no user id is
    /// accepted from the client.
    ///
    /// Independent of service requests, work orders and billing for now.
    /// </summary>
    [ApiController]
    [Route("EvoApi/xrf")]
    [EvoAuthorize]
    public class XrfController : BaseController
    {
        private const int MaxFilterLength = 100;

        private readonly IXrfRepository _xrfRepository;

        public XrfController(IXrfRepository xrfRepository, IAuditService auditService)
        {
            _xrfRepository = xrfRepository;
            InitializeAuditService(auditService);
        }

        /// <summary>Most recent XRF waves with completion counts.</summary>
        [HttpGet("batches")]
        public async Task<ActionResult<ApiResponse<List<XrfBatchDto>>>> GetBatches()
        {
            try
            {
                var data = await _xrfRepository.GetBatchesAsync();
                return Ok(new ApiResponse<List<XrfBatchDto>>
                {
                    Success = true,
                    Message = "XRF waves retrieved",
                    Data = data,
                    Count = data.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetXrfBatches", ex);
                return StatusCode(500, new ApiResponse<List<XrfBatchDto>> { Success = false, Message = "Failed to retrieve XRF waves" });
            }
        }

        /// <summary>Teams across XRF locations with submitted / total counts.</summary>
        [HttpGet("teams")]
        public async Task<ActionResult<ApiResponse<List<XrfTeamDto>>>> GetTeams()
        {
            try
            {
                var data = await _xrfRepository.GetTeamsAsync();
                return Ok(new ApiResponse<List<XrfTeamDto>>
                {
                    Success = true,
                    Message = "XRF teams retrieved",
                    Data = data,
                    Count = data.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetXrfTeams", ex);
                return StatusCode(500, new ApiResponse<List<XrfTeamDto>> { Success = false, Message = "Failed to retrieve XRF teams" });
            }
        }

        /// <summary>
        /// Incomplete locations plus those completed today, with the matched High Volume
        /// visit. All filters are optional: team (exact), batch (wave name, exact),
        /// filter (premise / HV meter / address / stanpar contains).
        /// </summary>
        [HttpGet("active")]
        public async Task<ActionResult<ApiResponse<List<XrfLocationDto>>>> GetActive(
            [FromQuery] string? team, [FromQuery] string? batch, [FromQuery] string? filter)
        {
            try
            {
                var data = await _xrfRepository.GetActiveAsync(
                    Clean(team, 10), Clean(batch, 50), Clean(filter, MaxFilterLength));

                return Ok(new ApiResponse<List<XrfLocationDto>>
                {
                    Success = true,
                    Message = "XRF locations retrieved",
                    Data = data,
                    Count = data.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetXrfActive", ex, new { team, batch, filter });
                return StatusCode(500, new ApiResponse<List<XrfLocationDto>> { Success = false, Message = "Failed to retrieve XRF locations" });
            }
        }

        /// <summary>
        /// Submits a location's XRF result for the calling tech. The result is required and
        /// must be one of XrfResults.All. One-shot: a second submit returns 409 and leaves
        /// the original stamp untouched.
        /// </summary>
        [HttpPost("complete")]
        public async Task<ActionResult<ApiResponse<XrfCompleteResult>>> Complete([FromBody] XrfCompleteRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (request == null || request.XrfbdId <= 0)
                {
                    return BadRequest(new ApiResponse<XrfCompleteResult> { Success = false, Message = "Required info not provided - xrfbdId" });
                }

                var result = XrfResults.Normalize(request.Result);
                if (result == null)
                {
                    return BadRequest(new ApiResponse<XrfCompleteResult>
                    {
                        Success = false,
                        Message = "Select a result: " + string.Join(", ", XrfResults.All)
                    });
                }

                var outcome = await _xrfRepository.CompleteAsync(
                    request.XrfbdId, UserId, result, request.Comment, request.Latitude, request.Longitude, request.GeoAccuracy);

                switch (outcome.Status)
                {
                    case XrfCompleteStatus.NotFound:
                        return NotFound(new ApiResponse<XrfCompleteResult> { Success = false, Message = "XRF location not found" });
                    case XrfCompleteStatus.AlreadyCompleted:
                        return Conflict(new ApiResponse<XrfCompleteResult> { Success = false, Message = "This location has already been submitted" });
                }

                stopwatch.Stop();
                await LogAuditAsync("CompleteXrf",
                    new
                    {
                        xrfbdId = request.XrfbdId,
                        userId = UserId,
                        result,
                        locationCaptured = outcome.Result!.LocationCaptured,
                        geoAccuracy = request.GeoAccuracy,
                        hasComment = !string.IsNullOrWhiteSpace(request.Comment)
                    },
                    stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<XrfCompleteResult>
                {
                    Success = true,
                    Message = "XRF result recorded",
                    Data = outcome.Result,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CompleteXrf", ex, new { xrfbdId = request?.XrfbdId });
                return StatusCode(500, new ApiResponse<XrfCompleteResult> { Success = false, Message = "Failed to record the XRF result" });
            }
        }

        private static string Clean(string? value, int maxLength)
        {
            var trimmed = value?.Trim() ?? string.Empty;
            return trimmed.Length > maxLength ? trimmed.Substring(0, maxLength) : trimmed;
        }
    }
}
