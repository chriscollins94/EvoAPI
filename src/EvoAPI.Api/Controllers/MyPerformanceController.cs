using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Models;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;

namespace EvoAPI.Api.Controllers
{
    /// <summary>
    /// Technician self-service performance. Open to any logged-in user, which is only safe
    /// because of one rule that must hold for every action in this class:
    ///
    ///   NO ENDPOINT HERE TAKES A USER IDENTIFIER.
    ///
    /// Identity comes from the JWT 'id' claim (BaseController.UserId) and nothing else, so
    /// there is no route value, query string or body field a caller could tamper with to read
    /// somebody else's numbers. If an endpoint ever needs to accept a user id, it belongs in
    /// PerformanceController behind [PerformanceOnly] instead.
    ///
    /// The one endpoint that returns other people's figures (peers) returns them anonymized by
    /// the repository — see PerformanceRepository.GetPeerComparisonAsync.
    ///
    /// Zone performance is deliberately absent: it is admin-only and stays in
    /// PerformanceController.
    /// </summary>
    [ApiController]
    [Route("EvoApi/myperformance")]
    [EvoAuthorize]
    public class MyPerformanceController : BaseController
    {
        private readonly IPerformanceRepository _performanceRepository;

        public MyPerformanceController(IPerformanceRepository performanceRepository, IAuditService auditService)
        {
            _performanceRepository = performanceRepository;
            InitializeAuditService(auditService);
        }

        /// <summary>The caller's own latest performance row, or null if they have never been uploaded.</summary>
        [HttpGet("latest")]
        public async Task<ActionResult<ApiResponse<PerformanceEmployeeDto?>>> GetMyLatest()
        {
            try
            {
                var data = await _performanceRepository.GetMyLatestPerformanceAsync(UserId);
                return Ok(new ApiResponse<PerformanceEmployeeDto?>
                {
                    Success = true,
                    Message = data == null ? "No performance data for this user" : "Performance retrieved",
                    Data = data
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetMyPerformanceLatest", ex);
                return StatusCode(500, new ApiResponse<PerformanceEmployeeDto?> { Success = false, Message = "Failed to retrieve your performance data" });
            }
        }

        /// <summary>The caller's own upload history, oldest first — drives the trend chart.</summary>
        [HttpGet("history")]
        public async Task<ActionResult<ApiResponse<List<PerformanceEmployeeDto>>>> GetMyHistory()
        {
            try
            {
                var data = await _performanceRepository.GetEmployeeHistoryAsync(UserId);
                return Ok(new ApiResponse<List<PerformanceEmployeeDto>>
                {
                    Success = true,
                    Message = "Performance history retrieved",
                    Data = data,
                    Count = data.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetMyPerformanceHistory", ex);
                return StatusCode(500, new ApiResponse<List<PerformanceEmployeeDto>> { Success = false, Message = "Failed to retrieve your performance history" });
            }
        }

        /// <summary>The caller's own job mix for the configured lookback window.</summary>
        [HttpGet("jobmix")]
        public async Task<ActionResult<ApiResponse<PerformanceJobMixDto>>> GetMyJobMix()
        {
            try
            {
                var data = await _performanceRepository.GetEmployeeJobMixAsync(UserId);
                return Ok(new ApiResponse<PerformanceJobMixDto>
                {
                    Success = true,
                    Message = "Job mix retrieved",
                    Data = data
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetMyPerformanceJobMix", ex);
                return StatusCode(500, new ApiResponse<PerformanceJobMixDto> { Success = false, Message = "Failed to retrieve your job mix" });
            }
        }

        /// <summary>
        /// Everyone's latest figures with identity stripped for all but the caller, so a
        /// technician can see where they rank without learning who anyone else is.
        /// </summary>
        [HttpGet("peers")]
        public async Task<ActionResult<ApiResponse<List<PerformancePeerDto>>>> GetPeers()
        {
            try
            {
                var data = await _performanceRepository.GetPeerComparisonAsync(UserId);
                return Ok(new ApiResponse<List<PerformancePeerDto>>
                {
                    Success = true,
                    Message = "Peer comparison retrieved",
                    Data = data,
                    Count = data.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetMyPerformancePeers", ex);
                return StatusCode(500, new ApiResponse<List<PerformancePeerDto>> { Success = false, Message = "Failed to retrieve peer comparison" });
            }
        }

        /// <summary>
        /// Company-wide metric targets. Read-only here; they are goals rather than personal
        /// data, and the dashboard cannot colour a single card without them. Editing targets
        /// stays on PerformanceController behind [PerformanceOnly].
        /// </summary>
        [HttpGet("targets")]
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
                await LogAuditErrorAsync("GetMyPerformanceTargets", ex);
                return StatusCode(500, new ApiResponse<List<PerformanceTargetDto>> { Success = false, Message = "Failed to retrieve performance targets" });
            }
        }
    }
}
