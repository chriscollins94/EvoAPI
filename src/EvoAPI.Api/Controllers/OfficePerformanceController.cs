using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Models;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;

namespace EvoAPI.Api.Controllers
{
    /// <summary>
    /// Office/admin performance dashboard — live metrics computed from status change
    /// history and customer inquiries, scoped per admin by their zone + status
    /// assignments (xrefAdminZoneStatusSecondary).
    ///
    /// Class-level policy is PerformanceOnly (same function claim as the technician
    /// performance dashboard) so an action added without its own attribute fails closed.
    /// </summary>
    [ApiController]
    [Route("EvoApi/officeperformance")]
    [PerformanceOnly]
    public class OfficePerformanceController : BaseController
    {
        private readonly IOfficePerformanceRepository _repository;

        public OfficePerformanceController(IOfficePerformanceRepository repository, IAuditService auditService)
        {
            _repository = repository;
            InitializeAuditService(auditService);
        }

        [HttpGet("admins")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<List<OfficePerformanceAdminDto>>>> GetAdmins()
        {
            try
            {
                var admins = await _repository.GetAdminsAsync();
                return Ok(new ApiResponse<List<OfficePerformanceAdminDto>>
                {
                    Success = true,
                    Message = "Office performance admins retrieved",
                    Data = admins,
                    Count = admins.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetOfficePerformanceAdmins", ex);
                return StatusCode(500, new ApiResponse<List<OfficePerformanceAdminDto>> { Success = false, Message = "Failed to retrieve office performance admins" });
            }
        }

        [HttpGet("{userId}/summary")]
        [PerformanceOnly]
        public async Task<ActionResult<ApiResponse<OfficePerformanceSummaryDto>>> GetSummary(int userId, [FromQuery] int? zoneId = null)
        {
            try
            {
                var summary = await _repository.GetSummaryAsync(userId, zoneId);
                return Ok(new ApiResponse<OfficePerformanceSummaryDto>
                {
                    Success = true,
                    Message = "Office performance summary retrieved",
                    Data = summary
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetOfficePerformanceSummary", ex);
                return StatusCode(500, new ApiResponse<OfficePerformanceSummaryDto> { Success = false, Message = "Failed to retrieve office performance summary" });
            }
        }
    }
}
