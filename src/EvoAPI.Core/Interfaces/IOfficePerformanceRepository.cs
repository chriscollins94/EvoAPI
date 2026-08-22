using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface IOfficePerformanceRepository
{
    Task<List<OfficePerformanceAdminDto>> GetAdminsAsync();
    Task<OfficePerformanceSummaryDto> GetSummaryAsync(int userId, int? zoneId);
}
