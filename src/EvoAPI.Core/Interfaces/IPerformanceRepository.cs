using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface IPerformanceRepository
{
    Task<Dictionary<string, int>> GetEmployeeNumberMapAsync();
    Task<Dictionary<string, int>> GetZoneAcronymMapAsync();
    Task<HashSet<int>> GetUserIdsAsync();

    Task<int> CreateEmployeeUploadAsync(DateTime reportDate, string? filename, int uploaderId, int skippedCount,
        List<(int UserId, PerformanceEmployeeRowDto Row)> rows);
    Task<int> CreateZoneUploadAsync(DateTime reportDate, string? filename, int uploaderId, int skippedCount,
        List<(int ZoneId, PerformanceZoneRowDto Row)> rows);

    Task<List<PerformanceUploadDto>> GetUploadsAsync(string? type);
    Task<bool> DeleteUploadAsync(int uploadId);

    Task<List<PerformanceEmployeeDto>> GetLatestEmployeePerformanceAsync();
    Task<List<PerformanceEmployeeDto>> GetEmployeeHistoryAsync(int userId);

    // Technician-facing (MyPerformanceController): self only, plus an anonymized peer set.
    Task<PerformanceEmployeeDto?> GetMyLatestPerformanceAsync(int userId);
    Task<List<PerformancePeerDto>> GetPeerComparisonAsync(int userId);

    Task<List<PerformanceZoneDto>> GetLatestZonePerformanceAsync();
    Task<List<PerformanceZoneDto>> GetZoneHistoryAsync(int zoneId);

    Task<List<PerformanceTargetDto>> GetTargetsAsync();
    Task<int> UpdateTargetsAsync(List<PerformanceTargetDto> targets);

    Task<PerformanceJobMixDto> GetEmployeeJobMixAsync(int userId);
}
