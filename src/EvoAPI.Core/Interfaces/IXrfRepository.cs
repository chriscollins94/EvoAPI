using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface IXrfRepository
{
    /// Most recent waves with their completion counts (drives the wave dropdown).
    Task<List<XrfBatchDto>> GetBatchesAsync();

    /// Teams across all XRF locations with submitted / total counts, numeric order.
    Task<List<XrfTeamDto>> GetTeamsAsync();

    /// Incomplete locations plus those completed today, with the matched High Volume
    /// row and its checklist answers. Empty filter values mean "no filter".
    Task<List<XrfLocationDto>> GetActiveAsync(string team, string batch, string filter);

    /// Stamps the row submitted (result / who / when / where / comment). One-shot: a row that
    /// is already submitted is left untouched and reported as AlreadyCompleted. `result` must
    /// already be a canonical XrfResults value.
    Task<XrfCompleteOutcome> CompleteAsync(int xrfbdId, int userId, string result, string? comment,
        double? latitude, double? longitude, int? geoAccuracy);
}
