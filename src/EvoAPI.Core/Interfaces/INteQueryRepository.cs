using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface INteQueryRepository
{
    Task<List<NteServiceRequestRow>> GetActiveServiceRequestsAsync(CancellationToken ct);
}
