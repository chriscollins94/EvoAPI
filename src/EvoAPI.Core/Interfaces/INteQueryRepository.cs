using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface INteQueryRepository
{
    // One row per (service request, checked-in tech). Only SRs with an open check-in newer than
    // maxOpenCheckInHours are returned (NTE_NOTIFICATION_PLAN.md D8 / D9).
    Task<List<NteServiceRequestRow>> GetActiveServiceRequestsAsync(int maxOpenCheckInHours, CancellationToken ct);
}
