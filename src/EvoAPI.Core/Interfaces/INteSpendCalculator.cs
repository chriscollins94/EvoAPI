using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface INteSpendCalculator
{
    // Open check-ins older than this many hours are a forgotten check-out and are ignored
    // (NTE_NOTIFICATION_PLAN.md D8). Overridden by ConfigSetting NteConfig/MaxOpenCheckInHours.
    const int DefaultMaxOpenCheckInHours = 24;

    // Spend for every open service request with an NTE, keyed by sr_id. One
    // set-based query per scan rather than one query per service request.
    Task<Dictionary<int, NteSpendResult>> CalculateActiveAsync(int maxOpenCheckInHours, CancellationToken ct);
}
