using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface INteSpendCalculator
{
    Task<NteSpendResult> CalculateAsync(NteServiceRequestRow row, CancellationToken ct);
}
