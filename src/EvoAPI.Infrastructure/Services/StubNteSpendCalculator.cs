using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;

namespace EvoAPI.Infrastructure.Services;

// Placeholder until real cost calculation lands. Returns a deterministic
// percent based on sr_id so a smoke test exercises 0 / 50 / 76 / 101 buckets.
public class StubNteSpendCalculator : INteSpendCalculator
{
    public Task<NteSpendResult> CalculateAsync(NteServiceRequestRow row, CancellationToken ct)
    {
        decimal percent = (row.SrId % 4) switch
        {
            0 => 0m,
            1 => 50m,
            2 => 76m,
            _ => 101m
        };

        var spent = Math.Round(row.Nte * percent / 100m, 2);
        return Task.FromResult(new NteSpendResult { Spent = spent, Percent = percent });
    }
}
