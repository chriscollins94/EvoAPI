using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

// Loads the markup/tax configuration that applies to a given SR — the same
// inputs evo's invoice flow pulls from MaterialsMarkup / LaborRate /
// xrefCompanyCallCenter / ConfigSetting. Used by the labor-context endpoint
// and the Quote AI markup calculator.
public interface IMarkupConfigLoader
{
    Task<MarkupConfigDto?> LoadAsync(int srId, CancellationToken ct = default);
}
