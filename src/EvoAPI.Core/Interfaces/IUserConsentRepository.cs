using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface IUserConsentRepository
{
    Task<bool> HasOptedInAsync(int userId, string consentType, string disclosureVersion, CancellationToken ct);

    Task<int> InsertAsync(UserConsentRecord record, CancellationToken ct);
}
