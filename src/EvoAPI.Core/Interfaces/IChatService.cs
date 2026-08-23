using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface IChatService
{
    Task<CompanyChatParticipantsDto> GetCompanyChatParticipantsAsync();
    Task RemoveCompanyChatParticipantAsync(string userId);
}
