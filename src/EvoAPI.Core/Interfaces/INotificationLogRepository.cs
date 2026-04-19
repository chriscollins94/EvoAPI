using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface INotificationLogRepository
{
    Task<HashSet<(string Key, string Channel)>> GetFiredAsync(
        string type, string entityType, int entityId, CancellationToken ct);

    Task<int?> TryInsertSendingAsync(NotificationLogEntry entry, CancellationToken ct);

    Task UpdateStatusAsync(int nlId, string status, string? error, CancellationToken ct);
}
