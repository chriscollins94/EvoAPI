using EvoAPI.Shared.Models;

namespace EvoAPI.Core.Interfaces;

public interface IAuditService
{
    Task LogAsync(AuditEntry auditEntry);
    Task LogErrorAsync(AuditEntry auditEntry);
}
