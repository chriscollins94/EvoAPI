using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

// dbo.AuditNTE: the audit trail of every NTE text / email AND the dedup lock that stops a
// second send of the same (service request, channel, key). See NTE_NOTIFICATION_PLAN.md.
public interface IAuditNteRepository
{
    // Keys already handled for a service request: Sending, Sent and Skipped rows. Failed rows
    // are not included so the next scan retries them; Reviewed rows (go-live cutover) are not
    // included so the tech gets the real alert at their next check-in.
    Task<HashSet<(string Key, string Channel)>> GetFiredAsync(int srId, CancellationToken ct);

    // Inserts the row (status as given, normally 'Sending') before the send. If a row with the
    // same (sr_id, channel, key) already exists: a Failed row is reclaimed for a retry (status
    // reset, attempts + 1, recipient/body refreshed, earlier error kept) and its id returned;
    // any other status returns null, meaning another instance already handled it.
    Task<int?> TryInsertSendingAsync(AuditNteEntry entry, CancellationToken ct);

    // Final outcome of a send. 'Sent' stamps an_sentdatetime; an error is prepended to whatever
    // earlier attempts left in an_errormessage.
    Task UpdateStatusAsync(int anId, string status, string? error, CancellationToken ct);

    // Rows left in 'Sending' longer than olderThan belong to a scan that died mid-send
    // (process stopped, SMTP hang). Marks them Failed so they can be retried. Returns the count.
    Task<int> AbandonStaleSendingAsync(TimeSpan olderThan, CancellationToken ct);
}
