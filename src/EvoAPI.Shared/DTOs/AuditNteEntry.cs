namespace EvoAPI.Shared.DTOs;

// One dbo.AuditNTE row: the audit record and the dedup lock for a single NTE text or email.
// Column mapping is by name in AuditNteRepository.
public class AuditNteEntry
{
    public string Channel { get; set; } = string.Empty;      // Sms | Email
    public int Threshold { get; set; }
    public string Key { get; set; } = string.Empty;          // dedup key within (sr_id, channel)
    public string Status { get; set; } = "Sending";          // Sending | Sent | Failed | Skipped | Reviewed

    public int SrId { get; set; }
    public string? SrNumber { get; set; }
    public int? TechUserId { get; set; }
    public string? TechName { get; set; }
    public int? ZoneId { get; set; }
    public string? ZoneName { get; set; }

    public string? IntendedRecipient { get; set; }           // tech phone / zone email it was meant for
    public string? Recipient { get; set; }                   // where it actually went; null when skipped
    public bool Overridden { get; set; }                     // rerouted by a SmokeTestOverride* row

    public string? Subject { get; set; }
    public string? Body { get; set; }
    public string? Description { get; set; }                 // one readable line
    public string? Detail { get; set; }                      // spend snapshot JSON
    public string? ErrorMessage { get; set; }                // send error or skip reason
    public string? ServerName { get; set; }

    public decimal Nte { get; set; }
    public decimal Spent { get; set; }
    public decimal Percent { get; set; }
}
