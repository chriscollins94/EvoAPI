namespace EvoAPI.Shared.DTOs;

// One row per (service request, recipient). A service request with two techs
// checked in at the same time yields two rows; one with nobody checked in and
// nobody assigned to the primary work order yields a single row with null tech
// fields so the orchestrator can log a Skipped entry. See NTE_NOTIFICATION_PLAN.md D2.
public class NteServiceRequestRow
{
    public int SrId { get; set; }
    public string? SrNumber { get; set; }
    public decimal Nte { get; set; }
    public string? CustomerName { get; set; }
    public string? LocationName { get; set; }

    public int? PrimaryWoId { get; set; }

    public int? TechUserId { get; set; }
    public string? TechFirstName { get; set; }
    public string? TechLastName { get; set; }
    public string? TechMobile { get; set; }
    public string? TechMobileSource { get; set; }   // "EvoCell" | "PersonalCell" | null
    public bool TechIsCheckedIn { get; set; }

    public int? ZoneId { get; set; }
    public string? ZoneName { get; set; }
    public string? ZoneEmail { get; set; }

    public string TechFullName => $"{TechFirstName} {TechLastName}".Trim();
}
