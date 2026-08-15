namespace EvoAPI.Shared.DTOs;

public class NteServiceRequestRow
{
    public int SrId { get; set; }
    public string? SrNumber { get; set; }
    public decimal Nte { get; set; }
    public bool ExcludeQuotedSi { get; set; }

    public int? WoId { get; set; }
    public decimal? WoNte { get; set; }

    public int? TechUserId { get; set; }
    public string? TechFirstName { get; set; }
    public string? TechLastName { get; set; }
    public string? TechMobile { get; set; }
    public string? TechEmail { get; set; }

    public int? ZoneId { get; set; }
    public string? ZoneName { get; set; }
    public string? ZoneEmail { get; set; }

    public string TechFullName => $"{TechFirstName} {TechLastName}".Trim();
}
