namespace EvoAPI.Shared.DTOs;

public class ConfigSettingDto
{
    public int CsId { get; set; }
    public string? CsType { get; set; }
    public string CsIdentifier { get; set; } = string.Empty;
    public string? CsValue { get; set; }
    public DateTime CsInsertDateTime { get; set; }
    public DateTime? CsModifiedDateTime { get; set; }
    public string? CsDescription { get; set; }
}
