namespace EvoAPI.Core.Services;

/// <summary>
/// Everything the form resolver may know about one visit / unit when it decides which questions apply.
/// Season, equipment type and heating type can be typed in on the FORM RULES preview; the rest comes from a
/// stored asset (AssetFactsDto) when the preview is run for a real unit. Any missing fact leaves the
/// dependent questions as "shown when ..." instead of excluding them.
/// </summary>
public sealed class FormResolveFacts
{
    public string? Season { get; set; }
    public string? EquipmentType { get; set; }
    public string? HeatingType { get; set; }
    public string? Mount { get; set; }
    public int? AsId { get; set; }
    public string? AssetLabel { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, int> RepeatCounts { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}
