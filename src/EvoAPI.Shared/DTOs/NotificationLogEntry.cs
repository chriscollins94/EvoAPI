namespace EvoAPI.Shared.DTOs;

public class NotificationLogEntry
{
    public string Type { get; set; } = string.Empty;
    public string? Key { get; set; }
    public string? EntityType { get; set; }
    public int? EntityId { get; set; }
    public string Channel { get; set; } = string.Empty;
    public string Recipient { get; set; } = string.Empty;
    public string? Subject { get; set; }
    public string? Body { get; set; }
    public string Status { get; set; } = "Sending";
    public string? Error { get; set; }
    public string? Metadata { get; set; }
}
