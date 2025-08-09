namespace EvoAPI.Shared.Models;

public class AuditEntry
{
    public int Id { get; set; }
    public string Username { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Detail { get; set; } = string.Empty;
    public string ResponseTime { get; set; } = string.Empty;
    public string IPAddress { get; set; } = string.Empty;
    public string UserAgent { get; set; } = string.Empty;
    public string MachineName { get; set; } = string.Empty;
    public bool IsError { get; set; }
    public DateTime CreatedDate { get; set; } = DateTime.Now;
}
