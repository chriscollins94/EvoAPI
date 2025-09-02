namespace EvoAPI.Shared.DTOs;

public class ServiceRequestNumberChangesDto
{
    public int SrId { get; set; }
    public DateTime CreatedDate { get; set; }
    public string CallCenter { get; set; } = string.Empty;
    public string Company { get; set; } = string.Empty;
    public string ServiceRequest { get; set; } = string.Empty;
    public string PrimaryWorkOrder { get; set; } = string.Empty;
}
