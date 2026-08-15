namespace EvoAPI.Shared.DTOs;

public class ServiceRequestReportDto
{
    public string CallCenter { get; set; } = string.Empty;
    public string CallCenterPortalName { get; set; } = string.Empty;
    public string CallCenterPortalUrl { get; set; } = string.Empty;
    public string CallCenterPortalCredentials { get; set; } = string.Empty;

    public string Company { get; set; } = string.Empty;
    public string CompanyPortalName { get; set; } = string.Empty;
    public string CompanyPortalUrl { get; set; } = string.Empty;
    public string CompanyPortalCredentials { get; set; } = string.Empty;

    public string ParentTrade { get; set; } = string.Empty;
    public string Trade { get; set; } = string.Empty;

    public string ServiceRequestNumber { get; set; } = string.Empty;
    public DateTime? Created { get; set; }

    public string PrimaryTech { get; set; } = string.Empty;
    public string AdditionalTechs { get; set; } = string.Empty;

    public DateTime? PrimaryWoStart { get; set; }
    public DateTime? PrimaryWoEnd { get; set; }

    public string InvoiceNumber { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public decimal? TotalDue { get; set; }
    public string SummaryOfWorkCompleted { get; set; } = string.Empty;

    public string Location { get; set; } = string.Empty;
    public string Address1 { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string State { get; set; } = string.Empty;
    public string Zip { get; set; } = string.Empty;

    public string NoteCreatedBy { get; set; } = string.Empty;
    public DateTime? NoteCreated { get; set; }
    public string MostRecentNote { get; set; } = string.Empty;

    public int ServiceItemCount { get; set; }
    public string ServiceItems { get; set; } = string.Empty;
}
