namespace EvoAPI.Shared.DTOs;

public class ReceiptsReportDto
{
    public string CallCenter { get; set; } = string.Empty;
    public string Company { get; set; } = string.Empty;
    public string ReceiptType { get; set; } = string.Empty;
    public string Supplier { get; set; } = string.Empty;
    public string SupplierEntered { get; set; } = string.Empty;
    public string RequestNumber { get; set; } = string.Empty;
    public string TechFirstName { get; set; } = string.Empty;
    public string TechLastName { get; set; } = string.Empty;
    public string SubmittedBy { get; set; } = string.Empty;
    public decimal ReceiptAmount { get; set; }
    public DateTime InsertDateTime { get; set; }
    public string FileName { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string Comment { get; set; } = string.Empty;
    public string Path { get; set; } = string.Empty;
    public int ServiceRequestId { get; set; }
    public int WorkOrderId { get; set; }
    public int AttachmentId { get; set; }
    public string Extension { get; set; } = string.Empty;
    public string TechFullName => $"{TechFirstName} {TechLastName}".Trim();
}
