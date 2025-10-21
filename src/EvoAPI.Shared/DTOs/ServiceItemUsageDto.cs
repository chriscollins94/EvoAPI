namespace EvoAPI.Shared.DTOs
{
    public class ServiceItemUsageDto
    {
        public int ServiceRequestId { get; set; }
        public string RequestNumber { get; set; } = string.Empty;
        public string Summary { get; set; } = string.Empty;
        public DateTime InsertDateTime { get; set; }
        public DateTime? DateDue { get; set; }
        public string CompanyName { get; set; } = string.Empty;
        public string LocationName { get; set; } = string.Empty;
        public decimal TotalQuantity { get; set; }
        public decimal AverageCost { get; set; }
        public decimal TotalCost { get; set; }
    }
}
