using System.ComponentModel.DataAnnotations;

namespace EvoAPI.Shared.DTOs
{
    public class MissingReceiptDto
    {
        public int RmId { get; set; }
        public DateTime? RmDateUpload { get; set; }
        public DateTime? RmDateReceipt { get; set; }
        public string? RmDescription { get; set; }
        public decimal? RmAmount { get; set; }
        public string? UEmployeeNumber { get; set; }
    }

    public class MissingReceiptDashboardDto
    {
        public int UId { get; set; }
        public int RmId { get; set; }
        public DateTime? RmDateUpload { get; set; }
        public DateTime? RmDateReceipt { get; set; }
        public string? RmDescription { get; set; }
        public decimal? RmAmount { get; set; }
        public string? UFirstName { get; set; }
        public string? ULastName { get; set; }
        public string? UEmployeeNumber { get; set; }
    }

    public class MissingReceiptUploadDto
    {
        [Required]
        public DateTime RmDateReceipt { get; set; }
        
        [Required]
        [MaxLength(50)]
        public string RmDescription { get; set; } = string.Empty;
        
        [Required]
        public decimal RmAmount { get; set; }
        
        [Required]
        [MaxLength(50)]
        public string UEmployeeNumber { get; set; } = string.Empty;
    }
}
