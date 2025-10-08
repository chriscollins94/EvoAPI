namespace EvoAPI.Shared.DTOs;

public class AttachmentDto
{
    public int att_id { get; set; }
    public DateTime att_insertdatetime { get; set; }
    public string att_filename { get; set; } = string.Empty;
    public string att_description { get; set; } = string.Empty;
    public bool att_active { get; set; }
    public bool att_receipt { get; set; }
    public bool att_public { get; set; }
    public bool att_signoff { get; set; }
    public string att_submittedby { get; set; } = string.Empty;
    public decimal? att_receiptamount { get; set; }
    public int sr_id { get; set; }
}
