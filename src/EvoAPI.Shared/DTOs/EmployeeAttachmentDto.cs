namespace EvoAPI.Shared.DTOs;

public class EmployeeAttachmentDto
{
    public int xua_id { get; set; }
    public DateTime xua_insertdatetime { get; set; }
    public DateTime? xua_modifieddatetime { get; set; }
    public int u_id { get; set; }
    public int att_id { get; set; }
    public int uat_id { get; set; }
    public string xua_description { get; set; } = string.Empty;
    public string xua_issuingauthority { get; set; } = string.Empty;
    public string xua_dateexpires { get; set; } = string.Empty;

    // Related attachment data (denormalized for convenience)
    public string att_filename { get; set; } = string.Empty;
    public DateTime att_insertdatetime { get; set; }

    // User attachment type name
    public string uat_type { get; set; } = string.Empty;
}

public class CreateEmployeeAttachmentRequest
{
    public string description { get; set; } = string.Empty;
    public int uat_id { get; set; }
    public string xua_issuingauthority { get; set; } = string.Empty;
    public string xua_dateexpires { get; set; } = string.Empty;
    // File is uploaded via multipart form-data
}

public class UpdateEmployeeAttachmentRequest
{
    public string description { get; set; } = string.Empty;
    public int uat_id { get; set; }
    public string xua_issuingauthority { get; set; } = string.Empty;
    public string xua_dateexpires { get; set; } = string.Empty;
    // Optional file for replacement - if provided, orphans old attachment
}
