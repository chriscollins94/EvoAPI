namespace EvoAPI.Shared.DTOs;

public class UserAttachmentTypeDto
{
    public int uat_id { get; set; }
    public DateTime uat_insertdatetime { get; set; }
    public DateTime? uat_modifieddatetime { get; set; }
    public string uat_type { get; set; } = string.Empty;
}

public class CreateUserAttachmentTypeRequest
{
    public string uat_type { get; set; } = string.Empty;
}

public class UpdateUserAttachmentTypeRequest
{
    public int uat_id { get; set; }
    public string uat_type { get; set; } = string.Empty;
}
