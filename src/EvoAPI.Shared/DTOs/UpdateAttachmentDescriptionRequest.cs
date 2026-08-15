namespace EvoAPI.Shared.DTOs;

public class UpdateAttachmentDescriptionRequest
{
    public int AttachmentId { get; set; }
    public string? Description { get; set; }
}
