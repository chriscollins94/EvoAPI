namespace EvoAPI.Shared.DTOs;

public class ContactDto
{
    public int ConId { get; set; }
    public int? OId { get; set; }
    public int CtId { get; set; }
    public string? CtTitle { get; set; }
    public string? ConFirstname { get; set; }
    public string? ConLastname { get; set; }
    public string? ConEmail { get; set; }
    public string? ConPhone { get; set; }
    public string? ConMobile { get; set; }
    public string? ConFax { get; set; }
    public DateTime ConInsertDateTime { get; set; }
    public DateTime? ConModifiedDateTime { get; set; }
}

public class ContactTitleDto
{
    public int CtId { get; set; }
    public int? OId { get; set; }
    public string CtTitle { get; set; } = string.Empty;
    public DateTime CtInsertDateTime { get; set; }
    public DateTime? CtModifiedDateTime { get; set; }
    public bool CtActive { get; set; }
}

public class CreateContactRequest
{
    public int CtId { get; set; }
    public string ConFirstname { get; set; } = string.Empty;
    public string ConLastname { get; set; } = string.Empty;
    public string? ConEmail { get; set; }
    public string? ConPhone { get; set; }
    public string? ConMobile { get; set; }
    public string? ConFax { get; set; }
}

public class UpdateContactRequest
{
    public int CtId { get; set; }
    public string ConFirstname { get; set; } = string.Empty;
    public string ConLastname { get; set; } = string.Empty;
    public string? ConEmail { get; set; }
    public string? ConPhone { get; set; }
    public string? ConMobile { get; set; }
    public string? ConFax { get; set; }
}
