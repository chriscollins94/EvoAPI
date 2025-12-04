namespace EvoAPI.Shared.DTOs;

public class AddressDto
{
    // Database column names (with 'A' prefix for company addresses)
    public int AId { get; set; }
    public int OId { get; set; }
    public int AtId { get; set; }
    public string? AtTitle { get; set; }
    public DateTime AInsertDateTime { get; set; }
    public DateTime? AModifiedDateTime { get; set; }
    public string? ADescription { get; set; }
    public string? AAddress1 { get; set; }
    public string? AAddress2 { get; set; }
    public string? ACity { get; set; }
    public string? AState { get; set; }
    public string? AZip { get; set; }
    public string? ALatitude { get; set; }
    public string? ALongitude { get; set; }
    public string? APicture { get; set; }
    public bool AActive { get; set; }
    public string? ATempId { get; set; }
    
    // Legacy property names (for backward compatibility with employee management)
    public int Id { get => AId; set => AId = value; }
    public DateTime InsertDateTime { get => AInsertDateTime; set => AInsertDateTime = value; }
    public DateTime? ModifiedDateTime { get => AModifiedDateTime; set => AModifiedDateTime = value; }
    public string? Address1 { get => AAddress1; set => AAddress1 = value; }
    public string? Address2 { get => AAddress2; set => AAddress2 = value; }
    public string? City { get => ACity; set => ACity = value; }
    public string? State { get => AState; set => AState = value; }
    public string? Zip { get => AZip; set => AZip = value; }
    public string? Phone { get; set; }
    public string? Email { get; set; }
    public string? Notes { get; set; }
    public bool Active { get => AActive; set => AActive = value; }
}

public class AddressTitleDto
{
    public int AtId { get; set; }
    public int OId { get; set; }
    public DateTime AtInsertDateTime { get; set; }
    public DateTime? AtModifiedDateTime { get; set; }
    public string AtTitle { get; set; } = string.Empty;
    public bool AtActive { get; set; }
}

public class CreateAddressRequest
{
    public int AtId { get; set; }
    public string? ADescription { get; set; }
    public string AAddress1 { get; set; } = string.Empty;
    public string? AAddress2 { get; set; }
    public string ACity { get; set; } = string.Empty;
    public string AState { get; set; } = string.Empty;
    public string AZip { get; set; } = string.Empty;
    public string? ALatitude { get; set; }
    public string? ALongitude { get; set; }
}

public class UpdateAddressRequest
{
    public int AtId { get; set; }
    public string? ADescription { get; set; }
    public string AAddress1 { get; set; } = string.Empty;
    public string? AAddress2 { get; set; }
    public string ACity { get; set; } = string.Empty;
    public string AState { get; set; } = string.Empty;
    public string AZip { get; set; } = string.Empty;
    public string? ALatitude { get; set; }
    public string? ALongitude { get; set; }
}