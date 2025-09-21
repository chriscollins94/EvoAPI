namespace EvoAPI.Shared.DTOs;

public class AddressDto
{
    public int Id { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public string? Address1 { get; set; }
    public string? Address2 { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? Zip { get; set; }
    public string? Phone { get; set; }
    public string? Email { get; set; }
    public string? Notes { get; set; }
    public bool Active { get; set; }

    public string FullAddress => BuildFullAddress();

    private string BuildFullAddress()
    {
        var addressParts = new List<string>();

        if (!string.IsNullOrWhiteSpace(Address1))
            addressParts.Add(Address1);

        if (!string.IsNullOrWhiteSpace(Address2))
            addressParts.Add(Address2);

        var cityStateZip = new List<string>();
        if (!string.IsNullOrWhiteSpace(City))
            cityStateZip.Add(City);
        if (!string.IsNullOrWhiteSpace(State))
            cityStateZip.Add(State);
        if (!string.IsNullOrWhiteSpace(Zip))
            cityStateZip.Add(Zip);

        if (cityStateZip.Count > 0)
            addressParts.Add(string.Join(", ", cityStateZip));

        return string.Join(", ", addressParts);
    }
}

public class CreateAddressRequest
{
    public string? Address1 { get; set; }
    public string? Address2 { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? Zip { get; set; }
    public bool Active { get; set; } = true;
}

public class UpdateAddressRequest
{
    public int Id { get; set; }
    public string? Address1 { get; set; }
    public string? Address2 { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? Zip { get; set; }
    public bool Active { get; set; }
}