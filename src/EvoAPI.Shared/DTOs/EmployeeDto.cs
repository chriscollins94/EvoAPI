namespace EvoAPI.Shared.DTOs;

public class EmployeeDto
{
    // Basic User Information
    public int Id { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? EmployeeNumber { get; set; }
    public string? Email { get; set; }
    public string? PhoneMobile { get; set; }
    public string? PhoneHome { get; set; }
    public string? PhoneDesk { get; set; }
    public string? Extension { get; set; }
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public bool Active { get; set; }
    public bool DirectoryOnly { get; set; }
    public decimal? DaysAvailablePTO { get; set; }
    public decimal? DaysAvailableVacation { get; set; }
    public string? Note { get; set; }
    public string? VehicleNumber { get; set; }
    public string? Picture { get; set; }
    public int? ZoneId { get; set; }
    public string? ZoneNumber { get; set; }
    public string? ZoneName { get; set; }
    public int? AddressId { get; set; }

    // Address Information (if available)
    public string? Address1 { get; set; }
    public string? Address2 { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? Zip { get; set; }

    // Clothing Size Information (IDs reference userclothing table)
    public int? ShirtSizeId { get; set; }
    public int? JacketSizeId { get; set; }
    
    // Pants Size Information (IDs reference UserPantsWaist and UserPantsLength tables)
    public int? PantsWaistId { get; set; }
    public int? PantsLengthId { get; set; }
    
    // Clothing Size Display Text (resolved from userclothing table)
    public string? ShirtSize { get; set; }
    public string? JacketSize { get; set; }
    
    // Pants Size Display Text (resolved from UserPantsWaist and UserPantsLength tables)
    public string? PantsWaistSize { get; set; }
    public string? PantsLengthSize { get; set; }

    // License Information
    public string? LicenseNumber { get; set; }
    public string? LicenseState { get; set; }
    public DateTime? LicenseExpiration { get; set; }

    // Role Information
    public List<UserRoleDto> Roles { get; set; } = new();
    
    // Trade General Information
    public List<UserTradeGeneralDto> TradeGenerals { get; set; } = new();

    // Facility Manager Flags
    public bool IsZoneFacilityManager { get; set; }
    public bool IsRegionFacilityManager { get; set; }

    // Computed Properties
    public string FullName => $"{FirstName} {LastName}".Trim();
    public string DisplayName => !string.IsNullOrEmpty(FullName) ? FullName : Username;
    public string FullAddress => BuildFullAddress();
    public string RoleNames => string.Join(", ", Roles.Select(r => r.RoleName));

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

public class EmployeeManagementDto
{
    public List<EmployeeDto> Employees { get; set; } = new();
    public List<ZoneDto> Zones { get; set; } = new();
    public List<RoleDto> Roles { get; set; } = new();
}

public class CreateEmployeeRequest
{
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? EmployeeNumber { get; set; }
    public string? Email { get; set; }
    public string? PhoneMobile { get; set; }
    public string? PhoneHome { get; set; }
    public string? PhoneDesk { get; set; }
    public string? Extension { get; set; }
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public bool Active { get; set; } = true;
    public bool DirectoryOnly { get; set; } = false;
    public decimal? DaysAvailablePTO { get; set; }
    public decimal? DaysAvailableVacation { get; set; }
    public string? Note { get; set; }
    public string? Picture { get; set; }
    public int? ZoneId { get; set; }
    
    // Address Information
    public string? Address1 { get; set; }
    public string? Address2 { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? Zip { get; set; }
    
    // Clothing Size Assignments (IDs reference userclothing table)
    public int? ShirtSizeId { get; set; }
    public int? JacketSizeId { get; set; }
    
    // Pants Size Assignments (IDs reference UserPantsWaist and UserPantsLength tables)
    public int? PantsWaistId { get; set; }
    public int? PantsLengthId { get; set; }
    
    // License Information
    public string? LicenseNumber { get; set; }
    public string? LicenseState { get; set; }
    public DateTime? LicenseExpiration { get; set; }
    
    // Role Assignments
    public List<int> RoleIds { get; set; } = new();
}

public class UpdateEmployeeRequest
{
    public int Id { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? EmployeeNumber { get; set; }
    public string? Email { get; set; }
    public string? PhoneMobile { get; set; }
    public string? PhoneHome { get; set; }
    public string? PhoneDesk { get; set; }
    public string? Extension { get; set; }
    public string Username { get; set; } = string.Empty;
    public string? Password { get; set; } // Optional for updates
    public bool Active { get; set; }
    public bool DirectoryOnly { get; set; }
    public decimal? DaysAvailablePTO { get; set; }
    public decimal? DaysAvailableVacation { get; set; }
    public string? Note { get; set; }
    public string? Picture { get; set; }
    public int? ZoneId { get; set; }
    
    // Address Information
    public int? AddressId { get; set; }
    public string? Address1 { get; set; }
    public string? Address2 { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? Zip { get; set; }
    
    // Clothing Size Assignments (IDs reference userclothing table)
    public int? ShirtSizeId { get; set; }
    public int? JacketSizeId { get; set; }
    
    // Pants Size Assignments (IDs reference UserPantsWaist and UserPantsLength tables)
    public int? PantsWaistId { get; set; }
    public int? PantsLengthId { get; set; }
    
    // License Information
    public string? LicenseNumber { get; set; }
    public string? LicenseState { get; set; }
    public DateTime? LicenseExpiration { get; set; }
    
    // Role Assignments
    public List<int> RoleIds { get; set; } = new();
}