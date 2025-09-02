namespace EvoAPI.Shared.DTOs;

public class UserDto
{
    public int Id { get; set; }
    public int OId { get; set; }
    public int? AId { get; set; }
    public int? VId { get; set; }
    public int? SupervisorId { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? EmployeeNumber { get; set; }
    public string? Email { get; set; }
    public string? PhoneHome { get; set; }
    public string? PhoneMobile { get; set; }
    public bool Active { get; set; }
    public string? Picture { get; set; }
    public string? SSN { get; set; }
    public DateTime? DateOfHire { get; set; }
    public DateTime? DateEligiblePTO { get; set; }
    public DateTime? DateEligibleVacation { get; set; }
    public decimal? DaysAvailablePTO { get; set; }
    public decimal? DaysAvailableVacation { get; set; }
    public string? ClothingShirt { get; set; }
    public string? ClothingJacket { get; set; }
    public string? ClothingPants { get; set; }
    public string? WirelessProvider { get; set; }
    public string? PreferredNotification { get; set; }
    public string? QuickBooksName { get; set; }
    public DateTime? PasswordChanged { get; set; }
    public bool U_2FA { get; set; }
    public int? ZoneId { get; set; }
    public DateTime? CovidVaccineDate { get; set; }
    public string? Note { get; set; }
    public string? NoteDashboard { get; set; }

    // Computed properties for display
    public string FullName => $"{FirstName} {LastName}".Trim();
    public string DisplayName => !string.IsNullOrEmpty(FullName) ? FullName : Username;
}

public class CreateUserRequest
{
    public int OId { get; set; }
    public int? AId { get; set; }
    public int? VId { get; set; }
    public int? SupervisorId { get; set; }
    public string Username { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? EmployeeNumber { get; set; }
    public string? Email { get; set; }
    public string? PhoneHome { get; set; }
    public string? PhoneMobile { get; set; }
    public bool Active { get; set; } = true;
    public string? Picture { get; set; }
    public string? SSN { get; set; }
    public DateTime? DateOfHire { get; set; }
    public DateTime? DateEligiblePTO { get; set; }
    public DateTime? DateEligibleVacation { get; set; }
    public decimal? DaysAvailablePTO { get; set; }
    public decimal? DaysAvailableVacation { get; set; }
    public string? ClothingShirt { get; set; }
    public string? ClothingJacket { get; set; }
    public string? ClothingPants { get; set; }
    public string? WirelessProvider { get; set; }
    public string? PreferredNotification { get; set; }
    public string? QuickBooksName { get; set; }
    public bool U_2FA { get; set; }
    public int? ZoneId { get; set; }
    public DateTime? CovidVaccineDate { get; set; }
    public string? Note { get; set; }
    public string? NoteDashboard { get; set; }
}

public class UpdateUserRequest
{
    public int Id { get; set; }
    public int OId { get; set; }
    public int? AId { get; set; }
    public int? VId { get; set; }
    public int? SupervisorId { get; set; }
    public string Username { get; set; } = string.Empty;
    public string? Password { get; set; } // Optional for updates
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? EmployeeNumber { get; set; }
    public string? Email { get; set; }
    public string? PhoneHome { get; set; }
    public string? PhoneMobile { get; set; }
    public bool Active { get; set; }
    public string? Picture { get; set; }
    public string? SSN { get; set; }
    public DateTime? DateOfHire { get; set; }
    public DateTime? DateEligiblePTO { get; set; }
    public DateTime? DateEligibleVacation { get; set; }
    public decimal? DaysAvailablePTO { get; set; }
    public decimal? DaysAvailableVacation { get; set; }
    public string? ClothingShirt { get; set; }
    public string? ClothingJacket { get; set; }
    public string? ClothingPants { get; set; }
    public string? WirelessProvider { get; set; }
    public string? PreferredNotification { get; set; }
    public string? QuickBooksName { get; set; }
    public bool U_2FA { get; set; }
    public int? ZoneId { get; set; }
    public DateTime? CovidVaccineDate { get; set; }
    public string? Note { get; set; }
    public string? NoteDashboard { get; set; }
}
