namespace EvoAPI.Shared.DTOs;

// ============================================================
// Time Off Request DTOs
// ============================================================

/// <summary>
/// Request type (parent category - e.g., "Time Off", "Office")
/// </summary>
public class TimeOffRequestTypeDto
{
    public int TortId { get; set; }
    public string TortType { get; set; } = string.Empty;
}

/// <summary>
/// Request type detail (sub-type - e.g., "Vacation", "PTO", "Home")
/// </summary>
public class TimeOffRequestTypeDetailDto
{
    public int TortdId { get; set; }
    public int TortId { get; set; }
    public string TortdTypedetail { get; set; } = string.Empty;
    public bool TortdWorkflowrequired { get; set; }
    public bool TortdAdmincancreate { get; set; }
    public bool TortdTechcancreate { get; set; }
    public int TortdMaxdaysoff { get; set; }
}

/// <summary>
/// Time off balance for a user
/// </summary>
public class TimeOffBalanceDto
{
    public int UserId { get; set; }
    public string Username { get; set; } = string.Empty;
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public int DaysAvailablePto { get; set; }
    public int DaysAvailableVacation { get; set; }
}

/// <summary>
/// Time off request (main record)
/// </summary>
public class TimeOffRequestDto
{
    public int TorId { get; set; }
    public DateTime TorInsertdatetime { get; set; }
    public DateTime TorStartdate { get; set; }
    public DateTime TorEnddate { get; set; }
    public string TortdTypedetail { get; set; } = string.Empty;
    public int TortdId { get; set; }
    public string TorsStatus { get; set; } = string.Empty;
    public int TorsId { get; set; }
    public int UserId { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public int DaysAvailableVacation { get; set; }
    public int DaysAvailablePto { get; set; }
    public int TorTotalhours { get; set; }
    public string TorNote { get; set; } = string.Empty;
    public string TorNotereason { get; set; } = string.Empty;
    public int ZoneId { get; set; }
}

/// <summary>
/// Time off request detail (per-day record)
/// </summary>
public class TimeOffRequestDetailDto
{
    public int TordId { get; set; }
    public int TorId { get; set; }
    public DateTime TordDate { get; set; }
    public int TordStarthour { get; set; }
    public int TordEndhour { get; set; }
}

/// <summary>
/// Request to create a new time off request
/// </summary>
public class CreateTimeOffRequestDto
{
    public int TortdId { get; set; }
    public int UserId { get; set; }
    public int UserIdAdmincreated { get; set; }
    public DateTime StartDate { get; set; }
    public DateTime EndDate { get; set; }
    public string Note { get; set; } = string.Empty;
    public List<CreateTimeOffRequestDetailDto> Details { get; set; } = new();
}

/// <summary>
/// Per-day detail for creating a time off request
/// </summary>
public class CreateTimeOffRequestDetailDto
{
    public string Date { get; set; } = string.Empty;
    public int StartHour { get; set; }
    public int EndHour { get; set; }
}

/// <summary>
/// Active employee for admin creator dropdown
/// </summary>
public class TimeOffActiveEmployeeDto
{
    public int UserId { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
}

/// <summary>
/// Request to update a time off request (approve/reject)
/// </summary>
public class UpdateTimeOffRequestDto
{
    public int TorId { get; set; }
    public int TorsId { get; set; } // New status: 1=Approved, 4=Rejected
    public string TorNotereason { get; set; } = string.Empty;
}
