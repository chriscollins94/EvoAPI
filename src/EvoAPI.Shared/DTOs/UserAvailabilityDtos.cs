namespace EvoAPI.Shared.DTOs;

/// <summary>
/// One row of the UserAvailabilityStatus lookup (On call / Available / Not Available).
/// The legacy Angular pages hardcoded this list in three separate places; the table is the
/// source of truth now.
/// </summary>
public class AvailabilityStatusDto
{
    public int StatusId { get; set; }
    public string Status { get; set; } = string.Empty;
}

/// <summary>
/// A technician in the availability tech picker. Zone comes along so the picker and the
/// coverage view can filter by it - TechnicianDto has no zone.
/// </summary>
public class AvailabilityTechnicianDto
{
    public int UserId { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string? EmployeeNumber { get; set; }
    public string? ZoneAcronym { get; set; }
    public bool Active { get; set; }

    /// <summary>Count of set slots, so the picker can show who has a week filled in.</summary>
    public int SlotCount { get; set; }
}

/// <summary>
/// One (day, hour) cell. DayOfWeek is 1=Monday..7=Sunday - the legacy app's own numbering,
/// not SQL Server's DATEPART default. Hours are 0-23 in Central time.
/// </summary>
public class AvailabilitySlotDto
{
    public int DayOfWeek { get; set; }
    public int HourOfDay { get; set; }
    public int StatusId { get; set; }
}

/// <summary>
/// One technician's whole week. Slots are sparse - only cells that have been set are
/// returned; everything else is unset.
/// </summary>
public class TechnicianAvailabilityDto
{
    public int UserId { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string? ZoneAcronym { get; set; }
    public List<AvailabilitySlotDto> Slots { get; set; } = new();
}

/// <summary>
/// A slot in a batch write. StatusId null means clear the cell.
/// </summary>
public class AvailabilitySlotWrite
{
    public int DayOfWeek { get; set; }
    public int HourOfDay { get; set; }
    public int? StatusId { get; set; }
}

public class SaveAvailabilityRequest
{
    public int UserId { get; set; }
    public List<AvailabilitySlotWrite> Slots { get; set; } = new();
}

public class ClearAvailabilityRequest
{
    public int UserId { get; set; }

    /// <summary>Empty or null clears the technician's entire week.</summary>
    public List<AvailabilitySlotWrite>? Slots { get; set; }
}

public class CopyAvailabilityRequest
{
    public int FromUserId { get; set; }
    public int ToUserId { get; set; }

    /// <summary>
    /// When true the target's existing week is wiped first, so the copy is an exact mirror.
    /// When false only cells the target has not set are filled in.
    /// </summary>
    public bool Overwrite { get; set; }
}

/// <summary>
/// One technician's status in a single coverage cell.
/// </summary>
public class CoverageTechnicianDto
{
    public int UserId { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string? ZoneAcronym { get; set; }
    public int StatusId { get; set; }
}

/// <summary>
/// Every set slot across all technicians, flattened. The page pivots this into the
/// day x hour headcount grid - one request instead of one per technician.
/// </summary>
public class AvailabilityCoverageDto
{
    public List<CoverageCellDto> Cells { get; set; } = new();
    public int TechnicianCount { get; set; }
}

public class CoverageCellDto
{
    public int DayOfWeek { get; set; }
    public int HourOfDay { get; set; }
    public List<CoverageTechnicianDto> Technicians { get; set; } = new();
}
