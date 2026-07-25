namespace EvoAPI.Shared.DTOs;

/// <summary>
/// One row of the SkillLevel lookup (the 0-5 scale plus the KY-cleared tiers).
/// </summary>
public class SkillLevelDto
{
    public int SkillLevelId { get; set; }
    public int Score { get; set; }
    public string Description { get; set; } = string.Empty;

    /// <summary>True for the "inc KY" tiers - technicians cleared to work in Kentucky.</summary>
    public bool IsKentucky { get; set; }

    /// <summary>
    /// Legacy display format: score, a "KY" suffix for Kentucky tiers, then the description
    /// (e.g. "4KY - Advanced (Repairs) inc KY"). Computed here so every consumer renders
    /// it identically.
    /// </summary>
    public string DisplayLabel { get; set; } = string.Empty;
}

/// <summary>
/// A child trade column in the skill matrix.
/// </summary>
public class SkillTradeDto
{
    public int TradeId { get; set; }
    public string Trade { get; set; } = string.Empty;
}

/// <summary>
/// One technician's assignment for a single trade.
/// </summary>
public class SkillAssignmentDto
{
    public int TradeId { get; set; }
    public int? SkillLevelId { get; set; }
    public string? Note { get; set; }
}

/// <summary>
/// A technician row in the skill matrix, with their assignments for the selected parent trade.
/// </summary>
public class SkillMatrixTechnicianDto
{
    public int UserId { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string? EmployeeNumber { get; set; }
    public string? ZoneAcronym { get; set; }
    public bool Active { get; set; }
    public List<SkillAssignmentDto> Assignments { get; set; } = new();
}

/// <summary>
/// Everything the "By Trade" grid needs for one parent trade, in a single response.
/// </summary>
public class SkillMatrixDto
{
    public int ParentTradeId { get; set; }
    public string ParentTrade { get; set; } = string.Empty;
    public List<SkillTradeDto> ChildTrades { get; set; } = new();
    public List<SkillMatrixTechnicianDto> Technicians { get; set; } = new();
}

/// <summary>
/// A parent trade and its children for the "By Technician" view.
/// </summary>
public class TechnicianSkillGroupDto
{
    public int ParentTradeId { get; set; }
    public string ParentTrade { get; set; } = string.Empty;
    public List<TechnicianSkillRowDto> Children { get; set; } = new();
}

public class TechnicianSkillRowDto
{
    public int TradeId { get; set; }
    public string Trade { get; set; } = string.Empty;
    public int? SkillLevelId { get; set; }
    public string? Note { get; set; }
}

/// <summary>
/// Every trade grouped by parent, with one technician's level and note on each.
/// </summary>
public class TechnicianSkillsDto
{
    public int UserId { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string? ZoneAcronym { get; set; }
    public List<TechnicianSkillGroupDto> Groups { get; set; } = new();
}

/// <summary>
/// Upsert for a single (technician, trade) cell. SetLevel/SetNote say which columns the
/// caller is actually changing, so saving a note can't wipe a level and vice versa.
/// </summary>
public class UpsertSkillAssignmentRequest
{
    public int UserId { get; set; }
    public int TradeId { get; set; }
    public int? SkillLevelId { get; set; }
    public string? Note { get; set; }
    public bool SetLevel { get; set; }
    public bool SetNote { get; set; }
}

/// <summary>
/// Copy one technician's assignments onto another, optionally limited to the child trades
/// of a single parent trade.
/// </summary>
public class CopySkillsRequest
{
    public int FromUserId { get; set; }
    public int ToUserId { get; set; }
    public int? ParentTradeId { get; set; }

    /// <summary>When false, trades the target already has a level on are left alone.</summary>
    public bool OverwriteExisting { get; set; }
}

/// <summary>
/// Set every child trade of a parent to one level for a single technician.
/// </summary>
public class BulkSetSkillLevelRequest
{
    public int UserId { get; set; }
    public int ParentTradeId { get; set; }
    public int SkillLevelId { get; set; }

    /// <summary>When false, trades the technician already has a level on are left alone.</summary>
    public bool OverwriteExisting { get; set; }
}
