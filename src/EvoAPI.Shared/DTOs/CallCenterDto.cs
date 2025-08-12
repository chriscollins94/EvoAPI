namespace EvoAPI.Shared.DTOs;

public class CallCenterDto
{
    public int Id { get; set; }
    public int OId { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool Active { get; set; }
    public string? TempId { get; set; }
    public string? Note { get; set; }
    public int Attack { get; set; }
}

public class UpdateCallCenterRequest
{
    public int Id { get; set; }
    public int OId { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool Active { get; set; }
    public string? TempId { get; set; }
    public string? Note { get; set; }
    public int Attack { get; set; }
}

public class CreateCallCenterRequest
{
    public int O_id { get; set; }
    public string Name { get; set; } = string.Empty;
    public bool Active { get; set; }
    public string? Note { get; set; }
    public int Attack { get; set; }
}

// Attack Point Note DTOs
public class AttackPointNoteDto
{
    public int Id { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public string Description { get; set; } = string.Empty;
    public int Hours { get; set; }
    public int Attack { get; set; }
}

public class UpdateAttackPointNoteRequest
{
    public int Id { get; set; }
    public string Description { get; set; } = string.Empty;
    public int Hours { get; set; }
    public int Attack { get; set; }
}

public class CreateAttackPointNoteRequest
{
    public string Description { get; set; } = string.Empty;
    public int Hours { get; set; }
    public int Attack { get; set; }
}

// Attack Point Status DTOs
public class AttackPointStatusDto
{
    public int Id { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public int DaysInStatus { get; set; }
    public int Attack { get; set; }
}

public class UpdateAttackPointStatusRequest
{
    public int Id { get; set; }
    public int DaysInStatus { get; set; }
    public int Attack { get; set; }
}

public class CreateAttackPointStatusRequest
{
    public int DaysInStatus { get; set; }
    public int Attack { get; set; }
}