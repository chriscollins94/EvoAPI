namespace EvoAPI.Shared.DTOs;

public class BacklogItemDto
{
    public int BacklogItemId { get; set; }
    public string Code { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string? Source { get; set; }
    public string? Author { get; set; }
    public string? Date { get; set; }
    public string? Category { get; set; }
    public string Type { get; set; } = string.Empty;
    public string Priority { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string? Effort { get; set; }
    public string? Desc { get; set; }
    public string? Notes { get; set; }
    public string? DetailMarkdown { get; set; }
    public string? History { get; set; }
    public bool Active { get; set; }
    public string? InsertDateTime { get; set; }
    public string? LastUpdated { get; set; }
    public int? LastUpdatedByUserId { get; set; }
}

public class CreateBacklogItemRequest
{
    public string Code { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string? Source { get; set; }
    public string? Author { get; set; }
    public string? Date { get; set; }
    public string? Category { get; set; }
    public string Type { get; set; } = "Feature";
    public string Priority { get; set; } = "Medium";
    public string Status { get; set; } = "New";
    public string? Effort { get; set; }
    public string? Desc { get; set; }
    public string? Notes { get; set; }
    public string? DetailMarkdown { get; set; }
    public bool Active { get; set; } = true;
}

public class UpdateBacklogItemRequest : CreateBacklogItemRequest
{
    public int BacklogItemId { get; set; }
}
