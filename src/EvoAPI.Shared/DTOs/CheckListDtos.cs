namespace EvoAPI.Shared.DTOs;

public class CheckListQuestionDto
{
    public int ClqId { get; set; }
    public int ClId { get; set; }
    public int ClatId { get; set; }
    public string ClqQuestion { get; set; } = string.Empty;
    public int? ClqOrder { get; set; }
    public bool ClqRequired { get; set; }
    public string? ClqAnswerValues { get; set; }
    public string? ClatType { get; set; }
    public string? ClqSkipAnswer { get; set; }
    public int? ClqSkipToOrder { get; set; }
}

public class CheckListTypeDto
{
    public int CltId { get; set; }
    public string CltType { get; set; } = string.Empty;
}

public class CheckListAnswerTypeDto
{
    public int ClatId { get; set; }
    public string ClatType { get; set; } = string.Empty;
}

public class CreateCheckListRequest
{
    public int CltId { get; set; }
    public string ClName { get; set; } = string.Empty;
    public bool ClPublicForQuote { get; set; }
    public bool ClPublicForInvoice { get; set; }
    public bool ClActive { get; set; } = true;
}

public class UpdateCheckListRequest
{
    public int CltId { get; set; }
    public string ClName { get; set; } = string.Empty;
    public bool ClPublicForQuote { get; set; }
    public bool ClPublicForInvoice { get; set; }
    public bool ClActive { get; set; }
}

public class CreateCheckListQuestionRequest
{
    public int ClatId { get; set; }
    public string ClqQuestion { get; set; } = string.Empty;
    public int? ClqOrder { get; set; }
    public bool ClqRequired { get; set; }
    public string? ClqAnswerValues { get; set; }
    public string? ClqSkipAnswer { get; set; }
    public int? ClqSkipToOrder { get; set; }
}

public class UpdateCheckListQuestionRequest
{
    public int ClatId { get; set; }
    public string ClqQuestion { get; set; } = string.Empty;
    public int? ClqOrder { get; set; }
    public bool ClqRequired { get; set; }
    public string? ClqAnswerValues { get; set; }
    public string? ClqSkipAnswer { get; set; }
    public int? ClqSkipToOrder { get; set; }
}

public class CloneCheckListRequest
{
    public int TargetXcccId { get; set; }
    public List<int>? ChecklistIds { get; set; }
}
