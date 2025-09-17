namespace EvoAPI.Shared.DTOs;

public class WorkOrderDto
{
    public int sr_id { get; set; }
    public int? wo_id { get; set; }
    public string CreateDate { get; set; } = string.Empty;
    public string CallCenter { get; set; } = string.Empty;
    public string Company { get; set; } = string.Empty;
    public string Trade { get; set; } = string.Empty;
    public string StartDate { get; set; } = string.Empty;
    public string EndDate { get; set; } = string.Empty;
    public string RequestNumber { get; set; } = string.Empty;
    public decimal? TotalDue { get; set; }
    public string Priority { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string SecondaryStatus { get; set; } = string.Empty;
    public string StatusColor { get; set; } = string.Empty;
    public string AssignedFirstName { get; set; } = string.Empty;
    public string AssignedLastName { get; set; } = string.Empty;
    public string Location { get; set; } = string.Empty;
    public string Address { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string State { get; set; } = string.Empty;
    public string Zip { get; set; } = string.Empty;
    public string Zone { get; set; } = string.Empty;
    public string CreatedBy { get; set; } = string.Empty;
    public DateTime? Escalated { get; set; }
    public bool ScheduleLock { get; set; }
    public string ActionableNote { get; set; } = string.Empty;
   
}

public class ActiveServiceRequestDto
{
    public int SrId { get; set; }
    public string RequestNumber { get; set; } = string.Empty;
    public DateTime InsertDateTime { get; set; }
    public string Status { get; set; } = string.Empty;
    public string TechFirstName { get; set; } = string.Empty;
    public string TechLastName { get; set; } = string.Empty;
    public bool IsActive { get; set; }
}

public class WorkOrderRequest
{
    public int NumberOfDays { get; set; } = 30;
}

public class UpdateWorkOrderEscalatedRequest
{
    public int ServiceRequestId { get; set; }
    public bool IsEscalated { get; set; }
}

public class UpdateWorkOrderScheduleLockRequest
{
    public int ServiceRequestId { get; set; }
    public bool IsScheduleLocked { get; set; }
}

public class ApiResponse<T>
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public T? Data { get; set; }
    public int Count { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}
