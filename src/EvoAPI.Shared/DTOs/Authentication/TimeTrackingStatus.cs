namespace EvoAPI.Shared.DTOs.Authentication;

public class TimeTrackingStatus
{
    public DateTime? LoginTime { get; set; }
    public DateTime? ClockInTime { get; set; }
    public DateTime? CheckInTime { get; set; }
    public DateTime? BreakTime { get; set; }
    public int MinutesWorkedToday { get; set; }
    public decimal HoursWorkedToday => MinutesWorkedToday / 60.0m;
}
