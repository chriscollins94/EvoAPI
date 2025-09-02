using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Models;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class ScheduleController : BaseController
{
    public ScheduleController(IAuditService auditService)
    {
        InitializeAuditService(auditService);
    }

    [HttpPost("GetActiveTechs")]
    public async Task<IActionResult> GetActiveTechs([FromBody] SimpleRequest request)
    {
        return await ExecuteWithAuditAsync(
            "GetActiveTechs", 
            request,
            async () =>
            {
                // Simulate business logic
                await Task.Delay(100); // Simulate database call
                
                var result = new 
                {
                    Techs = new[]
                    {
                        new { Id = 1, Name = "John Doe", Status = "Available" },
                        new { Id = 2, Name = "Jane Smith", Status = "On Call" },
                        new { Id = 3, Name = "Mike Johnson", Status = "Available" }
                    },
                    Count = 3
                };

                return Ok(result);
            }
        );
    }

    [HttpPost("GetScheduleDetails")]
    public async Task<IActionResult> GetScheduleDetails([FromBody] ScheduleRequest scheduleRequest)
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            #region Initialize & Audit Entry
            await LogAuditAsync("GetScheduleDetails", scheduleRequest);
            #endregion
            
            #region Verify required data
            if (scheduleRequest.StartDate == default || scheduleRequest.EndDate == default)
            {
                await LogAuditErrorAsync("GetScheduleDetails", 
                    new ArgumentException("Required info not provided - Start/End Date"), scheduleRequest);
                return BadRequest("Start Date and End Date are required");
            }
            #endregion
            
            #region Business Logic
            // Simulate data retrieval
            await Task.Delay(200);
            
            var scheduleData = new
            {
                StartDate = scheduleRequest.StartDate,
                EndDate = scheduleRequest.EndDate,
                TechId = scheduleRequest.TechId,
                Appointments = new[]
                {
                    new { 
                        Id = 1, 
                        TechName = "John Doe", 
                        CustomerName = "ABC Company",
                        AppointmentTime = DateTime.Now.AddHours(2),
                        ServiceType = "Maintenance"
                    },
                    new { 
                        Id = 2, 
                        TechName = "Jane Smith", 
                        CustomerName = "XYZ Corp",
                        AppointmentTime = DateTime.Now.AddHours(4),
                        ServiceType = "Repair"
                    }
                }
            };
            
            stopwatch.Stop();
            await LogAuditAsync("GetScheduleDetails - Data Retrieved", null, 
                stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
            
            return Ok(scheduleData);
            #endregion
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetScheduleDetails", ex, scheduleRequest);
            return StatusCode(500, "Internal server error");
        }
    }

    [HttpPost("UpdateSchedule")]
    public async Task<IActionResult> UpdateSchedule([FromBody] UpdateScheduleRequest request)
    {
        try
        {
            #region Admin Check
            if (!IsAdmin)
            {
                await LogAuditErrorAsync("UpdateSchedule", 
                    new UnauthorizedAccessException("Admin access required"), request);
                return Forbid("Admin access required");
            }
            #endregion
            
            await LogAuditAsync("UpdateSchedule", request);
            
            // Simulate update logic
            await Task.Delay(150);
            
            await LogAuditAsync("UpdateSchedule - Completed");
            
            return Ok(new { Success = true, Message = "Schedule updated successfully" });
        }
        catch (Exception ex)
        {
            await LogAuditErrorAsync("UpdateSchedule", ex, request);
            throw;
        }
    }
}

#region Request Models
public class SimpleRequest
{
    public int Id { get; set; }
}

public class ScheduleRequest
{
    public DateTime StartDate { get; set; }
    public DateTime EndDate { get; set; }
    public int? TechId { get; set; }
}

public class UpdateScheduleRequest
{
    public int ScheduleId { get; set; }
    public int TechId { get; set; }
    public DateTime NewDateTime { get; set; }
    public string? Notes { get; set; }
}
#endregion
