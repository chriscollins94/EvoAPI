using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace EvoAPI.Core.Services;

public class DataService : IDataService
{
    private readonly ILogger<DataService> _logger;
    private readonly IConfiguration _configuration;
    private readonly IAuditService _auditService;
    private readonly IFleetmaticsService _fleetmaticsService;
    private readonly IGoogleMapsService _googleMapsService;

    public DataService(ILogger<DataService> logger, IConfiguration configuration, IAuditService auditService, IFleetmaticsService fleetmaticsService, IGoogleMapsService googleMapsService)
    {
        _logger = logger;
        _configuration = configuration;
        _auditService = auditService;
        _fleetmaticsService = fleetmaticsService;
        _googleMapsService = googleMapsService;
    }

    public async Task<DataTable> GetWorkOrdersAsync(int numberOfDays)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            if (numberOfDays > 1500) numberOfDays = 180;

            const string sql = @" 

                WITH RankedOrders AS (
                    SELECT 
                        sr.sr_id              AS sr_id,
                        wo.wo_id              AS wo_id,
                        FORMAT(sr.sr_insertdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'yyyy-MM-dd HH:mm') CreateDate,
                        cc.cc_name            AS CallCenter,
                        c.c_name              AS Company,
                        t.t_trade             AS Trade,
                        FORMAT(wo.wo_startdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'yyyy-MM-dd HH:mm') StartDate,
                        FORMAT(wo.wo_enddatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'yyyy-MM-dd HH:mm') EndDate,
                        sr.sr_requestnumber   AS RequestNumber,
                        sr.sr_totaldue        AS TotalDue,
                        s.s_status            AS Status,
                        ss.ss_statussecondary AS SecondaryStatus,
                        ss.ss_color           AS StatusColor,
                        p.p_priority          AS Priority,
                        u.u_firstname         AS AssignedFirstName,
                        u.u_lastname          AS AssignedLastName,
                        l.l_location          AS Location,
                        a.a_address1          AS Address,
                        a.a_city              AS City,
                        a.a_state             AS State,
                        a.a_zip               AS Zip,
                        z.z_number            AS Zone,
                        u_createdby.u_firstname + ' ' + u_createdby.u_lastname AS CreatedBy,
                        sr.sr_escalated       AS Escalated,
                        ISNULL(sr.sr_schedulelock, 0) AS ScheduleLock,
                        ISNULL(sr.sr_actionablenote, '') AS ActionableNote,
                        ROW_NUMBER() OVER (
                            PARTITION BY cc.cc_name
                            ORDER BY wo.wo_startdatetime DESC
                        ) AS rn
                    FROM servicerequest sr
                    INNER JOIN xrefCompanyCallCenter xccc ON sr.xccc_id = xccc.xccc_id
                    INNER JOIN company c ON xccc.c_id = c.c_id
                    INNER JOIN callcenter cc ON xccc.cc_id = cc.cc_id
                    INNER JOIN [status] s ON sr.s_id = s.s_id
                    INNER JOIN location l ON sr.l_id = l.l_id
                    INNER JOIN priority p ON sr.p_id = p.p_id
                    INNER JOIN address a ON l.a_id = a.a_id
                    INNER JOIN trade t ON sr.t_id = t.t_id
                    LEFT JOIN workorder wo ON sr.wo_id_primary = wo.wo_id
                    LEFT JOIN StatusSecondary ss ON wo.ss_id = ss.ss_id
                    LEFT JOIN xrefworkorderuser xwou ON wo.wo_id = xwou.wo_id
                    LEFT JOIN [user] u ON xwou.u_id = u.u_id
                    LEFT JOIN xrefuserrole xur ON xur.u_id = u.u_id
                    LEFT JOIN role r ON r.r_id = xur.r_id
                    LEFT JOIN Zone z ON u.z_id = z.z_id
                    LEFT JOIN [user] u_createdby ON sr.u_id_createdby = u_createdby.u_id
                    WHERE 
                        (wo.wo_startdatetime BETWEEN DATEADD(DAY, -@numberOfDays, GETDATE()) AND DATEADD(DAY, 180, GETDATE()) or (wo.wo_startdatetime is null AND not s.s_status in ('Paid', 'Invoiced')))
                        AND c.c_name NOT IN ('Metro Pipe Program')
                        AND (r.r_role = 'Technician' or r.r_role is null)
               )
                SELECT
                    sr_id,
                    wo_id,
                    CreateDate,
                    CallCenter,
                    Company,
                    Trade,
                    StartDate,
                    EndDate,
                    RequestNumber,
                    TotalDue,
                    Status,
                    SecondaryStatus,
                    StatusColor,
                    Priority,
                    AssignedFirstName,
                    AssignedLastName,
                    Location,
                    Address,
                    City,
                    State,
                    Zip,
                    Zone,
                    CreatedBy,
                    Escalated,
                    ScheduleLock,
                    ActionableNote
                FROM RankedOrders
                ORDER BY sr_id desc;";

            var parameters = new Dictionary<string, object> { { "@numberOfDays", numberOfDays } };

            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetWorkOrders",
                Detail = $"Retrieved work orders for {numberOfDays} days",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetWorkOrders",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving work orders for {Days} days", numberOfDays);
            throw;
        }
    }

    public async Task<DataTable> GetWorkOrdersScheduleAsync(int numberOfDays, int? technicianId = null)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            if (numberOfDays > 1500) numberOfDays = 180;

            var technicianFilter = technicianId.HasValue ? "AND u.u_id = @technicianId" : "";

            const string sqlTemplate = @" 
                WITH RankedOrders AS (
                    SELECT
                        sr.sr_id              AS sr_id,
                        wo.wo_id              AS wo_id,
                        FORMAT(sr.sr_insertdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'yyyy-MM-dd HH:mm') CreateDate,
                        cc.cc_name            AS CallCenter,
                        c.c_name              AS Company,
                        t.t_trade             AS Trade,
                        FORMAT(wo.wo_startdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'yyyy-MM-dd HH:mm') StartDate,
                        FORMAT(wo.wo_enddatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'yyyy-MM-dd HH:mm') EndDate,
                        sr.sr_requestnumber   AS RequestNumber,
                        sr.sr_totaldue        AS TotalDue,
                        s.s_status            AS Status,
                        ss.ss_statussecondary AS SecondaryStatus,
                        ss.ss_color           AS StatusColor,
                        p.p_priority          AS Priority,
                        u.u_firstname         AS AssignedFirstName,
                        u.u_lastname          AS AssignedLastName,
                        l.l_location          AS Location,
                        a.a_address1          AS Address,
                        a.a_city              AS City,
                        a.a_state             AS State,
                        a.a_zip               AS Zip,
                        z.z_number            AS Zone,
                            u_createdby.u_firstname + ' ' + u_createdby.u_lastname AS CreatedBy,
                        sr.sr_escalated       AS Escalated,
                        ISNULL(sr.sr_schedulelock, 0) AS ScheduleLock,
                        ISNULL(sr.sr_actionablenote, '') AS ActionableNote,
                        ROW_NUMBER() OVER (
                            PARTITION BY cc.cc_name
                            ORDER BY wo.wo_startdatetime DESC
                        ) AS rn
                    FROM servicerequest sr
                    INNER JOIN xrefCompanyCallCenter xccc ON sr.xccc_id = xccc.xccc_id
                    INNER JOIN company c ON xccc.c_id = c.c_id
                    INNER JOIN callcenter cc ON xccc.cc_id = cc.cc_id
                    INNER JOIN [status] s ON sr.s_id = s.s_id
                    INNER JOIN location l ON sr.l_id = l.l_id
                    INNER JOIN priority p ON sr.p_id = p.p_id
                    INNER JOIN address a ON l.a_id = a.a_id
                    INNER JOIN trade t ON sr.t_id = t.t_id
                    LEFT JOIN workorder wo ON sr.sr_id = wo.sr_id
                    LEFT JOIN StatusSecondary ss ON wo.ss_id = ss.ss_id
                    LEFT JOIN xrefworkorderuser xwou ON wo.wo_id = xwou.wo_id
                    LEFT JOIN [user] u ON xwou.u_id = u.u_id
                    LEFT JOIN xrefuserrole xur ON xur.u_id = u.u_id
                    LEFT JOIN role r ON r.r_id = xur.r_id
                    LEFT JOIN Zone z ON u.z_id = z.z_id
                    LEFT JOIN [user] u_createdby ON sr.u_id_createdby = u_createdby.u_id
                    WHERE 
                        (wo.wo_startdatetime BETWEEN DATEADD(DAY, -@numberOfDays, GETDATE()) AND DATEADD(DAY, 180, GETDATE()) or (wo.wo_startdatetime is null AND not s.s_status in ('Rejected', 'Paid', 'Invoiced')))
                        AND c.c_name NOT IN ('Metro Pipe Program')
                        AND (r.r_role = 'Technician' or r.r_role is null)
                        {technicianFilter}
                )
                SELECT
                    sr_id,
                    wo_id,
                    CreateDate,
                    CallCenter,
                    Company,
                    Trade,
                    StartDate,
                    EndDate,
                    RequestNumber,
                    TotalDue,
                    Status,
                    Priority,
                    SecondaryStatus,
                    StatusColor,
                    AssignedFirstName,
                    AssignedLastName,
                    Location,
                    Address,
                    City,
                    State,
                    Zip,
                    Zone,
                    CreatedBy,
                    Escalated,
                    ScheduleLock,
                    ActionableNote
                FROM RankedOrders
                ORDER BY CallCenter, Company, Trade, requestnumber;";

            var sql = sqlTemplate.Replace("{technicianFilter}", technicianFilter);
            var parameters = new Dictionary<string, object> { { "@numberOfDays", numberOfDays } };
            
            if (technicianId.HasValue)
            {
                parameters.Add("@technicianId", technicianId.Value);
            }

            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetWorkOrdersSchedule",
                Detail = $"Retrieved work orders schedule for {numberOfDays} days",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetWorkOrdersSchedule",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving work orders schedule for {Days} days", numberOfDays);
            throw;
        }
    }

    public async Task<bool> UpdateWorkOrderEscalatedAsync(UpdateWorkOrderEscalatedRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var sql = request.IsEscalated 
                ? "UPDATE servicerequest SET sr_escalated = GETDATE() WHERE sr_id = @serviceRequestId"
                : "UPDATE servicerequest SET sr_escalated = NULL WHERE sr_id = @serviceRequestId";

            var parameters = new Dictionary<string, object>
            {
                { "@serviceRequestId", request.ServiceRequestId }
            };

            var result = await ExecuteNonQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateWorkOrderEscalated",
                Detail = $"Updated escalated status for service request {request.ServiceRequestId} to {request.IsEscalated}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateWorkOrderEscalated",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating escalated status for service request {Id}", request.ServiceRequestId);
            throw;
        }
    }

    public async Task<bool> UpdateWorkOrderScheduleLockAsync(UpdateWorkOrderScheduleLockRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var sql = "UPDATE servicerequest SET sr_schedulelock = @isScheduleLocked WHERE sr_id = @serviceRequestId";

            var parameters = new Dictionary<string, object>
            {
                { "@serviceRequestId", request.ServiceRequestId },
                { "@isScheduleLocked", request.IsScheduleLocked ? 1 : 0 }
            };

            var result = await ExecuteNonQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateWorkOrderScheduleLock",
                Detail = $"Updated schedule lock status for service request {request.ServiceRequestId} to {request.IsScheduleLocked}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateWorkOrderScheduleLock",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating schedule lock status for service request {Id}", request.ServiceRequestId);
            throw;
        }
    }

    public async Task<DataTable> GetAllPrioritiesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    p_id as Id,
                    p_insertdatetime as InsertDateTime,
                    p_modifieddatetime as ModifiedDateTime,
                    p_priority as PriorityName,
                    p_order as [Order],
                    p_color as Color,
                    p_arrivaltimeinhours as ArrivalTimeInHours,
                    p_attack as Attack
                FROM Priority
                ORDER BY p_order, p_priority";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllPriorities",
                Detail = $"Retrieved {result.Rows.Count} priorities",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllPriorities",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving priorities");
            throw;
        }
    }

    public async Task<bool> UpdatePriorityAsync(EvoAPI.Shared.DTOs.UpdatePriorityRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE Priority 
                SET 
                    p_priority = @PriorityName,
                    p_order = @Order,
                    p_color = @Color,
                    p_arrivaltimeinhours = @ArrivalTimeInHours,
                    p_attack = @Attack,
                    p_modifieddatetime = GETDATE()
                WHERE p_id = @Id";

            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@PriorityName", request.PriorityName },
                { "@Order", request.Order ?? (object)DBNull.Value },
                { "@Color", request.Color ?? (object)DBNull.Value },
                { "@ArrivalTimeInHours", request.ArrivalTimeInHours ?? (object)DBNull.Value },
                { "@Attack", request.Attack }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdatePriority",
                Detail = $"Updated priority {request.Id} - {request.PriorityName}. Rows affected: {rowsAffected}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return rowsAffected > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdatePriority",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating priority {Id}", request.Id);
            throw;
        }
    }

    public async Task<DataTable> GetAllStatusSecondariesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    ss_id as Id,
                    ss_insertdatetime as InsertDateTime,
                    ss_modifieddatetime as ModifiedDateTime,
                    s_id as StatusId,
                    ss_statussecondary as StatusSecondary,
                    ss_color as Color,
                    ss_code as Code,
                    ss_attack as Attack
                FROM dbo.StatusSecondary
                ORDER BY ss_statussecondary";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllStatusSecondaries",
                Detail = $"Retrieved {result.Rows.Count} status secondaries",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllStatusSecondaries",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving status secondaries");
            throw;
        }
    }

    public async Task<bool> UpdateStatusSecondaryAsync(EvoAPI.Shared.DTOs.UpdateStatusSecondaryRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.StatusSecondary 
                SET 
                    s_id = @StatusId,
                    ss_statussecondary = @StatusSecondary,
                    ss_color = @Color,
                    ss_code = @Code,
                    ss_attack = @Attack,
                    ss_modifieddatetime = GETDATE()
                WHERE ss_id = @Id";

            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@StatusId", request.StatusId },
                { "@StatusSecondary", request.StatusSecondary },
                { "@Color", request.Color ?? (object)DBNull.Value },
                { "@Code", request.Code ?? (object)DBNull.Value },
                { "@Attack", request.Attack }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateStatusSecondary",
                Detail = $"Updated status secondary {request.Id} - {request.StatusSecondary}. Rows affected: {rowsAffected}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return rowsAffected > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateStatusSecondary",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating status secondary {Id}", request.Id);
            throw;
        }
    }

    public async Task<DataTable> GetAllCallCentersAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    cc_id as Id,
                    o_id as OId,
                    cc_insertdatetime as InsertDateTime,
                    cc_modifieddatetime as ModifiedDateTime,
                    cc_name as Name,
                    cc_active as Active,
                    cc_tempid as TempId,
                    cc_note as Note,
                    cc_attack as Attack
                FROM dbo.CallCenter
                ORDER BY cc_name";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllCallCenters",
                Detail = $"Retrieved {result.Rows.Count} call centers",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllCallCenters",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving call centers");
            throw;
        }
    }

    public async Task<bool> UpdateCallCenterAsync(EvoAPI.Shared.DTOs.UpdateCallCenterRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.CallCenter 
                SET 
                    o_id = @OId,
                    cc_name = @Name,
                    cc_active = @Active,
                    cc_tempid = @TempId,
                    cc_note = @Note,
                    cc_attack = @Attack,
                    cc_modifieddatetime = GETDATE()
                WHERE cc_id = @Id";

            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@OId", request.OId },
                { "@Name", request.Name },
                { "@Active", request.Active },
                { "@TempId", request.TempId ?? (object)DBNull.Value },
                { "@Note", request.Note ?? (object)DBNull.Value },
                { "@Attack", request.Attack }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCallCenter",
                Detail = $"Updated call center {request.Id} - {request.Name}. Rows affected: {rowsAffected}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return rowsAffected > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCallCenter",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating call center {Id}", request.Id);
            throw;
        }
    }

    public async Task<int?> CreateCallCenterAsync(EvoAPI.Shared.DTOs.CreateCallCenterRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.CallCenter 
                (o_id, cc_name, cc_active, cc_tempid, cc_note, cc_attack, cc_insertdatetime, cc_modifieddatetime)
                VALUES 
                (@OId, @Name, @Active, @TempId, @Note, @Attack, GETDATE(), GETDATE());
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@OId", request.O_id },
                { "@Name", request.Name },
                { "@Active", request.Active },
                { "@TempId", (object)DBNull.Value },
                { "@Note", request.Note ?? (object)DBNull.Value },
                { "@Attack", request.Attack }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var newId = await command.ExecuteScalarAsync();
            
            if (newId != null && int.TryParse(newId.ToString(), out var id))
            {
                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "CreateCallCenter",
                    Detail = $"Created new call center '{request.Name}' with ID {id}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return id;
            }
            
            return null;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateCallCenter",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating call center {Name}", request.Name);
            throw;
        }
    }

    // Attack Point Notes methods
    public async Task<DataTable> GetAllAttackPointNotesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    apn_id as Id,
                    apn_insertdatetime as InsertDateTime,
                    apn_modifieddatetime as ModifiedDateTime,
                    apn_description as Description,
                    apn_hours as Hours,
                    apn_attack as Attack
                FROM dbo.AttackPointNote
                ORDER BY apn_hours";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllAttackPointNotes",
                Detail = $"Retrieved {result.Rows.Count} attack point notes",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllAttackPointNotes",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving attack point notes");
            throw;
        }
    }

    public async Task<bool> UpdateAttackPointNoteAsync(EvoAPI.Shared.DTOs.UpdateAttackPointNoteRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.AttackPointNote 
                SET 
                    apn_description = @Description,
                    apn_hours = @Hours,
                    apn_attack = @Attack,
                    apn_modifieddatetime = GETDATE()
                WHERE apn_id = @Id";

            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@Description", request.Description },
                { "@Hours", request.Hours },
                { "@Attack", request.Attack }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAttackPointNote",
                Detail = $"Updated attack point note {request.Id} - {request.Description}. Rows affected: {rowsAffected}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return rowsAffected > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAttackPointNote",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating attack point note {Id}", request.Id);
            throw;
        }
    }

    public async Task<int?> CreateAttackPointNoteAsync(EvoAPI.Shared.DTOs.CreateAttackPointNoteRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.AttackPointNote 
                (apn_description, apn_hours, apn_attack, apn_insertdatetime, apn_modifieddatetime)
                VALUES 
                (@Description, @Hours, @Attack, GETDATE(), GETDATE());
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@Description", request.Description },
                { "@Hours", request.Hours },
                { "@Attack", request.Attack }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var newId = await command.ExecuteScalarAsync();
            
            if (newId != null && int.TryParse(newId.ToString(), out var id))
            {
                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "CreateAttackPointNote",
                    Detail = $"Created new attack point note '{request.Description}' with ID {id}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return id;
            }
            
            return null;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateAttackPointNote",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating attack point note {Description}", request.Description);
            throw;
        }
    }

    // Attack Point Status methods
    public async Task<DataTable> GetAllAttackPointStatusAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    aps_id as Id,
                    aps_insertdatetime as InsertDateTime,
                    aps_modifieddatetime as ModifiedDateTime,
                    aps_daysinstatus as DaysInStatus,
                    aps_attack as Attack
                FROM dbo.AttackPointStatus
                ORDER BY aps_daysinstatus";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllAttackPointStatus",
                Detail = $"Retrieved {result.Rows.Count} attack point status records",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllAttackPointStatus",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving attack point status records");
            throw;
        }
    }

    public async Task<bool> UpdateAttackPointStatusAsync(EvoAPI.Shared.DTOs.UpdateAttackPointStatusRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.AttackPointStatus 
                SET 
                    aps_daysinstatus = @DaysInStatus,
                    aps_attack = @Attack,
                    aps_modifieddatetime = GETDATE()
                WHERE aps_id = @Id";

            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@DaysInStatus", request.DaysInStatus },
                { "@Attack", request.Attack }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAttackPointStatus",
                Detail = $"Updated attack point status {request.Id} - {request.DaysInStatus} days. Rows affected: {rowsAffected}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return rowsAffected > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAttackPointStatus",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating attack point status {Id}", request.Id);
            throw;
        }
    }

    public async Task<int?> CreateAttackPointStatusAsync(EvoAPI.Shared.DTOs.CreateAttackPointStatusRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.AttackPointStatus 
                (aps_daysinstatus, aps_attack, aps_insertdatetime, aps_modifieddatetime)
                VALUES 
                (@DaysInStatus, @Attack, GETDATE(), GETDATE());
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@DaysInStatus", request.DaysInStatus },
                { "@Attack", request.Attack }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var newId = await command.ExecuteScalarAsync();
            
            if (newId != null && int.TryParse(newId.ToString(), out var id))
            {
                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "CreateAttackPointStatus",
                    Detail = $"Created new attack point status '{request.DaysInStatus} days' with ID {id}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return id;
            }
            
            return null;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateAttackPointStatus",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating attack point status {DaysInStatus}", request.DaysInStatus);
            throw;
        }
    }

    // Attack Point Actionable Date methods
    public async Task<DataTable> GetAllAttackPointActionableDatesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    apad_id as Id,
                    apad_insertdatetime as InsertDateTime,
                    apad_modifieddatetime as ModifiedDateTime,
                    apad_description as Description,
                    apad_days as Days,
                    apad_attack as Attack
                FROM dbo.AttackPointActionableDate
                ORDER BY apad_days";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllAttackPointActionableDates",
                Detail = $"Retrieved {result.Rows.Count} attack point actionable dates",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllAttackPointActionableDates",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving attack point actionable dates");
            throw;
        }
    }

    public async Task<bool> UpdateAttackPointActionableDateAsync(EvoAPI.Shared.DTOs.UpdateAttackPointActionableDateRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.AttackPointActionableDate 
                SET
                    apad_description = @Description,
                    apad_days = @Days,
                    apad_attack = @Attack,
                    apad_modifieddatetime = GETDATE()
                WHERE apad_id = @Id";

            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@Description", request.Description },
                { "@Days", request.Days },
                { "@Attack", request.Attack }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAttackPointActionableDate",
                Detail = $"Updated attack point actionable date {request.Id} - {request.Description}. Rows affected: {rowsAffected}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return rowsAffected > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAttackPointActionableDate",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating attack point actionable date {Id}", request.Id);
            throw;
        }
    }

    public async Task<int?> CreateAttackPointActionableDateAsync(EvoAPI.Shared.DTOs.CreateAttackPointActionableDateRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.AttackPointActionableDate 
                (apad_description, apad_days, apad_attack, apad_insertdatetime, apad_modifieddatetime)
                VALUES 
                (@Description, @Days, @Attack, GETDATE(), GETDATE());
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@Description", request.Description },
                { "@Days", request.Days },
                { "@Attack", request.Attack }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var newId = await command.ExecuteScalarAsync();
            
            if (newId != null && int.TryParse(newId.ToString(), out var id))
            {
                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "CreateAttackPointActionableDate",
                    Detail = $"Created new attack point actionable date '{request.Description}' with ID {id}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return id;
            }
            
            return null;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateAttackPointActionableDate",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating attack point actionable date {Description}", request.Description);
            throw;
        }
    }

    public async Task<DataTable> GetAllUsersForManagementAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT u_id as Id, o_id as OId, a_id as AId, v_id as VId, supervisor_id as SupervisorId,
                       u_insertdatetime as InsertDateTime, u_modifieddatetime as ModifiedDateTime,
                       u_username as Username, u_password as Password, u_firstname as FirstName, u_lastname as LastName,
                       u_employeenumber as EmployeeNumber, u_email as Email, u_phonehome as PhoneHome, u_phonemobile as PhoneMobile,
                       u_phonedesk as PhoneDesk, u_extension as Extension,
                       u_active as Active, u_picture as Picture, u_ssn as SSN, u_dateofhire as DateOfHire,
                       u_dateeligiblepto as DateEligiblePTO, u_dateeligiblevacation as DateEligibleVacation,
                       u_daysavailablepto as DaysAvailablePTO, u_daysavailablevacation as DaysAvailableVacation,
                       u_clothingshirt as ClothingShirt, u_clothingjacket as ClothingJacket, u_clothingpants as ClothingPants,
                       u_wirelessprovider as WirelessProvider, u_preferrednotification as PreferredNotification,
                       u_quickbooksname as QuickBooksName, u_passwordchanged as PasswordChanged, u_2fa as U_2FA,
                       z_id as ZoneId, u_covidvaccinedate as CovidVaccineDate, u_note as Note, u_notedashboard as NoteDashboard
                FROM dbo.[User]
                ORDER BY u_firstname, u_lastname, u_username";
            
            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllUsersForManagement",
                Detail = $"Retrieved {result.Rows.Count} users for management",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllUsersForManagement",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving users for management");
            throw;
        }
    }

    public async Task<DataTable> GetUserByIdAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT u.u_id as Id, u.o_id as OId, u.a_id as AId, u.v_id as VId, u.supervisor_id as SupervisorId,
                       u.u_insertdatetime as InsertDateTime, u.u_modifieddatetime as ModifiedDateTime,
                       u.u_username as Username, u.u_password as Password, u.u_firstname as FirstName, u.u_lastname as LastName,
                       u.u_employeenumber as EmployeeNumber, u.u_email as Email, u.u_phonehome as PhoneHome, u.u_phonemobile as PhoneMobile,
                       u.u_phonedesk as PhoneDesk, u.u_extension as Extension,
                       u.u_active as Active, u.u_picture as Picture, u.u_ssn as SSN, u.u_dateofhire as DateOfHire,
                       u.u_dateeligiblepto as DateEligiblePTO, u.u_dateeligiblevacation as DateEligibleVacation,
                       u.u_daysavailablepto as DaysAvailablePTO, u.u_daysavailablevacation as DaysAvailableVacation,
                       u.u_clothingshirt as ClothingShirt, u.u_clothingjacket as ClothingJacket, u.u_clothingpants as ClothingPants,
                       u.u_wirelessprovider as WirelessProvider, u.u_preferrednotification as PreferredNotification,
                       u.u_quickbooksname as QuickBooksName, u.u_passwordchanged as PasswordChanged, u.u_2fa as U_2FA,
                       u.z_id as ZoneId, u.u_covidvaccinedate as CovidVaccineDate, u.u_note as Note, u.u_notedashboard as NoteDashboard,
                       a.a_address1 as Address1, a.a_address2 as Address2, a.a_city as City, a.a_state as State, a.a_zip as Zip
                FROM dbo.[User] u
                LEFT JOIN address a ON u.a_id = a.a_id
                WHERE u.u_id = @UserId";

            var parameters = new Dictionary<string, object>
            {
                { "@UserId", userId }
            };
            
            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetUserById",
                Detail = $"Retrieved user {userId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetUserById",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving user {UserId}", userId);
            throw;
        }
    }

    public async Task<int?> CreateUserAsync(CreateUserRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.[User] 
                (o_id, a_id, v_id, supervisor_id, u_insertdatetime, u_modifieddatetime, u_username, u_password, 
                 u_firstname, u_lastname, u_employeenumber, u_email, u_phonehome, u_phonemobile, u_phonedesk, u_extension, u_active, 
                 u_picture, u_ssn, u_dateofhire, u_dateeligiblepto, u_dateeligiblevacation, u_daysavailablepto, 
                 u_daysavailablevacation, u_clothingshirt, u_clothingjacket, u_clothingpants, u_wirelessprovider, 
                 u_preferrednotification, u_quickbooksname, u_passwordchanged, u_2fa, z_id, u_covidvaccinedate, 
                 u_note, u_notedashboard)
                VALUES 
                (@OId, @AId, @VId, @SupervisorId, GETDATE(), GETDATE(), @Username, @Password,
                 @FirstName, @LastName, @EmployeeNumber, @Email, @PhoneHome, @PhoneMobile, @PhoneDesk, @Extension, @Active,
                 @Picture, @SSN, @DateOfHire, @DateEligiblePTO, @DateEligibleVacation, @DaysAvailablePTO,
                 @DaysAvailableVacation, @ClothingShirt, @ClothingJacket, @ClothingPants, @WirelessProvider,
                 @PreferredNotification, @QuickBooksName, @PasswordChanged, @U_2FA, @ZoneId, @CovidVaccineDate,
                 @Note, @NoteDashboard);
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@OId", request.OId },
                { "@AId", request.AId ?? (object)DBNull.Value },
                { "@VId", request.VId ?? (object)DBNull.Value },
                { "@SupervisorId", request.SupervisorId ?? (object)DBNull.Value },
                { "@Username", request.Username },
                { "@Password", request.Password },
                { "@FirstName", request.FirstName ?? (object)DBNull.Value },
                { "@LastName", request.LastName ?? (object)DBNull.Value },
                { "@EmployeeNumber", request.EmployeeNumber ?? (object)DBNull.Value },
                { "@Email", request.Email ?? (object)DBNull.Value },
                { "@PhoneHome", request.PhoneHome ?? (object)DBNull.Value },
                { "@PhoneMobile", request.PhoneMobile ?? (object)DBNull.Value },
                { "@PhoneDesk", request.PhoneDesk ?? (object)DBNull.Value },
                { "@Extension", request.Extension ?? (object)DBNull.Value },
                { "@Active", request.Active },
                { "@Picture", request.Picture ?? (object)DBNull.Value },
                { "@SSN", request.SSN ?? (object)DBNull.Value },
                { "@DateOfHire", request.DateOfHire ?? (object)DBNull.Value },
                { "@DateEligiblePTO", request.DateEligiblePTO ?? (object)DBNull.Value },
                { "@DateEligibleVacation", request.DateEligibleVacation ?? (object)DBNull.Value },
                { "@DaysAvailablePTO", request.DaysAvailablePTO ?? (object)DBNull.Value },
                { "@DaysAvailableVacation", request.DaysAvailableVacation ?? (object)DBNull.Value },
                { "@ClothingShirt", request.ClothingShirt ?? (object)DBNull.Value },
                { "@ClothingJacket", request.ClothingJacket ?? (object)DBNull.Value },
                { "@ClothingPants", request.ClothingPants ?? (object)DBNull.Value },
                { "@WirelessProvider", request.WirelessProvider ?? (object)DBNull.Value },
                { "@PreferredNotification", request.PreferredNotification ?? (object)DBNull.Value },
                { "@QuickBooksName", request.QuickBooksName ?? (object)DBNull.Value },
                { "@PasswordChanged", DateTime.UtcNow },
                { "@U_2FA", request.U_2FA },
                { "@ZoneId", request.ZoneId ?? (object)DBNull.Value },
                { "@CovidVaccineDate", request.CovidVaccineDate ?? (object)DBNull.Value },
                { "@Note", request.Note ?? (object)DBNull.Value },
                { "@NoteDashboard", request.NoteDashboard ?? (object)DBNull.Value }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var newId = await command.ExecuteScalarAsync();
            
            if (newId != null && int.TryParse(newId.ToString(), out var id))
            {
                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "CreateUser",
                    Detail = $"Created new user '{request.Username}' ({request.FirstName} {request.LastName}) with ID {id}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return id;
            }
            
            return null;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateUser",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating user {Username}", request.Username);
            throw;
        }
    }

    public async Task<bool> UpdateUserAsync(UpdateUserRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // Build SQL dynamically to only update password if provided
            var sql = @"
                UPDATE dbo.[User] 
                SET 
                    o_id = @OId,
                    a_id = @AId,
                    v_id = @VId,
                    supervisor_id = @SupervisorId,
                    u_modifieddatetime = GETDATE(),
                    u_username = @Username," +
                    (string.IsNullOrEmpty(request.Password) ? "" : "u_password = @Password, u_passwordchanged = GETDATE(),") + @"
                    u_firstname = @FirstName,
                    u_lastname = @LastName,
                    u_employeenumber = @EmployeeNumber,
                    u_email = @Email,
                    u_phonehome = @PhoneHome,
                    u_phonemobile = @PhoneMobile,
                    u_phonedesk = @PhoneDesk,
                    u_extension = @Extension,
                    u_active = @Active,
                    u_picture = @Picture,
                    u_ssn = @SSN,
                    u_dateofhire = @DateOfHire,
                    u_dateeligiblepto = @DateEligiblePTO,
                    u_dateeligiblevacation = @DateEligibleVacation,
                    u_daysavailablepto = @DaysAvailablePTO,
                    u_daysavailablevacation = @DaysAvailableVacation,
                    u_clothingshirt = @ClothingShirt,
                    u_clothingjacket = @ClothingJacket,
                    u_clothingpants = @ClothingPants,
                    u_wirelessprovider = @WirelessProvider,
                    u_preferrednotification = @PreferredNotification,
                    u_quickbooksname = @QuickBooksName,
                    u_2fa = @U_2FA,
                    z_id = @ZoneId,
                    u_covidvaccinedate = @CovidVaccineDate,
                    u_note = @Note,
                    u_notedashboard = @NoteDashboard
                WHERE u_id = @Id";

            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@OId", request.OId },
                { "@AId", request.AId ?? (object)DBNull.Value },
                { "@VId", request.VId ?? (object)DBNull.Value },
                { "@SupervisorId", request.SupervisorId ?? (object)DBNull.Value },
                { "@Username", request.Username },
                { "@FirstName", request.FirstName ?? (object)DBNull.Value },
                { "@LastName", request.LastName ?? (object)DBNull.Value },
                { "@EmployeeNumber", request.EmployeeNumber ?? (object)DBNull.Value },
                { "@Email", request.Email ?? (object)DBNull.Value },
                { "@PhoneHome", request.PhoneHome ?? (object)DBNull.Value },
                { "@PhoneMobile", request.PhoneMobile ?? (object)DBNull.Value },
                { "@PhoneDesk", request.PhoneDesk ?? (object)DBNull.Value },
                { "@Extension", request.Extension ?? (object)DBNull.Value },
                { "@Active", request.Active },
                { "@Picture", request.Picture ?? (object)DBNull.Value },
                { "@SSN", request.SSN ?? (object)DBNull.Value },
                { "@DateOfHire", request.DateOfHire ?? (object)DBNull.Value },
                { "@DateEligiblePTO", request.DateEligiblePTO ?? (object)DBNull.Value },
                { "@DateEligibleVacation", request.DateEligibleVacation ?? (object)DBNull.Value },
                { "@DaysAvailablePTO", request.DaysAvailablePTO ?? (object)DBNull.Value },
                { "@DaysAvailableVacation", request.DaysAvailableVacation ?? (object)DBNull.Value },
                { "@ClothingShirt", request.ClothingShirt ?? (object)DBNull.Value },
                { "@ClothingJacket", request.ClothingJacket ?? (object)DBNull.Value },
                { "@ClothingPants", request.ClothingPants ?? (object)DBNull.Value },
                { "@WirelessProvider", request.WirelessProvider ?? (object)DBNull.Value },
                { "@PreferredNotification", request.PreferredNotification ?? (object)DBNull.Value },
                { "@QuickBooksName", request.QuickBooksName ?? (object)DBNull.Value },
                { "@U_2FA", request.U_2FA },
                { "@ZoneId", request.ZoneId ?? (object)DBNull.Value },
                { "@CovidVaccineDate", request.CovidVaccineDate ?? (object)DBNull.Value },
                { "@Note", request.Note ?? (object)DBNull.Value },
                { "@NoteDashboard", request.NoteDashboard ?? (object)DBNull.Value }
            };

            // Only add password parameter if password is being updated
            if (!string.IsNullOrEmpty(request.Password))
            {
                parameters.Add("@Password", request.Password);
            }

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateUser",
                Detail = $"Updated user {request.Id} - {request.Username} ({request.FirstName} {request.LastName}). Rows affected: {rowsAffected}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return rowsAffected > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateUser",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating user {Id}", request.Id);
            throw;
        }
    }

    public async Task<bool> UpdateUserDashboardNoteAsync(int userId, string? dashboardNote)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var sql = @"
                UPDATE dbo.[User] 
                SET 
                    u_notedashboard = @NoteDashboard,
                    u_modifieddatetime = GETDATE()
                WHERE u_id = @UserId";

            var parameters = new Dictionary<string, object>
            {
                { "@UserId", userId },
                { "@NoteDashboard", dashboardNote ?? (object)DBNull.Value }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            
            using var command = new SqlCommand(sql, connection);
            command.CommandTimeout = 30;
            
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value);
            }
            
            await connection.OpenAsync();
            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateUserDashboardNote",
                Detail = $"Updated dashboard note for user {userId}. Note: '{dashboardNote ?? "NULL"}'. Rows affected: {rowsAffected}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return rowsAffected > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateUserDashboardNote",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating dashboard note for user {UserId}", userId);
            throw;
        }
    }

    #region Employee Management Methods

    public async Task<DataTable> GetAllEmployeesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    u.u_id as Id,
                    u.u_firstname as FirstName,
                    u.u_lastname as LastName,
                    u.u_employeenumber as EmployeeNumber,
                    u.u_email as Email,
                    u.u_phonemobile as PhoneMobile,
                    u.u_phonehome as PhoneHome,
                    u.u_phonedesk as PhoneDesk,
                    u.u_extension as Extension,
                    u.u_username as Username,
                    u.u_password as Password,
                    u.u_active as Active,
                    u.u_daysavailablepto as DaysAvailablePTO,
                    u.u_daysavailablevacation as DaysAvailableVacation,
                    u.u_note as Note,
                    u.u_vehiclenumber as VehicleNumber,
                    u.u_picture as Picture,
                    u.z_id as ZoneId,
                    z.z_number as ZoneNumber,
                    z.z_description as ZoneName,
                    u.a_id as AddressId,
                    a.a_address1 as Address1,
                    a.a_address2 as Address2,
                    a.a_city as City,
                    a.a_state as State,
                    a.a_zip as Zip
                FROM dbo.[User] u
                LEFT JOIN dbo.Zone z ON u.z_id = z.z_id
                LEFT JOIN dbo.Address a ON u.a_id = a.a_id
                ORDER BY u.u_firstname, u.u_lastname, u.u_username";
            
            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllEmployees",
                Detail = $"Retrieved {result.Rows.Count} employees",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllEmployees",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving employees");
            throw;
        }
    }

    public async Task<DataTable> GetEmployeeByIdAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    u.u_id as Id,
                    u.u_firstname as FirstName,
                    u.u_lastname as LastName,
                    u.u_employeenumber as EmployeeNumber,
                    u.u_email as Email,
                    u.u_phonemobile as PhoneMobile,
                    u.u_phonehome as PhoneHome,
                    u.u_phonedesk as PhoneDesk,
                    u.u_extension as Extension,
                    u.u_username as Username,
                    u.u_password as Password,
                    u.u_active as Active,
                    u.u_daysavailablepto as DaysAvailablePTO,
                    u.u_daysavailablevacation as DaysAvailableVacation,
                    u.u_note as Note,
                    u.u_vehiclenumber as VehicleNumber,
                    u.u_picture as Picture,
                    u.z_id as ZoneId,
                    z.z_number as ZoneNumber,
                    z.z_description as ZoneName,
                    u.a_id as AddressId,
                    a.a_address1 as Address1,
                    a.a_address2 as Address2,
                    a.a_city as City,
                    a.a_state as State,
                    a.a_zip as Zip
                FROM dbo.[User] u
                LEFT JOIN dbo.Zone z ON u.z_id = z.z_id
                LEFT JOIN dbo.Address a ON u.a_id = a.a_id
                WHERE u.u_id = @UserId";
            
            var parameters = new Dictionary<string, object>
            {
                { "@UserId", userId }
            };
            
            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetEmployeeById",
                Detail = $"Retrieved employee with ID {userId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetEmployeeById",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving employee with ID {UserId}", userId);
            throw;
        }
    }

    public async Task<DataTable> GetAllRolesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    r_id as Id,
                    r_role as Name,
                    r_description as Description,
                    1 as Active
                FROM dbo.Role
                ORDER BY r_role";
            
            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllRoles",
                Detail = $"Retrieved {result.Rows.Count} roles",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllRoles",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving roles");
            throw;
        }
    }

    public async Task<DataTable> GetUserRolesByUserIdAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    xur.u_id as UserId,
                    xur.r_id as RoleId,
                    r.r_role as RoleName,
                    r.r_description as RoleDescription
                FROM dbo.XRefUserRole xur
                INNER JOIN dbo.Role r ON xur.r_id = r.r_id
                WHERE xur.u_id = @UserId
                ORDER BY r.r_role";
            
            var parameters = new Dictionary<string, object>
            {
                { "@UserId", userId }
            };
            
            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetUserRolesByUserId",
                Detail = $"Retrieved {result.Rows.Count} roles for user {userId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetUserRolesByUserId",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving roles for user {UserId}", userId);
            throw;
        }
    }

    public async Task<DataTable> GetAddressByIdAsync(int addressId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    a_id as Id,
                    a_insertdatetime as InsertDateTime,
                    a_modifieddatetime as ModifiedDateTime,
                    a_address1 as Address1,
                    a_address2 as Address2,
                    a_city as City,
                    a_state as State,
                    a_zip as Zip,
                    a_phone as Phone,
                    a_email as Email,
                    a_notes as Notes,
                    a_active as Active
                FROM dbo.Address
                WHERE a_id = @AddressId";
            
            var parameters = new Dictionary<string, object>
            {
                { "@AddressId", addressId }
            };
            
            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAddressById",
                Detail = $"Retrieved address with ID {addressId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAddressById",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving address with ID {AddressId}", addressId);
            throw;
        }
    }

    public async Task<int?> CreateEmployeeAsync(CreateEmployeeRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // First create address if provided
            int? addressId = null;
            if (!string.IsNullOrWhiteSpace(request.Address1))
            {
                var addressRequest = new CreateAddressRequest
                {
                    Address1 = request.Address1,
                    Address2 = request.Address2,
                    City = request.City,
                    State = request.State,
                    Zip = request.Zip,
                    Active = true
                };
                addressId = await CreateAddressAsync(addressRequest);
            }

            // Create the user record
            const string userSql = @"
                INSERT INTO dbo.[User] (
                    o_id, a_id, u_insertdatetime, u_username, u_password, u_firstname, u_lastname,
                    u_employeenumber, u_email, u_phonemobile, u_phonehome, u_phonedesk, u_extension,
                    u_active, u_daysavailablepto, u_daysavailablevacation, u_note, u_picture, z_id
                )
                OUTPUT INSERTED.u_id
                VALUES (
                    1, @AddressId, GETDATE(), @Username, @Password, @FirstName, @LastName,
                    @EmployeeNumber, @Email, @PhoneMobile, @PhoneHome, @PhoneDesk, @Extension,
                    @Active, @DaysAvailablePTO, @DaysAvailableVacation, @Note, @Picture, @ZoneId
                )";

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            
            using var command = new SqlCommand(userSql, connection);
            command.Parameters.AddWithValue("@AddressId", addressId ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Username", request.Username);
            command.Parameters.AddWithValue("@Password", request.Password);
            command.Parameters.AddWithValue("@FirstName", request.FirstName ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@LastName", request.LastName ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@EmployeeNumber", request.EmployeeNumber ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Email", request.Email ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@PhoneMobile", request.PhoneMobile ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@PhoneHome", request.PhoneHome ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@PhoneDesk", request.PhoneDesk ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Extension", request.Extension ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Active", request.Active);
            command.Parameters.AddWithValue("@DaysAvailablePTO", request.DaysAvailablePTO ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@DaysAvailableVacation", request.DaysAvailableVacation ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Note", request.Note ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Picture", request.Picture ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@ZoneId", request.ZoneId ?? (object)DBNull.Value);

            var userId = (int)await command.ExecuteScalarAsync();

            // Assign roles if provided
            if (request.RoleIds.Any())
            {
                await UpdateEmployeeRolesAsync(userId, request.RoleIds);
            }
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateEmployee",
                Detail = $"Created employee {request.FirstName} {request.LastName} with ID {userId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return userId;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateEmployee",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating employee {FirstName} {LastName}", request.FirstName, request.LastName);
            throw;
        }
    }

    public async Task<bool> UpdateEmployeeAsync(UpdateEmployeeRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // Update or create address if provided
            int? addressId = request.AddressId;
            if (!string.IsNullOrWhiteSpace(request.Address1))
            {
                if (addressId.HasValue)
                {
                    var addressRequest = new UpdateAddressRequest
                    {
                        Id = addressId.Value,
                        Address1 = request.Address1,
                        Address2 = request.Address2,
                        City = request.City,
                        State = request.State,
                        Zip = request.Zip,
                        Active = true
                    };
                    await UpdateAddressAsync(addressRequest);
                }
                else
                {
                    var addressRequest = new CreateAddressRequest
                    {
                        Address1 = request.Address1,
                        Address2 = request.Address2,
                        City = request.City,
                        State = request.State,
                        Zip = request.Zip,
                        Active = true
                    };
                    addressId = await CreateAddressAsync(addressRequest);
                }
            }

            // Update the user record
            var userSql = @"
                UPDATE dbo.[User] 
                SET 
                    a_id = @AddressId,
                    u_modifieddatetime = GETDATE(),
                    u_username = @Username,
                    u_firstname = @FirstName,
                    u_lastname = @LastName,
                    u_employeenumber = @EmployeeNumber,
                    u_email = @Email,
                    u_phonemobile = @PhoneMobile,
                    u_phonehome = @PhoneHome,
                    u_phonedesk = @PhoneDesk,
                    u_extension = @Extension,
                    u_active = @Active,
                    u_daysavailablepto = @DaysAvailablePTO,
                    u_daysavailablevacation = @DaysAvailableVacation,
                    u_note = @Note,
                    u_picture = @Picture,
                    z_id = @ZoneId";

            if (!string.IsNullOrWhiteSpace(request.Password))
            {
                userSql += ", u_password = @Password";
            }

            userSql += " WHERE u_id = @UserId";

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            
            using var command = new SqlCommand(userSql, connection);
            command.Parameters.AddWithValue("@UserId", request.Id);
            command.Parameters.AddWithValue("@AddressId", addressId ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Username", request.Username);
            command.Parameters.AddWithValue("@FirstName", request.FirstName ?? "");
            command.Parameters.AddWithValue("@LastName", request.LastName ?? "");
            command.Parameters.AddWithValue("@EmployeeNumber", request.EmployeeNumber ?? "");
            command.Parameters.AddWithValue("@Email", request.Email ?? "");
            command.Parameters.AddWithValue("@PhoneMobile", request.PhoneMobile ?? "");
            command.Parameters.AddWithValue("@PhoneHome", request.PhoneHome ?? "");
            command.Parameters.AddWithValue("@PhoneDesk", request.PhoneDesk ?? "");
            command.Parameters.AddWithValue("@Extension", request.Extension ?? "");
            command.Parameters.AddWithValue("@Active", request.Active);
            command.Parameters.AddWithValue("@DaysAvailablePTO", request.DaysAvailablePTO ?? 0);
            command.Parameters.AddWithValue("@DaysAvailableVacation", request.DaysAvailableVacation ?? 0);
            command.Parameters.AddWithValue("@Note", request.Note ?? "");
            command.Parameters.AddWithValue("@Picture", request.Picture ?? "");
            command.Parameters.AddWithValue("@ZoneId", request.ZoneId ?? (object)DBNull.Value);

            if (!string.IsNullOrWhiteSpace(request.Password))
            {
                command.Parameters.AddWithValue("@Password", request.Password);
            }

            var rowsAffected = await command.ExecuteNonQueryAsync();

            // Update roles
            await UpdateEmployeeRolesAsync(request.Id, request.RoleIds);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateEmployee",
                Detail = $"Updated employee {request.FirstName} {request.LastName} (ID: {request.Id})",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return rowsAffected > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateEmployee",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating employee {FirstName} {LastName} (ID: {Id})", request.FirstName, request.LastName, request.Id);
            throw;
        }
    }

    public async Task<bool> UpdateEmployeeRolesAsync(int userId, List<int> roleIds)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            
            // Remove all existing roles for the user
            const string deleteSql = "DELETE FROM dbo.XRefUserRole WHERE u_id = @UserId";
            using var deleteCommand = new SqlCommand(deleteSql, connection);
            deleteCommand.Parameters.AddWithValue("@UserId", userId);
            await deleteCommand.ExecuteNonQueryAsync();

            // Add new roles
            if (roleIds.Any())
            {
                const string insertSql = @"
                    INSERT INTO dbo.XRefUserRole (u_id, r_id)
                    VALUES (@UserId, @RoleId)";

                foreach (var roleId in roleIds)
                {
                    using var insertCommand = new SqlCommand(insertSql, connection);
                    insertCommand.Parameters.AddWithValue("@UserId", userId);
                    insertCommand.Parameters.AddWithValue("@RoleId", roleId);
                    await insertCommand.ExecuteNonQueryAsync();
                }
            }
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateEmployeeRoles",
                Detail = $"Updated roles for user {userId}. Assigned {roleIds.Count} roles.",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return true;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateEmployeeRoles",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating roles for user {UserId}", userId);
            throw;
        }
    }

    public async Task<int?> CreateAddressAsync(CreateAddressRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.Address (
                    a_insertdatetime, a_address1, a_address2, a_city, a_state, a_zip, a_active
                )
                OUTPUT INSERTED.a_id
                VALUES (
                    GETDATE(), @Address1, @Address2, @City, @State, @Zip, @Active
                )";

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            
            using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@Address1", request.Address1 ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Address2", request.Address2 ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@City", request.City ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@State", request.State ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Zip", request.Zip ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Active", request.Active);

            var addressId = (int)await command.ExecuteScalarAsync();
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateAddress",
                Detail = $"Created address with ID {addressId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return addressId;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateAddress",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating address");
            throw;
        }
    }

    public async Task<bool> UpdateAddressAsync(UpdateAddressRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.Address 
                SET 
                    a_modifieddatetime = GETDATE(),
                    a_address1 = @Address1,
                    a_address2 = @Address2,
                    a_city = @City,
                    a_state = @State,
                    a_zip = @Zip,
                    a_active = @Active
                WHERE a_id = @AddressId";

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            
            using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@AddressId", request.Id);
            command.Parameters.AddWithValue("@Address1", request.Address1 ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Address2", request.Address2 ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@City", request.City ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@State", request.State ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Zip", request.Zip ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Active", request.Active);

            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAddress",
                Detail = $"Updated address with ID {request.Id}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return rowsAffected > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAddress",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating address with ID {AddressId}", request.Id);
            throw;
        }
    }

    public async Task<DataTable> GetAllEmployeesWithRolesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    u.u_id as Id,
                    u.u_firstname as FirstName,
                    u.u_lastname as LastName,
                    u.u_employeenumber as EmployeeNumber,
                    u.u_email as Email,
                    u.u_phonemobile as PhoneMobile,
                    u.u_phonehome as PhoneHome,
                    u.u_phonedesk as PhoneDesk,
                    u.u_extension as Extension,
                    u.u_username as Username,
                    u.u_password as Password,
                    u.u_active as Active,
                    u.u_daysavailablepto as DaysAvailablePTO,
                    u.u_daysavailablevacation as DaysAvailableVacation,
                    u.u_note as Note,
                    u.u_vehiclenumber as VehicleNumber,
                    u.u_picture as Picture,
                    u.z_id as ZoneId,
                    z.z_number as ZoneNumber,
                    z.z_description as ZoneName,
                    u.a_id as AddressId,
                    a.a_address1 as Address1,
                    a.a_address2 as Address2,
                    a.a_city as City,
                    a.a_state as State,
                    a.a_zip as Zip,
                    -- Role information (nullable since LEFT JOIN)
                    xur.r_id as RoleId,
                    r.r_role as RoleName,
                    r.r_description as RoleDescription
                FROM dbo.[User] u
                LEFT JOIN dbo.Zone z ON u.z_id = z.z_id
                LEFT JOIN dbo.Address a ON u.a_id = a.a_id
                LEFT JOIN dbo.XRefUserRole xur ON u.u_id = xur.u_id
                LEFT JOIN dbo.Role r ON xur.r_id = r.r_id
                ORDER BY u.u_firstname, u.u_lastname, u.u_username, r.r_role";
            
            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllEmployeesWithRoles",
                Detail = $"Retrieved {result.Rows.Count} employee-role records",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllEmployeesWithRoles",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving employees with roles");
            throw;
        }
    }

    public async Task<DataTable> GetAllTradeGeneralsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    tg.tg_id as Id,
                    tg.tg_trade as Trade,
                    tg.tg_type as Type
                FROM TradeGeneral tg WITH(NOLOCK)
                ORDER BY tg.tg_type, tg.tg_trade";
            
            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllTradeGenerals",
                Detail = $"Retrieved {result.Rows.Count} trade generals",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllTradeGenerals",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving trade generals");
            throw;
        }
    }

    public async Task<DataTable> GetAllEmployeesWithRolesAndTradeGeneralsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    u.u_id as Id,
                    u.u_firstname as FirstName,
                    u.u_lastname as LastName,
                    u.u_employeenumber as EmployeeNumber,
                    u.u_email as Email,
                    u.u_phonemobile as PhoneMobile,
                    u.u_phonehome as PhoneHome,
                    u.u_phonedesk as PhoneDesk,
                    u.u_extension as Extension,
                    u.u_username as Username,
                    u.u_password as Password,
                    u.u_active as Active,
                    u.u_daysavailablepto as DaysAvailablePTO,
                    u.u_daysavailablevacation as DaysAvailableVacation,
                    u.u_note as Note,
                    u.u_vehiclenumber as VehicleNumber,
                    u.u_picture as Picture,
                    u.z_id as ZoneId,
                    z.z_number as ZoneNumber,
                    z.z_description as ZoneName,
                    u.a_id as AddressId,
                    a.a_address1 as Address1,
                    a.a_address2 as Address2,
                    a.a_city as City,
                    a.a_state as State,
                    a.a_zip as Zip,
                    -- Role information (nullable since LEFT JOIN)
                    xur.r_id as RoleId,
                    r.r_role as RoleName,
                    r.r_description as RoleDescription,
                    -- Trade General information (nullable since LEFT JOIN)
                    xutg.xutg_id as UserTradeGeneralId,
                    xutg.tg_id as TradeGeneralId,
                    tg.tg_trade as Trade,
                    tg.tg_type as TradeType
                FROM dbo.[User] u
                LEFT JOIN dbo.Zone z ON u.z_id = z.z_id
                LEFT JOIN dbo.Address a ON u.a_id = a.a_id
                LEFT JOIN dbo.XRefUserRole xur ON u.u_id = xur.u_id
                LEFT JOIN dbo.Role r ON xur.r_id = r.r_id
                LEFT JOIN dbo.xrefUserTradeGeneral xutg ON u.u_id = xutg.u_id
                LEFT JOIN dbo.TradeGeneral tg ON xutg.tg_id = tg.tg_id
                ORDER BY u.u_firstname, u.u_lastname, u.u_username, r.r_role, tg.tg_type, tg.tg_trade";
            
            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllEmployeesWithRolesAndTradeGenerals",
                Detail = $"Retrieved {result.Rows.Count} employee records with roles and trade generals",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllEmployeesWithRolesAndTradeGenerals",
                Detail = $"Error: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<DataTable> GetUserTradeGeneralsByUserIdAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    xutg.xutg_id as Id,
                    xutg.u_id as UserId,
                    xutg.tg_id as TradeGeneralId,
                    tg.tg_trade as Trade,
                    tg.tg_type as Type
                FROM xrefUserTradeGeneral xutg WITH(NOLOCK)
                    INNER JOIN TradeGeneral tg WITH(NOLOCK) ON xutg.tg_id = tg.tg_id
                WHERE xutg.u_id = @userId
                ORDER BY tg.tg_type, tg.tg_trade";

            var parameters = new Dictionary<string, object>
            {
                { "@userId", userId }
            };

            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetUserTradeGeneralsByUserId",
                Detail = $"Retrieved {result.Rows.Count} user trade generals for user {userId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetUserTradeGeneralsByUserId",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving user trade generals for user {UserId}", userId);
            throw;
        }
    }

    public async Task<bool> UpdateEmployeeTradeGeneralsAsync(int userId, List<int> tradeGeneralIds)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");
            }

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            using var transaction = connection.BeginTransaction();

            try
            {
                // First, delete all existing trade general assignments for this user
                const string deleteSql = "DELETE FROM xrefUserTradeGeneral WHERE u_id = @userId";
                using (var deleteCommand = new SqlCommand(deleteSql, connection, transaction))
                {
                    deleteCommand.Parameters.AddWithValue("@userId", userId);
                    await deleteCommand.ExecuteNonQueryAsync();
                }

                // Then, insert new trade general assignments
                if (tradeGeneralIds?.Count > 0)
                {
                    const string insertSql = @"
                        INSERT INTO xrefUserTradeGeneral (xutg_insertdatetime, u_id, tg_id)
                        VALUES (GETDATE(), @userId, @tradeGeneralId)";

                    foreach (var tradeGeneralId in tradeGeneralIds)
                    {
                        using var insertCommand = new SqlCommand(insertSql, connection, transaction);
                        insertCommand.Parameters.AddWithValue("@userId", userId);
                        insertCommand.Parameters.AddWithValue("@tradeGeneralId", tradeGeneralId);
                        await insertCommand.ExecuteNonQueryAsync();
                    }
                }

                transaction.Commit();
                
                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "UpdateEmployeeTradeGenerals",
                    Detail = $"Updated trade generals for user {userId}, assigned {tradeGeneralIds?.Count ?? 0} trade generals",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });
                
                return true;
            }
            catch (Exception ex)
            {
                transaction.Rollback();
                throw;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateEmployeeTradeGenerals",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Failed to update employee trade generals for user {UserId}", userId);
            throw;
        }
    }

    #endregion

    public async Task<DataTable> ExecuteQueryAsync(string sql, Dictionary<string, object>? parameters = null)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        var dataTable = new DataTable();
        
        using var connection = new SqlConnection(connectionString);
        connection.ConnectionString += ";Connection Timeout=30;";
        
        using var command = new SqlCommand(sql, connection);
        command.CommandTimeout = 30;
        
        if (parameters != null)
        {
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value ?? DBNull.Value);
            }
        }
        
        await connection.OpenAsync();
        using var adapter = new SqlDataAdapter(command);
        adapter.Fill(dataTable);
        
        return dataTable;
    }

    public async Task<int> ExecuteNonQueryAsync(string sql, Dictionary<string, object>? parameters = null)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        // Log the exact SQL being executed
        
        using var connection = new SqlConnection(connectionString);
        connection.ConnectionString += ";Connection Timeout=30;";
        
        using var command = new SqlCommand(sql, connection);
        command.CommandTimeout = 30;
        
        if (parameters != null)
        {
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value ?? DBNull.Value);
            }
        }
        
        await connection.OpenAsync();
        var rowsAffected = await command.ExecuteNonQueryAsync();
        
        return rowsAffected;
    }

    public async Task<DataTable> GetAllZonesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT z_id as Id, z_insertdatetime as InsertDateTime, z_modifieddatetime as ModifiedDateTime,
                       z_number as Number, z_description as Description, z_acronym as Acronym, u_id as UserId
                FROM dbo.Zone
                ORDER BY z_number";
            
            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllZones",
                Detail = $"Retrieved {result.Rows.Count} zones",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllZones",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving zones");
            throw;
        }
    }

    public async Task<DataTable> GetAllUsersAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT u_id as Id, o_id as OId, a_id as AId, v_id as VId, supervisor_id as SupervisorId,
                       u_insertdatetime as InsertDateTime, u_modifieddatetime as ModifiedDateTime,
                       u_username as Username, u_firstname as FirstName, u_lastname as LastName,
                       u_email as Email, u_active as Active, z_id as ZoneId
                FROM dbo.[User]
                WHERE u_active = 1
                ORDER BY u_firstname, u_lastname, u_username";
            
            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllUsers",
                Detail = $"Retrieved {result.Rows.Count} active users",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllUsers",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving users");
            throw;
        }
    }

    public async Task<DataTable> GetAdminUsersAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT DISTINCT u.u_id as Id, u.o_id as OId, u.a_id as AId, u.v_id as VId, u.supervisor_id as SupervisorId,
                       u.u_insertdatetime as InsertDateTime, u.u_modifieddatetime as ModifiedDateTime,
                       u.u_username as Username, u.u_firstname as FirstName, u.u_lastname as LastName,
                       u.u_email as Email, u.u_active as Active, u.z_id as ZoneId
                FROM dbo.[User] u
                INNER JOIN dbo.xrefUserRole xur ON u.u_id = xur.u_id
                INNER JOIN dbo.Role r ON xur.r_id = r.r_id
                WHERE u.u_active = 1 
                  AND (r.r_role = 'System Administrator')
                ORDER BY u.u_firstname, u.u_lastname, u.u_username";
            
            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAdminUsers",
                Detail = $"Retrieved {result.Rows.Count} active admin users",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAdminUsers",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving admin users");
            throw;
        }
    }

    public async Task<DataTable> GetActiveTechniciansAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    u.u_id as Id,
                    u.u_employeenumber as EmployeeNumber, 
                    u.u_firstname as FirstName, 
                    u.u_lastname as LastName, 
                    u.u_username as Username, 
                    u.u_email as Email, 
                    u.u_picture as Picture, 
                    u.u_phonemobile as PhoneMobile,
                    u.u_phonehome as PhoneHome,
                    u.u_phonedesk as PhoneDesk,
                    u.u_extension as Extension,
                    a.a_address1 as Address1,
                    a.a_address2 as Address2,
                    a.a_city as City,
                    a.a_state as State,
                    a.a_zip as Zip
                FROM [user] u
                INNER JOIN xrefUserRole x ON u.u_id = x.u_id
                INNER JOIN role r ON r.r_id = x.r_id
                LEFT JOIN address a ON u.a_id = a.a_id
                WHERE r.r_role = 'Technician' 
                    AND u.u_active = 1 
                ORDER BY u.u_lastname";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetActiveTechnicians",
                Detail = $"Retrieved {result.Rows.Count} active technicians",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetActiveTechnicians",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving active technicians");
            throw;
        }
    }

    public async Task<DataTable> GetAdminZoneStatusAssignmentsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT a.xazss_id as Id, a.xazss_insertdatetime as InsertDateTime, 
                       a.xazss_modifieddatetime as ModifiedDateTime,
                       a.u_id as UserId, a.z_id as ZoneId, a.ss_id as StatusSecondaryId,
                       u.u_firstname + ' ' + u.u_lastname as UserDisplayName,
                       z.z_number as ZoneName,
                       ss.ss_statussecondary as StatusSecondaryName
                FROM dbo.xrefAdminZoneStatusSecondary a
                INNER JOIN dbo.[User] u ON a.u_id = u.u_id
                INNER JOIN dbo.Zone z ON a.z_id = z.z_id
                INNER JOIN dbo.StatusSecondary ss ON a.ss_id = ss.ss_id
                ORDER BY z.z_number, ss.ss_statussecondary, u.u_firstname, u.u_lastname";
            
            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAdminZoneStatusAssignments",
                Detail = $"Retrieved {result.Rows.Count} admin zone status assignments",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAdminZoneStatusAssignments",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving admin zone status assignments");
            throw;
        }
    }

    public async Task<int?> CreateAdminZoneStatusAssignmentAsync(CreateAdminZoneStatusAssignmentRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.xrefAdminZoneStatusSecondary (xazss_insertdatetime, u_id, z_id, ss_id)
                VALUES (GETUTCDATE(), @UserId, @ZoneId, @StatusSecondaryId);
                SELECT SCOPE_IDENTITY();";
            
            var parameters = new Dictionary<string, object>
            {
                { "@UserId", request.UserId },
                { "@ZoneId", request.ZoneId },
                { "@StatusSecondaryId", request.StatusSecondaryId }
            };
            
            var result = await ExecuteQueryAsync(sql, parameters);
            var newId = result.Rows.Count > 0 ? Convert.ToInt32(result.Rows[0][0]) : (int?)null;
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateAdminZoneStatusAssignment",
                Detail = $"Created assignment for User {request.UserId}, Zone {request.ZoneId}, Status {request.StatusSecondaryId}. New ID: {newId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return newId;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateAdminZoneStatusAssignment",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating admin zone status assignment for User {UserId}, Zone {ZoneId}, Status {StatusSecondaryId}", 
                request.UserId, request.ZoneId, request.StatusSecondaryId);
            throw;
        }
    }

    public async Task<bool> UpdateAdminZoneStatusAssignmentAsync(UpdateAdminZoneStatusAssignmentRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.xrefAdminZoneStatusSecondary 
                SET xazss_modifieddatetime = GETUTCDATE(),
                    u_id = @UserId,
                    z_id = @ZoneId,
                    ss_id = @StatusSecondaryId
                WHERE xazss_id = @Id";
            
            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@UserId", request.UserId },
                { "@ZoneId", request.ZoneId },
                { "@StatusSecondaryId", request.StatusSecondaryId }
            };
            
            await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAdminZoneStatusAssignment",
                Detail = $"Updated assignment {request.Id} to User {request.UserId}, Zone {request.ZoneId}, Status {request.StatusSecondaryId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return true;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAdminZoneStatusAssignment",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating admin zone status assignment {Id}", request.Id);
            return false;
        }
    }

    public async Task<bool> DeleteAdminZoneStatusAssignmentAsync(DeleteAdminZoneStatusAssignmentRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                DELETE FROM dbo.xrefAdminZoneStatusSecondary 
                WHERE u_id = @UserId AND z_id = @ZoneId AND ss_id = @StatusSecondaryId";
            
            var parameters = new Dictionary<string, object>
            {
                { "@UserId", request.UserId },
                { "@ZoneId", request.ZoneId },
                { "@StatusSecondaryId", request.StatusSecondaryId }
            };
            
            await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "DeleteAdminZoneStatusAssignment",
                Detail = $"Deleted assignment for User {request.UserId}, Zone {request.ZoneId}, Status {request.StatusSecondaryId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return true;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "DeleteAdminZoneStatusAssignment",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error deleting admin zone status assignment for User {UserId}, Zone {ZoneId}, Status {StatusSecondaryId}", 
                request.UserId, request.ZoneId, request.StatusSecondaryId);
            return false;
        }
    }

    public async Task<DataTable> GetAttackPointsAsync(int topCount = 15)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
            -- Ultra-Optimized Version with Temp Tables
                    DECLARE @CutoffDate DATETIME = DATEADD(DAY, -730, GETDATE());
                    DECLARE @FutureDate DATETIME = DATEADD(DAY, 180, GETDATE());

                    -- Create temp tables with proper filtering
                    IF OBJECT_ID('tempdb..#BaseData') IS NOT NULL DROP TABLE #BaseData;
                    IF OBJECT_ID('tempdb..#WorkOrderNotes') IS NOT NULL DROP TABLE #WorkOrderNotes;
                    IF OBJECT_ID('tempdb..#StatusChanges') IS NOT NULL DROP TABLE #StatusChanges;

                    -- First, get the base set of work orders we care about
                    SELECT DISTINCT wo.wo_id
                    INTO #BaseData
                    FROM servicerequest sr WITH (NOLOCK)
                    INNER JOIN workorder wo WITH (NOLOCK) ON sr.wo_id_primary = wo.wo_id
                    INNER JOIN xrefCompanyCallCenter xccc WITH (NOLOCK) ON sr.xccc_id = xccc.xccc_id
                    INNER JOIN Company c WITH (NOLOCK) ON xccc.c_id = c.c_id
                    WHERE sr.s_id NOT IN (9, 6)
                    AND c.c_name NOT IN ('Metro Pipe Program')
                    AND (wo.wo_startdatetime BETWEEN @CutoffDate AND @FutureDate OR wo.wo_startdatetime IS NULL);

                    CREATE CLUSTERED INDEX IX_BaseData ON #BaseData(wo_id);

                    -- Get latest notes only for relevant work orders
                    SELECT won.wo_id, MAX(won.won_insertdatetime) as latest_note_datetime
                    INTO #WorkOrderNotes
                    FROM WorkOrderNote won WITH (NOLOCK)
                    WHERE won.wo_id IN (SELECT wo_id FROM #BaseData)
                    GROUP BY won.wo_id;

                    CREATE CLUSTERED INDEX IX_WON ON #WorkOrderNotes(wo_id);

                    -- Get latest status changes only for relevant work orders  
                    SELECT ssc.wo_id, MAX(ssc.ssc_insertdatetime) as latest_status_datetime
                    INTO #StatusChanges
                    FROM StatusSecondaryChange ssc WITH (NOLOCK)
                    WHERE ssc.wo_id IN (SELECT wo_id FROM #BaseData)
                    GROUP BY ssc.wo_id;

                    CREATE CLUSTERED INDEX IX_SSC ON #StatusChanges(wo_id);

                    -- Main query using pre-filtered data
                    WITH ranked_results AS (
                        SELECT sr.sr_id, 
                            sr.sr_insertdatetime, 
                            sr.sr_totaldue,
                            sr.sr_requestnumber,
                            sr.sr_datenextstep,
                            sr.sr_actionablenote,
                            sr.sr_escalated,
                            wo.wo_startdatetime,
                            z.z_number + '-' + z.z_acronym zone, 
                            cc.cc_name,
                            c.c_name,
                            p.p_priority,
                            ss.ss_statussecondary,
                            t.t_trade,
                            CASE 
                                WHEN won.latest_note_datetime IS NULL THEN NULL
                                ELSE DATEDIFF(HOUR, won.latest_note_datetime, GETDATE())
                            END as hours_since_last_note,
                            ISNULL(DATEDIFF(DAY, ssc.latest_status_datetime, GETDATE()), 0) as days_in_current_status,
                            cc.cc_attack as AttackCallCenter,
                            p.p_attack as AttackPriority, 
                            ss.ss_attack as AttackStatusSecondary,
                            -- Inline attack point calculations
                            ISNULL((
                                SELECT TOP 1 aps_attack
                                FROM AttackPointStatus WITH (NOLOCK)
                                WHERE ISNULL(DATEDIFF(DAY, ssc.latest_status_datetime, GETDATE()), 0) >= aps_daysinstatus
                                ORDER BY aps_daysinstatus DESC, aps_id DESC
                            ), 0) as AttackDaysInStatus,
                            ISNULL((
                                SELECT TOP 1 
                                    CASE 
                                        WHEN won.latest_note_datetime IS NULL THEN apn_attack
                                        WHEN CAST(wo.wo_startdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS DATE) >= 
                                            CAST(GETDATE() AT TIME ZONE 'Central Standard Time' AS DATE) THEN 0
                                        ELSE apn_attack
                                    END
                                FROM AttackPointNote WITH (NOLOCK)
                                WHERE (won.latest_note_datetime IS NULL AND apn_id = 1)
                                OR (won.latest_note_datetime IS NOT NULL 
                                    AND DATEDIFF(HOUR, won.latest_note_datetime, GETDATE()) >= apn_hours
                                    AND apn_id > 1)
                                ORDER BY CASE WHEN won.latest_note_datetime IS NULL THEN 0 ELSE apn_hours END DESC
                            ), 0) as AttackHoursSinceLastNote,
                            ISNULL((
                                SELECT TOP 1 apad_attack
                                FROM AttackPointActionableDate WITH (NOLOCK)
                                WHERE (sr.sr_datenextstep IS NULL AND apad_id = 1)
                                OR (sr.sr_datenextstep IS NOT NULL 
                                    AND DATEDIFF(DAY, GETDATE(), sr.sr_datenextstep) <= apad_days
                                    AND apad_id > 1)
                                ORDER BY CASE WHEN sr.sr_datenextstep IS NULL THEN 0 ELSE apad_days END ASC
                            ), 0) as AttackActionableDate,
                            admin_user.u_id as admin_u_id,
                            admin_user.u_firstname as admin_firstname,
                            admin_user.u_lastname as admin_lastname,
                            CASE WHEN sr.sr_escalated IS NOT NULL THEN 1 ELSE 0 END as is_escalated
                        FROM servicerequest sr WITH (NOLOCK)
                        INNER JOIN workorder wo WITH (NOLOCK) ON sr.wo_id_primary = wo.wo_id
                        INNER JOIN #BaseData bd ON wo.wo_id = bd.wo_id -- Use filtered base
                        INNER JOIN xrefCompanyCallCenter xccc WITH (NOLOCK) ON sr.xccc_id = xccc.xccc_id
                        INNER JOIN Company c WITH (NOLOCK) ON xccc.c_id = c.c_id
                        INNER JOIN callcenter cc WITH (NOLOCK) ON xccc.cc_id = cc.cc_id
                        LEFT JOIN xrefWorkOrderUser xwou WITH (NOLOCK) ON xwou.wo_id = wo.wo_id 
                            AND (sr.sr_escalated IS NOT NULL OR xwou.wo_id IS NOT NULL)
                        LEFT JOIN [user] u WITH (NOLOCK) ON xwou.u_id = u.u_id
                        INNER JOIN location l WITH (NOLOCK) ON sr.l_id = l.l_id
                        INNER JOIN address a WITH (NOLOCK) ON l.a_id = a.a_id
                        INNER JOIN tax WITH (NOLOCK) ON LEFT(a.a_zip,5) = tax.tax_zip
                        INNER JOIN ZoneMicro zm WITH (NOLOCK) ON tax.zm_id = zm.zm_id
                        INNER JOIN zone z WITH (NOLOCK) ON CASE 
                            WHEN cc.cc_name = 'Residential' THEN (SELECT z_id FROM zone WHERE z_acronym = 'Residential')
                            ELSE zm.z_id 
                        END = z.z_id
                        INNER JOIN statussecondary ss WITH (NOLOCK) ON wo.ss_id = ss.ss_id
                        INNER JOIN Priority p WITH (NOLOCK) ON sr.p_id = p.p_id
                        LEFT JOIN trade t WITH (NOLOCK) ON sr.t_id = t.t_id
                        INNER JOIN xrefadminzonestatussecondary xazss WITH (NOLOCK) ON z.z_id = xazss.z_id AND ss.ss_id = xazss.ss_id
                        INNER JOIN [user] admin_user WITH (NOLOCK) ON xazss.u_id = admin_user.u_id
                        LEFT JOIN #WorkOrderNotes won ON won.wo_id = wo.wo_id
                        LEFT JOIN #StatusChanges ssc ON ssc.wo_id = wo.wo_id
                        WHERE sr.s_id NOT IN (9, 6)
                        AND c.c_name NOT IN ('Metro Pipe Program')
                    ),
                    final_with_attack_points AS (
                        SELECT *,
                            (AttackPriority + AttackStatusSecondary + AttackDaysInStatus + 
                            AttackHoursSinceLastNote + AttackCallCenter + AttackActionableDate) as AttackPoints,
                            CASE 
                                WHEN is_escalated = 1 THEN NULL
                                WHEN cc_name = 'Administrative' THEN NULL
                                ELSE ROW_NUMBER() OVER (
                                    PARTITION BY admin_u_id
                                    ORDER BY (AttackPriority + AttackStatusSecondary + AttackDaysInStatus + 
                                            AttackHoursSinceLastNote + AttackCallCenter + AttackActionableDate) DESC
                                )
                            END as rn_non_escalated
                        FROM ranked_results
                    )
                    SELECT sr_id, 
                        sr_insertdatetime, 
                        sr_totaldue,
                        sr_requestnumber,
                        sr_datenextstep,
                        sr_actionablenote,
                        sr_escalated,
                        wo_startdatetime,
                        zone, 
                        admin_u_id,
                        admin_firstname,
                        admin_lastname,
                        cc_name,
                        c_name,
                        p_priority,
                        ss_statussecondary,
                        t_trade, 
                        hours_since_last_note,
                        days_in_current_status,
                        AttackCallCenter,
                        AttackPriority, 
                        AttackStatusSecondary,
                        AttackHoursSinceLastNote,
                        AttackDaysInStatus,
                        AttackActionableDate,
                        AttackPoints,
                        is_escalated
                    FROM final_with_attack_points
                    WHERE (rn_non_escalated <= @TopCount) OR (is_escalated = 1)
                    ORDER BY ISNULL(admin_u_id, -1), is_escalated DESC, AttackPoints DESC;

                    -- Clean up
                    DROP TABLE #BaseData;
                    DROP TABLE #WorkOrderNotes;
                    DROP TABLE #StatusChanges;
            ";

            
            var parameters = new Dictionary<string, object>
            {
                { "@TopCount", topCount }
            };
            
            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            
            _logger.LogError(ex, "Error retrieving attack points with top {TopCount} results", topCount);
            throw;
        }
    }

    public async Task<DataTable> GetHighVolumeDashboardAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @" 

WITH LastFiveWeekdays AS (
    SELECT 
        DATEADD(DAY, -daysToSubtract, CONVERT(DATE, GETDATE())) AS Date,
        ROW_NUMBER() OVER (ORDER BY daysToSubtract) AS DayOrder,
        FORMAT(DATEADD(DAY, -daysToSubtract, CONVERT(DATE, GETDATE())), 'dddd') AS DayName
    FROM (
        SELECT TOP 5 
            SUM(CASE 
                    WHEN DATEPART(WEEKDAY, DATEADD(DAY, -number, CONVERT(DATE, GETDATE()))) IN (1, 7) THEN 1 
                    ELSE 0 
                END) OVER (ORDER BY number) + number AS daysToSubtract
        FROM (
            SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS number
            FROM sys.objects a
            CROSS JOIN sys.objects b
        ) nums
        WHERE DATEPART(WEEKDAY, DATEADD(DAY, -number, CONVERT(DATE, GETDATE()))) NOT IN (1, 7)
        ORDER BY number
    ) d
),
ActiveTechs AS (
    SELECT DISTINCT 
        u.u_firstname + ' ' + u.u_lastname AS Tech
    FROM HighVolumeBatchDetail hvbd
    JOIN [user] u ON hvbd.u_id = u.u_id
    WHERE DATEADD(HOUR, -6, hvbd.hvbd_completeddatetime) >= DATEADD(DAY, -30, GETDATE())
),
DailyStats AS (
    SELECT 
        u.u_firstname + ' ' + u.u_lastname AS Tech,
        CONVERT(DATE, DATEADD(HOUR, -6, hvbd.hvbd_completeddatetime)) AS CompletionDate,
        COUNT(hvbd.sr_id) AS CompletionCount
    FROM HighVolumeBatchDetail hvbd
    JOIN [user] u ON hvbd.u_id = u.u_id
    WHERE DATEADD(HOUR, -6, hvbd.hvbd_completeddatetime) >= DATEADD(DAY, -30, GETDATE())
    GROUP BY 
        u.u_firstname + ' ' + u.u_lastname,
        CONVERT(DATE, DATEADD(HOUR, -6, hvbd.hvbd_completeddatetime))
),
CrossJoined AS (
    SELECT 
        t.Tech,
        d.Date,
        d.DayOrder,
        d.DayName,
        ISNULL(ds.CompletionCount, 0) AS CompletionCount
    FROM ActiveTechs t
    CROSS JOIN LastFiveWeekdays d
    LEFT JOIN DailyStats ds ON ds.Tech = t.Tech 
        AND ds.CompletionDate = d.Date
),
DailyTechSummary AS (
    SELECT 
        Tech,
        MAX(CASE WHEN DayOrder = 1 THEN CompletionCount ELSE 0 END) AS [Today],
        MAX(CASE WHEN DayOrder = 2 THEN CompletionCount ELSE 0 END) AS [Previous_1],
        MAX(CASE WHEN DayOrder = 3 THEN CompletionCount ELSE 0 END) AS [Previous_2],
        MAX(CASE WHEN DayOrder = 4 THEN CompletionCount ELSE 0 END) AS [Previous_3],
        MAX(CASE WHEN DayOrder = 5 THEN CompletionCount ELSE 0 END) AS [Previous_4],
        MAX(CASE WHEN DayOrder = 1 THEN DayName END) AS Today_Name,
        MAX(CASE WHEN DayOrder = 2 THEN DayName END) AS Previous_1_Name,
        MAX(CASE WHEN DayOrder = 3 THEN DayName END) AS Previous_2_Name,
        MAX(CASE WHEN DayOrder = 4 THEN DayName END) AS Previous_3_Name,
        MAX(CASE WHEN DayOrder = 5 THEN DayName END) AS Previous_4_Name
    FROM CrossJoined
    GROUP BY Tech
),
NotCompleted AS (
    SELECT COUNT(*) AS NotCompleted
    FROM HighVolumeBatchDetail
    WHERE hvbd_completeddatetime IS NULL
)
-- Per-tech rows (NotCompleted is NULL here to avoid repeating the same scalar)
SELECT 
    Tech,
    [Today],
    [Previous_1],
    [Previous_2],
    [Previous_3],
    [Previous_4],
    Today_Name,
    Previous_1_Name,
    Previous_2_Name,
    Previous_3_Name,
    Previous_4_Name,
    CAST(NULL AS INT) AS NotCompleted
FROM DailyTechSummary

UNION ALL

-- TOTAL row with the single NotCompleted value
SELECT
    'TOTAL' AS Tech,
    SUM([Today]) AS [Today],
    SUM([Previous_1]) AS [Previous_1],
    SUM([Previous_2]) AS [Previous_2],
    SUM([Previous_3]) AS [Previous_3],
    SUM([Previous_4]) AS [Previous_4],
    MAX(Today_Name) AS Today_Name,
    MAX(Previous_1_Name) AS Previous_1_Name,
    MAX(Previous_2_Name) AS Previous_2_Name,
    MAX(Previous_3_Name) AS Previous_3_Name,
    MAX(Previous_4_Name) AS Previous_4_Name,
    (SELECT NotCompleted FROM NotCompleted) AS NotCompleted
FROM DailyTechSummary;

";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetHighVolumeDashboard",
                Detail = "Retrieved high volume dashboard data",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetHighVolumeDashboard",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving high volume dashboard data");
            throw;
        }
    }

    public async Task<DataTable> GetReceiptsDashboardAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var sql = @"

                SELECT 
                    cc.cc_name,
                    c.c_name,
                    rt.rt_receipttype ,
                    supplier.c_name as supplier,
                    att.att_company as supplierEntered,
                    sr.sr_requestnumber,
                    wou.u_firstname ,
                    wou.u_lastname ,
                    u_submittedby.u_firstname + ' ' + u_submittedby.u_lastname as submittedBy,
                    att.att_receiptamount ,
                    att.att_insertdatetime ,
                    att.att_filename ,
                    att.att_description ,
                    att.att_comment ,
                    att.att_path ,
                    sr.sr_id ,
                    sr.wo_id_primary as wo_id,
                    att.att_id ,
                    att.att_extension,
                    t.t_id,
                    t.t_trade
                FROM attachment att with(nolock)
                LEFT JOIN receipttype rt with(nolock) on att.rt_id = rt.rt_id
                LEFT JOIN servicerequest sr with(nolock) on att.sr_id = sr.sr_id
                LEFT JOIN trade t with(nolock) on sr.t_id = t.t_id
                LEFT JOIN xrefWorkOrderUser xwou with(nolock) on sr.wo_id_primary = xwou.wo_id 
                LEFT JOIN [user] wou with(nolock) on xwou.u_id = wou.u_id
                LEFT JOIN [user] u_submittedby with(nolock) on att.u_id_submittedby = u_submittedby.u_id
                LEFT JOIN xrefCompanyCallCenter xccc with(nolock) on sr.xccc_id = xccc.xccc_id
                LEFT JOIN company c with(nolock) on xccc.c_id = c.c_id
                LEFT JOIN company supplier with(nolock) on att.c_id = supplier.c_id
                LEFT JOIN callcenter cc with(nolock) on xccc.cc_id = cc.cc_id
                WHERE att_receipt = 1
                ORDER BY att_id desc
";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetReceiptsDashboard",
                Detail = $"Retrieved {result.Rows.Count} receipt records",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetReceiptsDashboard",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving receipts dashboard data");
            throw;
        }
    }

    public async Task<DataTable> GetTechDetailDashboardAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");
            }

            var sql = @"
                WITH RankedPerformance AS (
                    SELECT 
                        u.u_id, 
                        u.u_firstname, 
                        u.u_lastname, 
                        u.a_id,  -- Include a_id for joining with address table
                        u.z_id,  -- Include z_id for joining with zone table
                        perf.perf_id, 
                        CONVERT(DATE, perf.perf_insertdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time') AS perf_insertdate, 
                        perf.perf_utilization, 
                        perf.perf_profitability, 
                        perf.perf_attendance, 
                        perf.perf_comment,
                        ROW_NUMBER() OVER (PARTITION BY perf.u_id ORDER BY perf.perf_insertdatetime DESC) AS rn
                    FROM performance perf
                    JOIN [user] u ON perf.u_id = u.u_id
                    WHERE u.u_active = 1
                )
                SELECT 
                    rp.u_id, 
                    rp.u_firstname, 
                    rp.u_lastname, 
                    rp.perf_id, 
                    rp.perf_insertdate,  -- Only the date in CST
                    rp.perf_utilization, 
                    rp.perf_profitability, 
                    rp.perf_attendance, 
                    rp.perf_comment,
                    a.a_address1,
                    a.a_address2,
                    a.a_city,
                    a.a_state,
                    a.a_zip,
                    z.z_id,
                    z.z_number
                FROM RankedPerformance rp
                LEFT JOIN address a ON rp.a_id = a.a_id  -- Join with address table if a_id exists
                LEFT JOIN zone z ON rp.z_id = z.z_id     -- Join with zone table if z_id exists
                WHERE rp.rn = 1
                    AND rp.u_id NOT IN (SELECT DISTINCT u_id FROM zone WHERE u_id IS NOT NULL)  -- Exclude zone managers
                ORDER BY z.z_number, rp.u_lastname, rp.u_firstname;
            ";

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand(sql, connection))
                {
                    command.CommandTimeout = 60;
                    var adapter = new SqlDataAdapter(command);
                    var dataTable = new DataTable();
                    adapter.Fill(dataTable);
                    
                    stopwatch.Stop();
                    await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                    {
                        Name = "DataService",
                        Description = "GetTechDetailDashboard",
                        Detail = $"Retrieved {dataTable.Rows.Count} tech detail records",
                        ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                        MachineName = Environment.MachineName
                    });
                    
                    return dataTable;
                }
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTechDetailDashboard",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving tech detail dashboard data");
            throw;
        }
    }

    public async Task<DataTable> GetTechDetailByTechnicianAsync(int technicianId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");
            }

            var sql = @"
                SELECT TOP 5
                    u.u_id, 
                    u.u_firstname, 
                    u.u_lastname, 
                    perf.perf_id, 
                    CONVERT(DATE, perf.perf_insertdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time') AS perf_insertdate, 
                    perf.perf_utilization, 
                    perf.perf_profitability, 
                    perf.perf_attendance, 
                    perf.perf_comment,
                    a.a_address1,
                    a.a_address2,
                    a.a_city,
                    a.a_state,
                    a.a_zip,
                    z.z_id,
                    z.z_number
                FROM performance perf
                JOIN [user] u ON perf.u_id = u.u_id
                LEFT JOIN address a ON u.a_id = a.a_id
                LEFT JOIN zone z ON u.z_id = z.z_id
                WHERE u.u_active = 1 
                    AND perf.u_id = @technicianId
                ORDER BY perf.perf_insertdatetime DESC;
            ";

            var parameters = new Dictionary<string, object>
            {
                { "@technicianId", technicianId }
            };

            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTechDetailByTechnician",
                Detail = $"Retrieved {result.Rows.Count} performance records for technician {technicianId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTechDetailByTechnician",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving tech detail data for technician {TechnicianId}", technicianId);
            throw;
        }
    }

    public async Task<DataTable> GetTechActivityDashboardAsync(DateTime? startDate = null, DateTime? endDate = null)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            
            // Default to 90 days if no dates provided
            var effectiveStartDate = startDate ?? DateTime.Now.AddDays(-90);
            var effectiveEndDate = endDate ?? DateTime.Now;
            
            const string sql = @"
                SELECT 
                    tt.tt_id,
                    ttt.ttt_id,
                    tt.u_id,
                    tt.wo_id,
                    wo.sr_id,
                    sr.t_id,
                    ttt.ttt_timetype,
                    ttt.ttt_paidtime,
                    FORMAT(tt.tt_begin AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'yyyy-MM-dd HH:mm') AS tt_begin,
                    FORMAT(tt.tt_end AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'yyyy-MM-dd HH:mm') AS tt_end,
                    tt.tt_invoicedrate,
                    u.u_firstname,
                    u.u_lastname,
                    sr.sr_requestnumber,
                    t.t_trade,
                    cc.cc_name,
                    c.c_name,
                    z.z_id AS techz_id,
                    z.z_number + '-'+ z.z_acronym AS techzone,
                    CASE 
                        WHEN cc.cc_name = 'Residential' THEN resz.z_id 
                        ELSE srz.z_id 
                    END AS srz_id,
                    CASE 
                        WHEN cc.cc_name = 'Residential' THEN resz.z_number + '-'+ resz.z_acronym 
                        ELSE srz.z_number + '-'+ srz.z_acronym 
                    END AS srzone
                FROM timetracking tt
                    INNER JOIN TimeTrackingType ttt ON tt.ttt_id = ttt.ttt_id
                    INNER JOIN [user] u ON tt.u_id = u.u_id
                    LEFT JOIN workorder wo ON tt.wo_id = wo.wo_id
                    LEFT JOIN servicerequest sr ON wo.sr_id = sr.sr_id
                    LEFT JOIN trade t ON sr.t_id = t.t_id
                    LEFT JOIN xrefCompanyCallCenter xccc ON sr.xccc_id = xccc.xccc_id
                    LEFT JOIN callcenter cc ON xccc.cc_id = cc.cc_id
                    LEFT JOIN Company c ON xccc.c_id = c.c_id
                    LEFT JOIN Zone z ON u.z_id = z.z_id
                    -- Service Request Zone joins
                    LEFT JOIN location l ON sr.l_id = l.l_id
                    LEFT JOIN address a ON l.a_id = a.a_id
                    LEFT JOIN tax ON LEFT(a.a_zip, 5) = tax.tax_zip
                    LEFT JOIN ZoneMicro zm ON tax.zm_id = zm.zm_id
                    LEFT JOIN zone srz ON zm.z_id = srz.z_id
                    -- Residential zone lookup
                    LEFT JOIN zone resz ON resz.z_acronym = 'Residential'
                WHERE tt.tt_begin >= @StartDate 
                    AND tt.tt_begin <= @EndDate
                    AND ttt.ttt_id NOT IN (1)
                ORDER BY tt.tt_id DESC;
            ";

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@StartDate", effectiveStartDate);
                    command.Parameters.AddWithValue("@EndDate", effectiveEndDate);
                    command.CommandTimeout = 60;
                    var adapter = new SqlDataAdapter(command);
                    var dataTable = new DataTable();
                    adapter.Fill(dataTable);
                    
                    stopwatch.Stop();
                    await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                    {
                        Name = "DataService",
                        Description = "GetTechActivityDashboard",
                        Detail = $"Retrieved {dataTable.Rows.Count} tech activity records from {effectiveStartDate:yyyy-MM-dd} to {effectiveEndDate:yyyy-MM-dd}",
                        ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                        MachineName = Environment.MachineName
                    });
                    
                    return dataTable;
                }
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTechActivityDashboard",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving tech activity dashboard data");
            throw;
        }
    }

    public async Task<DataTable> GetServiceRequestNumberChangesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");
            }

            const string sql = @"
                SELECT
                    sr.sr_id,
                    sr.sr_insertdatetime AS 'Created Date',
                    cc.cc_name AS 'Call Center', 
                    c.c_name AS 'Company', 
                    sr.sr_requestnumber AS 'Service Request', 
                    wo.wo_workordernumber AS 'Primary Work Order'
                FROM callcenter cc, 
                     company c, 
                     xrefcompanycallcenter xccc, 
                     servicerequest sr, 
                     workorder wo
                WHERE cc.cc_id = xccc.cc_id
                AND c.c_id = xccc.c_id
                AND sr.xccc_id = xccc.xccc_id
                AND sr.wo_id_primary = wo.wo_id
                AND sr.sr_requestnumber != LEFT(wo.wo_workordernumber, LEN(wo.wo_workordernumber) - 2)
                AND sr.sr_requestnumber NOT LIKE '%Parts Pickup%'
                ORDER BY 1 DESC
            ";

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand(sql, connection))
                {
                    command.CommandTimeout = 60;
                    var adapter = new SqlDataAdapter(command);
                    var dataTable = new DataTable();
                    adapter.Fill(dataTable);
                    
                    stopwatch.Stop();
                    await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                    {
                        Name = "DataService",
                        Description = "GetServiceRequestNumberChanges",
                        Detail = $"Retrieved {dataTable.Rows.Count} service request number changes records",
                        ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                        MachineName = Environment.MachineName
                    });
                    
                    return dataTable;
                }
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetServiceRequestNumberChanges",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving service request number changes dashboard data");
            throw;
        }
    }

    public async Task<DataTable> GetActiveServiceRequestsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");
            }

            const string sql = @"
                select sr.sr_id, sr.sr_requestnumber, sr.sr_insertdatetime, s.s_status, u.u_firstname, u.u_lastname, u.u_active
                from servicerequest sr, workorder wo, xrefworkorderuser x, status s, [user] u, xrefWorkOrderUser xwou
                where wo.wo_id = x.wo_id
                and sr.sr_id = wo.sr_id
                and sr.s_id = s.s_id
                and wo.wo_id = xwou.wo_id
                and xwou.u_id = u.u_id
                and s.s_id not in (
                    select cast(value as int) 
                    from configsetting cs
                    cross apply string_split(cs.cs_value, ',')
                    where cs.cs_identifier = 'WorkOrderStatusActive'
                )
                order by u_lastname
            ";

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand(sql, connection))
                {
                    command.CommandTimeout = 60;
                    var adapter = new SqlDataAdapter(command);
                    var dataTable = new DataTable();
                    adapter.Fill(dataTable);
                    
                    stopwatch.Stop();
                    await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                    {
                        Name = "DataService",
                        Description = "GetActiveServiceRequests",
                        Detail = $"Retrieved {dataTable.Rows.Count} active service requests records",
                        ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                        MachineName = Environment.MachineName
                    });
                    
                    return dataTable;
                }
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetActiveServiceRequests",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving active service requests data");
            throw;
        }
    }

    public async Task<List<MissingReceiptDashboardDto>> GetMissingReceiptsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT u.u_id, rm.rm_id, rm_dateupload, rm_datereceipt, rm_description, rm_amount, 
                       u.u_firstname, u.u_lastname, u.u_employeenumber
                FROM receiptmissing rm
                INNER JOIN [user] u ON u.u_employeenumber = rm.u_employeenumber
                WHERE CAST(CAST(rm.rm_dateupload AS DATETIME) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS DATE) = (
                    SELECT MAX(CAST(CAST(rm_dateupload AS DATETIME) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS DATE)) 
                    FROM receiptmissing
                )
                ORDER BY u.u_firstname, u.u_lastname, rm.rm_id DESC";

            using var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection"));
            using var command = new SqlCommand(sql, connection);
            
            await connection.OpenAsync();
            using var reader = await command.ExecuteReaderAsync();
            
            var receipts = new List<MissingReceiptDashboardDto>();
            
            while (await reader.ReadAsync())
            {
                receipts.Add(new MissingReceiptDashboardDto
                {
                    UId = ConvertToInt(reader["u_id"]),
                    RmId = ConvertToInt(reader["rm_id"]),
                    RmDateUpload = reader["rm_dateupload"] as DateTime?,
                    RmDateReceipt = reader["rm_datereceipt"] as DateTime?,
                    RmDescription = reader["rm_description"]?.ToString(),
                    RmAmount = reader["rm_amount"] as decimal?,
                    UFirstName = reader["u_firstname"]?.ToString(),
                    ULastName = reader["u_lastname"]?.ToString(),
                    UEmployeeNumber = reader["u_employeenumber"]?.ToString()
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetMissingReceipts",
                Detail = $"Retrieved {receipts.Count} missing receipts for all users",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return receipts;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetMissingReceipts",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving missing receipts");
            throw;
        }
    }

    public async Task<List<MissingReceiptDashboardDto>> GetMissingReceiptsByUserAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT u.u_id, rm.rm_id, rm_dateupload, rm_datereceipt, rm_description, rm_amount, 
                       u.u_firstname, u.u_lastname, u.u_employeenumber
                FROM receiptmissing rm
                INNER JOIN [user] u ON u.u_employeenumber = rm.u_employeenumber
                WHERE u.u_id = @userId
                  AND CAST(CAST(rm.rm_dateupload AS DATETIME) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS DATE) = (
                    SELECT MAX(CAST(CAST(rm_dateupload AS DATETIME) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS DATE)) 
                    FROM receiptmissing
                )
                ORDER BY rm.rm_id DESC";

            using var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection"));
            using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@userId", userId);
            
            await connection.OpenAsync();
            using var reader = await command.ExecuteReaderAsync();
            
            var receipts = new List<MissingReceiptDashboardDto>();
            
            while (await reader.ReadAsync())
            {
                receipts.Add(new MissingReceiptDashboardDto
                {
                    UId = ConvertToInt(reader["u_id"]),
                    RmId = ConvertToInt(reader["rm_id"]),
                    RmDateUpload = reader["rm_dateupload"] as DateTime?,
                    RmDateReceipt = reader["rm_datereceipt"] as DateTime?,
                    RmDescription = reader["rm_description"]?.ToString(),
                    RmAmount = reader["rm_amount"] as decimal?,
                    UFirstName = reader["u_firstname"]?.ToString(),
                    ULastName = reader["u_lastname"]?.ToString(),
                    UEmployeeNumber = reader["u_employeenumber"]?.ToString()
                });
            }

            stopwatch.Stop();
            // await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            // {
            //     Name = "DataService",
            //     Description = "GetMissingReceiptsByUser",
            //     Detail = $"Retrieved {receipts.Count} missing receipts for user {userId}",
            //     ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
            //     MachineName = Environment.MachineName
            // });

            return receipts;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetMissingReceiptsByUser",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving missing receipts for user {UserId}", userId);
            throw;
        }
    }

    public async Task<int> UploadMissingReceiptsAsync(List<MissingReceiptUploadDto> receipts)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            using var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection"));
            await connection.OpenAsync();
            
            using var transaction = connection.BeginTransaction();
            
            try
            {
                // Delete existing records for today (Central Time) - simplified approach
                const string deleteSql = @"
                    DELETE FROM ReceiptMissing 
                    WHERE DATEDIFF(day, rm_dateupload, CAST(GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS DATE)) = 0";
                using var deleteCommand = new SqlCommand(deleteSql, connection, transaction);
                var deletedRows = await deleteCommand.ExecuteNonQueryAsync();
                
                // Insert new records with Central Time
                const string insertSql = @"
                    INSERT INTO ReceiptMissing (rm_dateupload, rm_datereceipt, rm_description, rm_amount, u_employeenumber)
                    VALUES (
                        CAST(GETDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS DATE), 
                        @RmDateReceipt, 
                        @RmDescription, 
                        @RmAmount, 
                        @UEmployeeNumber
                    )";

                int insertedCount = 0;
                foreach (var receipt in receipts)
                {
                    using var insertCommand = new SqlCommand(insertSql, connection, transaction);
                    insertCommand.Parameters.AddWithValue("@RmDateReceipt", receipt.RmDateReceipt);
                    insertCommand.Parameters.AddWithValue("@RmDescription", receipt.RmDescription ?? (object)DBNull.Value);
                    insertCommand.Parameters.AddWithValue("@RmAmount", receipt.RmAmount);
                    insertCommand.Parameters.AddWithValue("@UEmployeeNumber", receipt.UEmployeeNumber ?? (object)DBNull.Value);
                    
                    await insertCommand.ExecuteNonQueryAsync();
                    insertedCount++;
                }

                transaction.Commit();

                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "UploadMissingReceipts",
                    Detail = $"Uploaded {insertedCount} missing receipts",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return insertedCount;
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UploadMissingReceipts",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error uploading missing receipts");
            throw;
        }
    }

    public async Task<DataTable> GetWorkOrderSchedulingConflictsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                -- Query to identify potential scheduling conflicts based on geographic distance between consecutive work orders
                WITH WorkOrderData AS (
                    SELECT 
                        cc.cc_name, 
                        c.c_name, 
                        sr.sr_id, 
                        u.u_id, 
                        u.u_firstname, 
                        u.u_lastname,
                        sr.sr_insertdatetime, 
                        sr.sr_requestnumber, 
                        ss.ss_statussecondary, 
                        wo.wo_workordernumber, 
                        wo.wo_description, 
                        wo.wo_startdatetime, 
                        wo.wo_enddatetime,
                        l.l_location, 
                        a.a_address1, 
                        a.a_city, 
                        a.a_state, 
                        a.a_zip,
                        -- Create full address for distance calculation
                        RTRIM(LTRIM(ISNULL(a.a_address1, '') + ', ' + ISNULL(a.a_city, '') + ', ' + ISNULL(a.a_state, '') + ' ' + ISNULL(a.a_zip, ''))) as full_address,
                        -- Add row number for each user's work orders ordered by start time
                        ROW_NUMBER() OVER (PARTITION BY u.u_id ORDER BY wo.wo_startdatetime) as rn
                    FROM workorder wo
                    INNER JOIN servicerequest sr ON sr.sr_id = wo.sr_id
                    INNER JOIN location l ON sr.l_id = l.l_id
                    INNER JOIN address a ON l.a_id = a.a_id
                    INNER JOIN statussecondary ss ON wo.ss_id = ss.ss_id
                    INNER JOIN xrefcompanycallcenter xccc ON sr.xccc_id = xccc.xccc_id
                    INNER JOIN callcenter cc ON xccc.cc_id = cc.cc_id
                    INNER JOIN company c ON xccc.c_id = c.c_id
                    INNER JOIN xrefWorkOrderUser xwou ON wo.wo_id = xwou.wo_id
                    INNER JOIN [user] u ON xwou.u_id = u.u_id
                    INNER JOIN xrefUserRole xur ON u.u_id = xur.u_id
                    INNER JOIN role r ON xur.r_id = r.r_id
                    WHERE wo.wo_startdatetime >= GETDATE()
                    AND c.c_name NOT IN ('Metro Pipe Program Administration')
                    AND c.c_name NOT LIKE '%time off%'
                    AND wo.wo_description NOT LIKE '%Holiday%'
                    AND r.r_role = 'Technician'
                    AND wo.wo_enddatetime IS NOT NULL  -- Ensure we have end times
                ),
                ConsecutiveWorkOrders AS (
                    SELECT 
                        w1.u_id,
                        w1.u_firstname,
                        w1.u_lastname,
                        
                        -- Current work order details
                        w1.wo_workordernumber as current_wo,
                        w1.sr_id as current_sr_id,
                        w1.wo_startdatetime as current_start,
                        w1.wo_enddatetime as current_end,
                        w1.wo_description as current_description,
                        w1.full_address as current_address,
                        w1.l_location as current_location,
                        
                        -- Next work order details  
                        w2.wo_workordernumber as next_wo,
                        w2.sr_id as next_sr_id,
                        w2.wo_startdatetime as next_start,
                        w2.wo_enddatetime as next_end,
                        w2.wo_description as next_description,
                        w2.full_address as next_address,
                        w2.l_location as next_location,
                        
                        -- Time analysis
                        DATEDIFF(MINUTE, w1.wo_enddatetime, w2.wo_startdatetime) as travel_time_minutes,
                        
                        -- Basic distance estimation (you may want to replace this with actual geocoding)
                        -- This is a rough approximation - for production use, consider integrating with a mapping service
                        CASE 
                            WHEN w1.a_zip = w2.a_zip THEN 'SAME_ZIP'
                            WHEN w1.a_city = w2.a_city AND w1.a_state = w2.a_state THEN 'SAME_CITY'
                            WHEN w1.a_state = w2.a_state THEN 'SAME_STATE'
                            ELSE 'DIFFERENT_STATE'
                        END as geographic_proximity,
                        
                        -- Current work order company and call center details
                        w1.c_name as current_company,
                        w1.cc_name as current_call_center,
                        
                        -- Next work order company and call center details
                        w2.c_name as next_company,
                        w2.cc_name as next_call_center
                        
                    FROM WorkOrderData w1
                    INNER JOIN WorkOrderData w2 ON w1.u_id = w2.u_id AND w1.rn = w2.rn - 1
                    WHERE DATEDIFF(MINUTE, w1.wo_enddatetime, w2.wo_startdatetime) <= 120  -- Within 2 hours
                ),
                PotentialConflicts AS (
                    SELECT *,
                        -- Risk assessment based on time and distance
                        CASE 
                            WHEN travel_time_minutes < 0 THEN 'OVERLAPPING_ORDERS'
                            WHEN travel_time_minutes = 0 AND geographic_proximity NOT IN ('SAME_ZIP', 'SAME_CITY') THEN 'IMPOSSIBLE_CONSECUTIVE'
                            WHEN travel_time_minutes <= 15 AND geographic_proximity = 'DIFFERENT_STATE' THEN 'HIGH_RISK'
                            WHEN travel_time_minutes <= 30 AND geographic_proximity = 'SAME_STATE' THEN 'MEDIUM_RISK'
                            WHEN travel_time_minutes <= 30 AND geographic_proximity = 'SAME_CITY' THEN 'LOW_RISK'
                            WHEN travel_time_minutes <= 60 AND geographic_proximity NOT IN ('SAME_ZIP', 'SAME_CITY') THEN 'REVIEW_NEEDED'
                            ELSE 'PROBABLY_OK'
                        END as conflict_risk
                    FROM ConsecutiveWorkOrders
                )
                SELECT 
                    u_firstname + ' ' + u_lastname as technician_name,
                    conflict_risk,
                    travel_time_minutes,
                    geographic_proximity,
                    
                    -- Current work order
                    current_wo,
                    current_sr_id,
                    FORMAT(current_start AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'MM/dd/yyyy hh:mm tt') as current_start_formatted,
                    FORMAT(current_end AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'MM/dd/yyyy hh:mm tt') as current_end_formatted,
                    current_description,
                    current_address,
                    current_location,
                    
                    -- Next work order
                    next_wo,
                    next_sr_id,
                    FORMAT(next_start AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'MM/dd/yyyy hh:mm tt') as next_start_formatted,
                    FORMAT(next_end AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'MM/dd/yyyy hh:mm tt') as next_end_formatted,
                    next_description,
                    next_address,
                    next_location,
                    
                    -- Company and call center info
                    current_company,
                    current_call_center,
                    next_company,
                    next_call_center,
                    
                    -- Add indicator if switching companies/call centers
                    CASE 
                        WHEN current_company != next_company THEN 'COMPANY_SWITCH'
                        WHEN current_call_center != next_call_center THEN 'CALL_CENTER_SWITCH' 
                        ELSE 'SAME_ORGANIZATION'
                    END as organization_change
                    
                FROM PotentialConflicts
                WHERE conflict_risk IN ('OVERLAPPING_ORDERS', 'IMPOSSIBLE_CONSECUTIVE', 'HIGH_RISK', 'MEDIUM_RISK', 'REVIEW_NEEDED')
                ORDER BY 
                    CASE conflict_risk
                        WHEN 'OVERLAPPING_ORDERS' THEN 1
                        WHEN 'IMPOSSIBLE_CONSECUTIVE' THEN 2
                        WHEN 'HIGH_RISK' THEN 3
                        WHEN 'MEDIUM_RISK' THEN 4
                        WHEN 'REVIEW_NEEDED' THEN 5
                        ELSE 6
                    END,
                    u_id,
                    current_start;";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetWorkOrderSchedulingConflicts",
                Detail = $"Retrieved work order scheduling conflicts",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetWorkOrderSchedulingConflicts",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving work order scheduling conflicts");
            throw;
        }
    }

    public async Task<DataTable> GetWorkOrderSchedulingConflictsSummaryAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                -- Additional query to get summary statistics
                WITH WorkOrderSummary AS (
                    SELECT 
                        u.u_id, 
                        wo.wo_workordernumber, 
                        wo.wo_startdatetime, 
                        wo.wo_enddatetime,
                        a.a_city, a.a_state, a.a_zip,
                        ROW_NUMBER() OVER (PARTITION BY u.u_id ORDER BY wo.wo_startdatetime) as rn
                    FROM workorder wo
                    INNER JOIN servicerequest sr ON sr.sr_id = wo.sr_id
                    INNER JOIN location l ON sr.l_id = l.l_id
                    INNER JOIN address a ON l.a_id = a.a_id
                    INNER JOIN xrefcompanycallcenter xccc ON sr.xccc_id = xccc.xccc_id
                    INNER JOIN company c ON xccc.c_id = c.c_id
                    INNER JOIN xrefWorkOrderUser xwou ON wo.wo_id = xwou.wo_id
                    INNER JOIN [user] u ON xwou.u_id = u.u_id
                    INNER JOIN xrefUserRole xur ON u.u_id = xur.u_id
                    INNER JOIN role r ON xur.r_id = r.r_id
                    WHERE wo.wo_startdatetime >= GETDATE()
                    AND c.c_name NOT IN ('Metro Pipe Program Administration')
                    AND c.c_name NOT LIKE '%time off%'
                    AND wo.wo_description NOT LIKE '%Holiday%'
                    AND r.r_role = 'Technician'
                    AND wo.wo_enddatetime IS NOT NULL
                ),
                ConflictSummary AS (
                    SELECT 
                        DATEDIFF(MINUTE, w1.wo_enddatetime, w2.wo_startdatetime) as travel_time_minutes,
                        CASE 
                            WHEN w1.a_zip = w2.a_zip THEN 'SAME_ZIP'
                            WHEN w1.a_city = w2.a_city AND w1.a_state = w2.a_state THEN 'SAME_CITY'
                            WHEN w1.a_state = w2.a_state THEN 'SAME_STATE'
                            ELSE 'DIFFERENT_STATE'
                        END as geographic_proximity,
                        CASE 
                            WHEN DATEDIFF(MINUTE, w1.wo_enddatetime, w2.wo_startdatetime) < 0 THEN 'OVERLAPPING_ORDERS'
                            WHEN DATEDIFF(MINUTE, w1.wo_enddatetime, w2.wo_startdatetime) = 0 AND w1.a_zip != w2.a_zip THEN 'IMPOSSIBLE_CONSECUTIVE'
                            WHEN DATEDIFF(MINUTE, w1.wo_enddatetime, w2.wo_startdatetime) <= 15 AND w1.a_state != w2.a_state THEN 'HIGH_RISK'
                            WHEN DATEDIFF(MINUTE, w1.wo_enddatetime, w2.wo_startdatetime) <= 30 AND w1.a_state = w2.a_state AND w1.a_city != w2.a_city THEN 'MEDIUM_RISK'
                            WHEN DATEDIFF(MINUTE, w1.wo_enddatetime, w2.wo_startdatetime) <= 60 AND w1.a_zip != w2.a_zip THEN 'REVIEW_NEEDED'
                            ELSE 'PROBABLY_OK'
                        END as conflict_risk
                    FROM WorkOrderSummary w1
                    INNER JOIN WorkOrderSummary w2 ON w1.u_id = w2.u_id AND w1.rn = w2.rn - 1
                    WHERE DATEDIFF(MINUTE, w1.wo_enddatetime, w2.wo_startdatetime) <= 120
                )
                SELECT 
                    conflict_risk,
                    COUNT(*) as conflict_count,
                    AVG(CAST(travel_time_minutes as FLOAT)) as avg_travel_time,
                    MIN(travel_time_minutes) as min_travel_time,
                    MAX(travel_time_minutes) as max_travel_time
                FROM ConflictSummary
                GROUP BY conflict_risk
                ORDER BY 
                    CASE conflict_risk
                        WHEN 'OVERLAPPING_ORDERS' THEN 1
                        WHEN 'IMPOSSIBLE_CONSECUTIVE' THEN 2
                        WHEN 'HIGH_RISK' THEN 3
                        WHEN 'MEDIUM_RISK' THEN 4
                        WHEN 'REVIEW_NEEDED' THEN 5
                        ELSE 6
                    END;";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetWorkOrderSchedulingConflictsSummary",
                Detail = $"Retrieved work order scheduling conflicts summary",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetWorkOrderSchedulingConflictsSummary",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving work order scheduling conflicts summary");
            throw;
        }
    }

    public async Task<DataTable> GetTimecardDiscrepanciesAsync(DateTime startDate, DateTime endDate)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    ttd.ttd_id,
                    ttd.u_id,
                    u.u_firstname + ' ' + u.u_lastname as technician_name,
                    u.u_employeenumber,
                    ttd.ttt_id,
                    ttt.ttt_timetype as tracking_type,
                    ttd.wo_id,
                    sr.sr_id,
                    sr.sr_requestnumber as work_order_number,
                    ttd.ttd_insertdatetime,
                    ttd.ttd_lat_browser,
                    ttd.ttd_lon_browser,
                    ttd.ttd_lat_fleetmatics,
                    ttd.ttd_lon_fleetmatics,
                    ttd.ttd_type,
                    ttd.wo_startdatetime,
                    ttd.wo_enddatetime,
                    ttd.ttd_distanceinmilesbrowser,
                    ttd.ttd_distanceinmilesfleetmatics,
                    ttd.ttd_traveltimeinminutesbrowser,
                    ttd.ttd_traveltimeinminutesfleetmatics,
                    c.c_name as company_name,
                    cc.cc_name as call_center_name,
                    l.l_location as location_name,
                    a.a_address1 + ', ' + a.a_city + ', ' + a.a_state + ' ' + a.a_zip as work_order_address
                FROM timetrackingdetail ttd
                INNER JOIN [user] u ON ttd.u_id = u.u_id
                INNER JOIN timetrackingtype ttt ON ttd.ttt_id = ttt.ttt_id
                LEFT JOIN workorder wo ON ttd.wo_id = wo.wo_id
                LEFT JOIN servicerequest sr ON wo.sr_id = sr.sr_id
                LEFT JOIN location l ON sr.l_id = l.l_id
                LEFT JOIN address a ON l.a_id = a.a_id
                LEFT JOIN xrefcompanycallcenter xccc ON sr.xccc_id = xccc.xccc_id
                LEFT JOIN company c ON xccc.c_id = c.c_id
                LEFT JOIN callcenter cc ON xccc.cc_id = cc.cc_id
                WHERE ttd.ttd_insertdatetime >= @StartDate
                  AND ttd.ttd_insertdatetime <= @EndDate
                ORDER BY ttd.ttd_insertdatetime DESC";

            var parameters = new Dictionary<string, object>
            {
                { "@StartDate", startDate },
                { "@EndDate", endDate }
            };

            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTimecardDiscrepancies",
                Detail = $"Retrieved {result.Rows.Count} timecard discrepancy records from {startDate:yyyy-MM-dd} to {endDate:yyyy-MM-dd}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTimecardDiscrepancies",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving timecard discrepancies");
            throw;
        }
    }

    public async Task<DataTable> GetArrivingLateReportAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                WITH Upcoming AS (
                    SELECT
                        cc.cc_name,
                        t.t_trade,
                        wo.wo_workordernumber,
                        sr.sr_id,
                        u.u_id,
                        u.u_employeenumber,
                        u.u_firstname,
                        u.u_lastname,
                        u.u_vehiclenumber,
                        wo.wo_startdatetime,
                        a.a_address1,
                        a.a_city,
                        a.a_state,
                        a.a_zip,
                        ROW_NUMBER() OVER (
                            PARTITION BY u.u_id
                            ORDER BY wo.wo_startdatetime ASC
                        ) AS rn
                    FROM workorder          wo
                    JOIN servicerequest     sr  ON sr.sr_id = wo.sr_id
                    JOIN location           l   ON l.l_id = sr.l_id
                    JOIN address            a   ON a.a_id = l.a_id
                    JOIN xrefWorkOrderUser  xwou ON xwou.wo_id = wo.wo_id
                    JOIN [user]             u   ON u.u_id = xwou.u_id
                    JOIN xrefCompanyCallCenter xccc on sr.xccc_id = xccc.xccc_id
                    JOIN CallCenter         cc  ON xccc.cc_id = cc.cc_id
                    JOIN Trade              t   ON sr.t_id = t.t_id
                    WHERE wo.wo_startdatetime > GETDATE()
                      AND wo.wo_startdatetime <= DATEADD(HOUR, 8, GETDATE())
                      AND cc.cc_name not in ('Administrative', 'Administrative - Automotive')
                      AND u.u_vehiclenumber not in ('', 'NULL')
                      AND u.u_vehiclenumber IS NOT NULL
                )
                SELECT
                    cc_name,
                    t_trade,
                    wo_workordernumber,
                    sr_id,
                    u_id,
                    u_employeenumber,
                    u_firstname,
                    u_lastname,
                    u_vehiclenumber,
                    FORMAT(wo_startdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 'yyyy-MM-dd HH:mm') AS wo_startdatetime,
                    a_address1,
                    a_city,
                    a_state,
                    a_zip
                FROM Upcoming
                WHERE rn = 1
                ORDER BY 1;";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetArrivingLateReport",
                Detail = $"Retrieved {result.Rows.Count} arriving late report records",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetArrivingLateReport",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving arriving late report");
            throw;
        }
    }

    public async Task<DataTable> GetAttachmentsByServiceRequestAsync(int srId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    att_id,
                    att_insertdatetime,
                    att_filename,
                    att_description,
                    att_active,
                    att_receipt,
                    att_public,
                    att_signoff,
                    att_submittedby,
                    att_receiptamount,
                    sr_id
                FROM attachment 
                WHERE sr_id = @sr_id
                ORDER BY att_insertdatetime DESC";

            var parameters = new Dictionary<string, object>
            {
                ["@sr_id"] = srId
            };

            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAttachmentsByServiceRequest",
                Detail = $"Retrieved {result.Rows.Count} attachments for service request {srId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAttachmentsByServiceRequest",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving attachments for service request {SrId}", srId);
            throw;
        }
    }

    public async Task<DataTable> GetPendingTechInfoAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    wo.sr_id, 
                    xwou.xwou_id, 
                    sr.sr_requestnumber, 
                    u.u_firstname, 
                    u.u_lastname, 
                    wo.wo_insertdatetime, 
                    t.t_trade, 
                    c.c_name, 
                    wo.wo_startdatetime
                FROM servicerequest sr, 
                     workorder wo, 
                     statussecondary ss, 
                     xrefworkorderuser xwou, 
                     [user] u, 
                     trade t, 
                     xrefcompanycallcenter xccc, 
                     company c
                WHERE ss.ss_statussecondary LIKE 'Pending Tech Info%'
                AND wo.sr_id = sr.sr_id
                AND sr.xccc_id = xccc.xccc_id
                AND xccc.c_id = c.c_id
                AND wo.ss_id = ss.ss_id
                AND wo.wo_id = xwou.wo_id
                AND xwou.u_id = u.u_id
                AND sr.t_id = t.t_id
                AND xwou.u_id = @u_id 
                ORDER BY wo.wo_insertdatetime";

            var parameters = new Dictionary<string, object>
            {
                ["@u_id"] = userId
            };

            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetPendingTechInfo",
                Detail = $"Retrieved {result.Rows.Count} pending tech info records for user {userId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetPendingTechInfo",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving pending tech info for user {UserId}", userId);
            throw;
        }
    }

    public async Task<MapDistanceDto?> GetCachedDistanceAsync(string fromAddress, string toAddress)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // Normalize addresses for consistent caching
            var normalizedFrom = fromAddress.Trim().ToLowerInvariant();
            var normalizedTo = toAddress.Trim().ToLowerInvariant();

            const string sql = @"
                SELECT TOP 1
                    md_id,
                    md_address1,
                    md_address2,
                    md_distance_miles,
                    md_distance_text,
                    md_traveltime_minutes,
                    md_traveltime_text,
                    md_traveltime_traffic_minutes,
                    md_traveltime_traffic_text,
                    md_insertdatetime,
                    md_modifieddatetime
                FROM MapDistance
                WHERE LOWER(LTRIM(RTRIM(md_address1))) = @fromAddress
                AND LOWER(LTRIM(RTRIM(md_address2))) = @toAddress
                ORDER BY ISNULL(md_modifieddatetime, md_insertdatetime) DESC";

            var parameters = new Dictionary<string, object>
            {
                { "@fromAddress", normalizedFrom },
                { "@toAddress", normalizedTo }
            };

            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            // await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            // {
            //     Name = "DataService",
            //     Description = "GetCachedDistance",
            //     Detail = $"Retrieved cached distance from '{fromAddress}' to '{toAddress}', found: {result.Rows.Count > 0}",
            //     ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
            //     MachineName = Environment.MachineName
            // });

            if (result.Rows.Count == 0)
                return null;

            var row = result.Rows[0];
            return new MapDistanceDto
            {
                md_id = ConvertToInt(row["md_id"]),
                md_address1 = row["md_address1"]?.ToString() ?? string.Empty,
                md_address2 = row["md_address2"]?.ToString() ?? string.Empty,
                md_distance_miles = row["md_distance_miles"] != DBNull.Value ? Convert.ToDecimal(row["md_distance_miles"]) : null,
                md_distance_meters = null, // Not available in database
                md_distance_text = row["md_distance_text"]?.ToString(),
                md_traveltime_minutes = row["md_traveltime_minutes"] != DBNull.Value ? ConvertToInt(row["md_traveltime_minutes"]) : null,
                md_traveltime_seconds = null, // Not available in database  
                md_traveltime_text = row["md_traveltime_text"]?.ToString(),
                md_traveltime_traffic_minutes = row["md_traveltime_traffic_minutes"] != DBNull.Value ? ConvertToInt(row["md_traveltime_traffic_minutes"]) : null,
                md_traveltime_traffic_seconds = null, // Not available in database
                md_traveltime_traffic_text = row["md_traveltime_traffic_text"]?.ToString(),
                md_created_date = row["md_insertdatetime"] != DBNull.Value ? Convert.ToDateTime(row["md_insertdatetime"]) : DateTime.MinValue,
                md_last_updated = row["md_modifieddatetime"] != DBNull.Value ? Convert.ToDateTime(row["md_modifieddatetime"]) : 
                                 (row["md_insertdatetime"] != DBNull.Value ? Convert.ToDateTime(row["md_insertdatetime"]) : DateTime.MinValue)
            };
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCachedDistance",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving cached distance from '{FromAddress}' to '{ToAddress}'", fromAddress, toAddress);
            throw;
        }
    }

    public async Task<int> SaveCachedDistanceAsync(SaveMapDistanceRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // Normalize addresses for consistent caching
            var normalizedFrom = request.FromAddress.Trim().ToLowerInvariant();
            var normalizedTo = request.ToAddress.Trim().ToLowerInvariant();

            // Check if entry already exists
            const string checkSql = @"
                SELECT md_id 
                FROM MapDistance
                WHERE LOWER(LTRIM(RTRIM(md_address1))) = @fromAddress
                AND LOWER(LTRIM(RTRIM(md_address2))) = @toAddress";

            var checkParams = new Dictionary<string, object>
            {
                { "@fromAddress", normalizedFrom },
                { "@toAddress", normalizedTo }
            };

            var existingResult = await ExecuteQueryAsync(checkSql, checkParams);

            string sql;
            Dictionary<string, object> parameters;

            if (existingResult.Rows.Count > 0)
            {
                // Update existing record
                var existingId = ConvertToInt(existingResult.Rows[0]["md_id"]);
                
                sql = @"
                    UPDATE MapDistance 
                    SET 
                        md_distance_miles = @distanceMiles,
                        md_distance_text = @distanceText,
                        md_traveltime_minutes = @travelTimeMinutes,
                        md_traveltime_text = @travelTimeText,
                        md_traveltime_traffic_minutes = @travelTimeTrafficMinutes,
                        md_traveltime_traffic_text = @travelTimeTrafficText,
                        md_modifieddatetime = GETUTCDATE()
                    WHERE md_id = @id";

                parameters = new Dictionary<string, object>
                {
                    { "@id", existingId },
                    { "@distanceMiles", (object?)request.DistanceMiles ?? DBNull.Value },
                    { "@distanceText", (object?)request.DistanceText ?? DBNull.Value },
                    { "@travelTimeMinutes", (object?)request.TravelTimeMinutes ?? DBNull.Value },
                    { "@travelTimeText", (object?)request.TravelTimeText ?? DBNull.Value },
                    { "@travelTimeTrafficMinutes", (object?)request.TravelTimeTrafficMinutes ?? DBNull.Value },
                    { "@travelTimeTrafficText", (object?)request.TravelTimeTrafficText ?? DBNull.Value }
                };

                await ExecuteNonQueryAsync(sql, parameters);
                
                stopwatch.Stop();
                // await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                // {
                //     Name = "DataService",
                //     Description = "SaveCachedDistance",
                //     Detail = $"Updated cached distance from '{request.FromAddress}' to '{request.ToAddress}'",
                //     ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                //     MachineName = Environment.MachineName
                // });

                return existingId;
            }
            else
            {
                // Insert new record
                sql = @"
                    INSERT INTO MapDistance (
                        md_address1, md_address2, md_distance_miles, md_distance_text,
                        md_traveltime_minutes, md_traveltime_text,
                        md_traveltime_traffic_minutes, md_traveltime_traffic_text,
                        md_insertdatetime
                    ) 
                    OUTPUT INSERTED.md_id
                    VALUES (
                        @fromAddress, @toAddress, @distanceMiles, @distanceText,
                        @travelTimeMinutes, @travelTimeText,
                        @travelTimeTrafficMinutes, @travelTimeTrafficText,
                        GETUTCDATE()
                    )";

                parameters = new Dictionary<string, object>
                {
                    { "@fromAddress", request.FromAddress.Trim() },
                    { "@toAddress", request.ToAddress.Trim() },
                    { "@distanceMiles", (object?)request.DistanceMiles ?? DBNull.Value },
                    { "@distanceText", (object?)request.DistanceText ?? DBNull.Value },
                    { "@travelTimeMinutes", (object?)request.TravelTimeMinutes ?? DBNull.Value },
                    { "@travelTimeText", (object?)request.TravelTimeText ?? DBNull.Value },
                    { "@travelTimeTrafficMinutes", (object?)request.TravelTimeTrafficMinutes ?? DBNull.Value },
                    { "@travelTimeTrafficText", (object?)request.TravelTimeTrafficText ?? DBNull.Value }
                };

                var insertResult = await ExecuteQueryAsync(sql, parameters);
                var newId = ConvertToInt(insertResult.Rows[0][0]);
                
                stopwatch.Stop();
                // await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                // {
                //     Name = "DataService",
                //     Description = "SaveCachedDistance",
                //     Detail = $"Inserted new cached distance from '{request.FromAddress}' to '{request.ToAddress}'",
                //     ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                //     MachineName = Environment.MachineName
                // });

                return newId;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "SaveCachedDistance",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error saving cached distance from '{FromAddress}' to '{ToAddress}'", request.FromAddress, request.ToAddress);
            throw;
        }
    }

    public async Task<int> CleanupCachedDistanceAsync(int olderThanDays)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                DELETE FROM MapDistance 
                WHERE md_created_date < DATEADD(DAY, -@olderThanDays, GETUTCDATE())";

            var parameters = new Dictionary<string, object>
            {
                { "@olderThanDays", olderThanDays }
            };

            var deletedCount = await ExecuteNonQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CleanupCachedDistance",
                Detail = $"Deleted {deletedCount} cached distance entries older than {olderThanDays} days",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return deletedCount;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CleanupCachedDistance",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error cleaning up cached distance data older than {OlderThanDays} days", olderThanDays);
            throw;
        }
    }

    public async Task<DrivingScorecard> GetDrivingScorecardAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var sql = @"
                SELECT 
                    u.u_id,
                    SUM(CASE WHEN ag.ag_name = 'Speeding over 10' THEN 1 ELSE 0 END) AS SpeedingOver10,
                    SUM(CASE WHEN ag.ag_name = 'Speeding over 20' THEN 1 ELSE 0 END) AS SpeedingOver20,
                    SUM(CASE WHEN ag.ag_name = 'Hard Breaking' THEN 1 ELSE 0 END) AS HardBreaking,
                    SUM(CASE WHEN ag.ag_name = 'Hard Breaking Severe' THEN 1 ELSE 0 END) AS HardBreakingSevere,
                    SUM(CASE WHEN ag.ag_name = 'Hard Accelerating Severe' THEN 1 ELSE 0 END) AS HardAcceleratingSevere,
                    SUM(CASE WHEN ag.ag_name = 'Harsh Cornering Severe' THEN 1 ELSE 0 END) AS HarshCorneringSevere
                FROM alertgps ag
                JOIN [user] u ON u.u_employeenumber = ag.u_employeenumber
                WHERE ag.ag_name NOT IN ('Tech Home', 'Driver Home')
                  AND ag.ag_insertdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' 
                        >= DATEADD(DAY, -7, GETDATE())
                  AND u.u_id = @userId
                GROUP BY u.u_id
            ";

            var parameters = new Dictionary<string, object>
            {
                { "@userId", userId }
            };

            var result = await ExecuteQueryAsync(sql, parameters);
            
            var drivingScorecard = new DrivingScorecard
            {
                UserId = userId,
                SpeedingOver10 = 0,
                SpeedingOver20 = 0,
                HardBreaking = 0,
                HardBreakingSevere = 0,
                HardAcceleratingSevere = 0,
                HarshCorneringSevere = 0
            };

            if (result.Rows.Count > 0)
            {
                var row = result.Rows[0];
                drivingScorecard.UserId = ConvertToInt(row["u_id"]);
                drivingScorecard.SpeedingOver10 = ConvertToInt(row["SpeedingOver10"]);
                drivingScorecard.SpeedingOver20 = ConvertToInt(row["SpeedingOver20"]);
                drivingScorecard.HardBreaking = ConvertToInt(row["HardBreaking"]);
                drivingScorecard.HardBreakingSevere = ConvertToInt(row["HardBreakingSevere"]);
                drivingScorecard.HardAcceleratingSevere = ConvertToInt(row["HardAcceleratingSevere"]);
                drivingScorecard.HarshCorneringSevere = ConvertToInt(row["HarshCorneringSevere"]);
            }
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetDrivingScorecard",
                Detail = $"Retrieved driving scorecard for user {userId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return drivingScorecard;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetDrivingScorecard",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving driving scorecard for user {UserId}", userId);
            throw;
        }
    }

    public async Task<List<DrivingScorecardWithTechnicianInfo>> GetAllDrivingScorecardsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var sql = @"
                SELECT 
                    u.u_id,
                    u.u_firstname,
                    u.u_lastname,
                    u.u_employeenumber,
                    COALESCE(violations.SpeedingOver10, 0) AS SpeedingOver10,
                    COALESCE(violations.SpeedingOver20, 0) AS SpeedingOver20,
                    COALESCE(violations.HardBreaking, 0) AS HardBreaking,
                    COALESCE(violations.HardBreakingSevere, 0) AS HardBreakingSevere,
                    COALESCE(violations.HardAcceleratingSevere, 0) AS HardAcceleratingSevere,
                    COALESCE(violations.HarshCorneringSevere, 0) AS HarshCorneringSevere
                FROM [user] u
                INNER JOIN xrefUserRole x ON u.u_id = x.u_id
                INNER JOIN role r ON r.r_id = x.r_id
                LEFT JOIN (
                    SELECT 
                        u.u_id,
                        SUM(CASE WHEN ag.ag_name = 'Speeding over 10' THEN 1 ELSE 0 END) AS SpeedingOver10,
                        SUM(CASE WHEN ag.ag_name = 'Speeding over 20' THEN 1 ELSE 0 END) AS SpeedingOver20,
                        SUM(CASE WHEN ag.ag_name = 'Hard Breaking' THEN 1 ELSE 0 END) AS HardBreaking,
                        SUM(CASE WHEN ag.ag_name = 'Hard Breaking Severe' THEN 1 ELSE 0 END) AS HardBreakingSevere,
                        SUM(CASE WHEN ag.ag_name = 'Hard Accelerating Severe' THEN 1 ELSE 0 END) AS HardAcceleratingSevere,
                        SUM(CASE WHEN ag.ag_name = 'Harsh Cornering Severe' THEN 1 ELSE 0 END) AS HarshCorneringSevere
                    FROM alertgps ag
                    JOIN [user] u ON u.u_employeenumber = ag.u_employeenumber
                    WHERE ag.ag_name NOT IN ('Tech Home', 'Driver Home')
                      AND ag.ag_insertdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' 
                            >= DATEADD(DAY, -7, GETDATE())
                    GROUP BY u.u_id
                ) violations ON u.u_id = violations.u_id
                WHERE r.r_role = 'Technician' 
                    AND u.u_active = 1 
                ORDER BY u.u_lastname, u.u_firstname
            ";

            var result = await ExecuteQueryAsync(sql);
            var scorecards = new List<DrivingScorecardWithTechnicianInfo>();

            foreach (DataRow row in result.Rows)
            {
                var scorecard = new DrivingScorecardWithTechnicianInfo
                {
                    UserId = ConvertToInt(row["u_id"]),
                    FirstName = row["u_firstname"]?.ToString() ?? "",
                    LastName = row["u_lastname"]?.ToString() ?? "",
                    EmployeeNumber = row["u_employeenumber"]?.ToString() ?? "",
                    SpeedingOver10 = ConvertToInt(row["SpeedingOver10"]),
                    SpeedingOver20 = ConvertToInt(row["SpeedingOver20"]),
                    HardBreaking = ConvertToInt(row["HardBreaking"]),
                    HardBreakingSevere = ConvertToInt(row["HardBreakingSevere"]),
                    HardAcceleratingSevere = ConvertToInt(row["HardAcceleratingSevere"]),
                    HarshCorneringSevere = ConvertToInt(row["HarshCorneringSevere"])
                };
                
                scorecards.Add(scorecard);
            }
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllDrivingScorecard",
                Detail = $"Retrieved driving scorecards for all technicians. Count: {scorecards.Count}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            return scorecards;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllDrivingScorecard",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving all driving scorecards");
            throw;
        }
    }

    private static int ConvertToInt(object value)
    {
        if (value == null || value == DBNull.Value)
            return 0;
        
        if (int.TryParse(value.ToString(), out var result))
            return result;
            
        return 0;
    }

    public async Task<List<UserFleetmaticsDto>> GetUsersForFleetmaticsSyncAsync()
    {
        var stopwatch = Stopwatch.StartNew();
        var users = new List<UserFleetmaticsDto>();

        try
        {
            _logger.LogDebug("Getting users for Fleetmatics sync");

            var sql = @"
                SELECT u_id, u_username, u_firstname, u_lastname, u_employeenumber, u_vehiclenumber, u_active
                FROM [user] 
                WHERE u_active = 1 
                AND u_employeenumber IS NOT NULL 
                AND u_employeenumber != ''
                AND LEN(TRIM(u_employeenumber)) > 0
                ORDER BY u_employeenumber";

            var result = await ExecuteQueryAsync(sql);

            foreach (DataRow row in result.Rows)
            {
                var user = new UserFleetmaticsDto
                {
                    UserId = ConvertToInt(row["u_id"]),
                    Username = row["u_username"]?.ToString() ?? "",
                    FirstName = row["u_firstname"]?.ToString() ?? "",
                    LastName = row["u_lastname"]?.ToString() ?? "",
                    EmployeeNumber = row["u_employeenumber"]?.ToString() ?? "",
                    CurrentVehicleNumber = row["u_vehiclenumber"]?.ToString(),
                    IsActive = Convert.ToBoolean(row["u_active"])
                };

                users.Add(user);
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetUsersForFleetmaticsSync",
                Detail = $"Retrieved {users.Count} users eligible for Fleetmatics sync",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return users;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetUsersForFleetmaticsSync",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            _logger.LogError(ex, "Error retrieving users for Fleetmatics sync");
            throw;
        }
    }

    public async Task<bool> UpdateUserVehicleNumberAsync(int userId, string vehicleNumber)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            _logger.LogDebug("Updating vehicle number for user {UserId}: {VehicleNumber}", userId, vehicleNumber);

            var sql = @"
                UPDATE [user] 
                SET u_vehiclenumber = @VehicleNumber,
                    u_lastmodified = GETUTCDATE()
                WHERE u_id = @UserId";

            var parameters = new Dictionary<string, object>
            {
                { "@UserId", userId },
                { "@VehicleNumber", vehicleNumber }
            };

            var result = await ExecuteQueryAsync(sql, parameters);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateUserVehicleNumber",
                Detail = $"Updated vehicle number for user {userId}: {vehicleNumber}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return true;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateUserVehicleNumber",
                Detail = $"Error updating vehicle number for user {userId}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            _logger.LogError(ex, "Error updating vehicle number for user {UserId}", userId);
            throw;
        }
    }

    public async Task<List<VehicleMaintenanceDto>> GetVehicleMaintenanceRecordsAsync()
    {
        var stopwatch = Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("Database connection string is not configured");
        }

        try
        {
            var results = new List<VehicleMaintenanceDto>();

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            const string sql = @"
                SELECT 
                    vd_id, vd_insertdatetime, vd_modifieddatetime, vd_dateupload,
                    vd_maintproduct, vd_monthsoncurrentservice, vd_custname, vd_driver,
                    vd_vin, vd_maintcostcode, vd_customervehicleid, vd_year, vd_make,
                    vd_model, vd_series, vd_vehicle, vd_openrecall, vd_oilchangedate,
                    vd_oilchangemileage, vd_estmileagesinceoilchange, vd_contractedbrakesets,
                    vd_availablebrakesets, vd_brakereplacementdate, vd_frontrearboth,
                    vd_brakereplacementmileage, vd_estmileagesincebrakereplacement,
                    vd_contractedtires, vd_availabletires, vd_tirereplacementdate,
                    vd_tirereplacementmileage, vd_estmileagesincetirereplacement,
                    vd_estimatedcurrentmileage, u_employeenumber
                FROM VehicleDetail 
                WHERE CAST(vd_dateupload AS DATE) = (
                    SELECT MAX(CAST(vd_dateupload AS DATE)) 
                    FROM VehicleDetail 
                    WHERE vd_dateupload IS NOT NULL
                )
                ORDER BY vd_insertdatetime DESC";

            using var command = new SqlCommand(sql, connection);

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                results.Add(new VehicleMaintenanceDto
                {
                    vd_id = reader.GetInt32("vd_id"),
                    vd_insertdatetime = reader.GetDateTime("vd_insertdatetime"),
                    vd_modifieddatetime = reader.IsDBNull("vd_modifieddatetime") ? null : reader.GetDateTime("vd_modifieddatetime"),
                    vd_dateupload = reader.IsDBNull("vd_dateupload") ? null : reader.GetDateTime("vd_dateupload"),
                    vd_maintproduct = reader.IsDBNull("vd_maintproduct") ? null : reader.GetString("vd_maintproduct"),
                    vd_monthsoncurrentservice = reader.IsDBNull("vd_monthsoncurrentservice") ? null : reader.GetInt32("vd_monthsoncurrentservice"),
                    vd_custname = reader.IsDBNull("vd_custname") ? null : reader.GetString("vd_custname"),
                    vd_driver = reader.IsDBNull("vd_driver") ? null : reader.GetString("vd_driver"),
                    vd_vin = reader.IsDBNull("vd_vin") ? null : reader.GetString("vd_vin"),
                    vd_maintcostcode = reader.IsDBNull("vd_maintcostcode") ? null : reader.GetString("vd_maintcostcode"),
                    vd_customervehicleid = reader.IsDBNull("vd_customervehicleid") ? null : reader.GetString("vd_customervehicleid"),
                    vd_year = reader.IsDBNull("vd_year") ? null : reader.GetInt32("vd_year"),
                    vd_make = reader.IsDBNull("vd_make") ? null : reader.GetString("vd_make"),
                    vd_model = reader.IsDBNull("vd_model") ? null : reader.GetString("vd_model"),
                    vd_series = reader.IsDBNull("vd_series") ? null : reader.GetString("vd_series"),
                    vd_vehicle = reader.IsDBNull("vd_vehicle") ? null : reader.GetString("vd_vehicle"),
                    vd_openrecall = reader.IsDBNull("vd_openrecall") ? null : reader.GetString("vd_openrecall"),
                    vd_oilchangedate = reader.IsDBNull("vd_oilchangedate") ? null : reader.GetDateTime("vd_oilchangedate"),
                    vd_oilchangemileage = reader.IsDBNull("vd_oilchangemileage") ? null : reader.GetInt32("vd_oilchangemileage"),
                    vd_estmileagesinceoilchange = reader.IsDBNull("vd_estmileagesinceoilchange") ? null : reader.GetInt32("vd_estmileagesinceoilchange"),
                    vd_contractedbrakesets = reader.IsDBNull("vd_contractedbrakesets") ? null : reader.GetInt32("vd_contractedbrakesets"),
                    vd_availablebrakesets = reader.IsDBNull("vd_availablebrakesets") ? null : reader.GetInt32("vd_availablebrakesets"),
                    vd_brakereplacementdate = reader.IsDBNull("vd_brakereplacementdate") ? null : reader.GetDateTime("vd_brakereplacementdate"),
                    vd_frontrearboth = reader.IsDBNull("vd_frontrearboth") ? null : reader.GetString("vd_frontrearboth"),
                    vd_brakereplacementmileage = reader.IsDBNull("vd_brakereplacementmileage") ? null : reader.GetInt32("vd_brakereplacementmileage"),
                    vd_estmileagesincebrakereplacement = reader.IsDBNull("vd_estmileagesincebrakereplacement") ? null : reader.GetInt32("vd_estmileagesincebrakereplacement"),
                    vd_contractedtires = reader.IsDBNull("vd_contractedtires") ? null : reader.GetInt32("vd_contractedtires"),
                    vd_availabletires = reader.IsDBNull("vd_availabletires") ? null : reader.GetInt32("vd_availabletires"),
                    vd_tirereplacementdate = reader.IsDBNull("vd_tirereplacementdate") ? null : reader.GetDateTime("vd_tirereplacementdate"),
                    vd_tirereplacementmileage = reader.IsDBNull("vd_tirereplacementmileage") ? null : reader.GetInt32("vd_tirereplacementmileage"),
                    vd_estmileagesincetirereplacement = reader.IsDBNull("vd_estmileagesincetirereplacement") ? null : reader.GetInt32("vd_estmileagesincetirereplacement"),
                    vd_estimatedcurrentmileage = reader.IsDBNull("vd_estimatedcurrentmileage") ? null : reader.GetInt32("vd_estimatedcurrentmileage"),
                    u_employeenumber = reader.IsDBNull("u_employeenumber") ? null : reader.GetString("u_employeenumber")
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetVehicleMaintenanceRecords",
                Detail = $"Retrieved {results.Count} vehicle maintenance records",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return results;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error retrieving vehicle maintenance records");
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetVehicleMaintenanceRecords",
                Detail = $"Error retrieving vehicle maintenance records: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<VehicleMaintenanceDto?> GetVehicleMaintenanceByEmployeeNumberAsync(string employeeNumber)
    {
        var stopwatch = Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("Database connection string is not configured");
        }

        try
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            const string sql = @"
                SELECT 
                    vd_id, vd_insertdatetime, vd_modifieddatetime, vd_dateupload,
                    vd_maintproduct, vd_monthsoncurrentservice, vd_custname, vd_driver,
                    vd_vin, vd_maintcostcode, vd_customervehicleid, vd_year, vd_make,
                    vd_model, vd_series, vd_vehicle, vd_openrecall, vd_oilchangedate,
                    vd_oilchangemileage, vd_estmileagesinceoilchange, vd_contractedbrakesets,
                    vd_availablebrakesets, vd_brakereplacementdate, vd_frontrearboth,
                    vd_brakereplacementmileage, vd_estmileagesincebrakereplacement,
                    vd_contractedtires, vd_availabletires, vd_tirereplacementdate,
                    vd_tirereplacementmileage, vd_estmileagesincetirereplacement,
                    vd_estimatedcurrentmileage, u_employeenumber
                FROM VehicleDetail 
                WHERE (u_employeenumber = @employeeNumber 
                       OR (ISNUMERIC(@employeeNumber) = 1 AND ISNUMERIC(u_employeenumber) = 1 
                           AND CAST(u_employeenumber AS INT) = CAST(@employeeNumber AS INT)))
                AND CAST(vd_dateupload AS DATE) = (
                    SELECT MAX(CAST(vd_dateupload AS DATE)) 
                    FROM VehicleDetail 
                    WHERE vd_dateupload IS NOT NULL
                )
                ORDER BY vd_insertdatetime DESC";

            using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@employeeNumber", employeeNumber ?? (object)DBNull.Value);

            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                var result = new VehicleMaintenanceDto
                {
                    vd_id = reader.GetInt32("vd_id"),
                    vd_insertdatetime = reader.GetDateTime("vd_insertdatetime"),
                    vd_modifieddatetime = reader.IsDBNull("vd_modifieddatetime") ? null : reader.GetDateTime("vd_modifieddatetime"),
                    vd_dateupload = reader.IsDBNull("vd_dateupload") ? null : reader.GetDateTime("vd_dateupload"),
                    vd_maintproduct = reader.IsDBNull("vd_maintproduct") ? null : reader.GetString("vd_maintproduct"),
                    vd_monthsoncurrentservice = reader.IsDBNull("vd_monthsoncurrentservice") ? null : reader.GetInt32("vd_monthsoncurrentservice"),
                    vd_custname = reader.IsDBNull("vd_custname") ? null : reader.GetString("vd_custname"),
                    vd_driver = reader.IsDBNull("vd_driver") ? null : reader.GetString("vd_driver"),
                    vd_vin = reader.IsDBNull("vd_vin") ? null : reader.GetString("vd_vin"),
                    vd_maintcostcode = reader.IsDBNull("vd_maintcostcode") ? null : reader.GetString("vd_maintcostcode"),
                    vd_customervehicleid = reader.IsDBNull("vd_customervehicleid") ? null : reader.GetString("vd_customervehicleid"),
                    vd_year = reader.IsDBNull("vd_year") ? null : reader.GetInt32("vd_year"),
                    vd_make = reader.IsDBNull("vd_make") ? null : reader.GetString("vd_make"),
                    vd_model = reader.IsDBNull("vd_model") ? null : reader.GetString("vd_model"),
                    vd_series = reader.IsDBNull("vd_series") ? null : reader.GetString("vd_series"),
                    vd_vehicle = reader.IsDBNull("vd_vehicle") ? null : reader.GetString("vd_vehicle"),
                    vd_openrecall = reader.IsDBNull("vd_openrecall") ? null : reader.GetString("vd_openrecall"),
                    vd_oilchangedate = reader.IsDBNull("vd_oilchangedate") ? null : reader.GetDateTime("vd_oilchangedate"),
                    vd_oilchangemileage = reader.IsDBNull("vd_oilchangemileage") ? null : reader.GetInt32("vd_oilchangemileage"),
                    vd_estmileagesinceoilchange = reader.IsDBNull("vd_estmileagesinceoilchange") ? null : reader.GetInt32("vd_estmileagesinceoilchange"),
                    vd_contractedbrakesets = reader.IsDBNull("vd_contractedbrakesets") ? null : reader.GetInt32("vd_contractedbrakesets"),
                    vd_availablebrakesets = reader.IsDBNull("vd_availablebrakesets") ? null : reader.GetInt32("vd_availablebrakesets"),
                    vd_brakereplacementdate = reader.IsDBNull("vd_brakereplacementdate") ? null : reader.GetDateTime("vd_brakereplacementdate"),
                    vd_frontrearboth = reader.IsDBNull("vd_frontrearboth") ? null : reader.GetString("vd_frontrearboth"),
                    vd_brakereplacementmileage = reader.IsDBNull("vd_brakereplacementmileage") ? null : reader.GetInt32("vd_brakereplacementmileage"),
                    vd_estmileagesincebrakereplacement = reader.IsDBNull("vd_estmileagesincebrakereplacement") ? null : reader.GetInt32("vd_estmileagesincebrakereplacement"),
                    vd_contractedtires = reader.IsDBNull("vd_contractedtires") ? null : reader.GetInt32("vd_contractedtires"),
                    vd_availabletires = reader.IsDBNull("vd_availabletires") ? null : reader.GetInt32("vd_availabletires"),
                    vd_tirereplacementdate = reader.IsDBNull("vd_tirereplacementdate") ? null : reader.GetDateTime("vd_tirereplacementdate"),
                    vd_tirereplacementmileage = reader.IsDBNull("vd_tirereplacementmileage") ? null : reader.GetInt32("vd_tirereplacementmileage"),
                    vd_estmileagesincetirereplacement = reader.IsDBNull("vd_estmileagesincetirereplacement") ? null : reader.GetInt32("vd_estmileagesincetirereplacement"),
                    vd_estimatedcurrentmileage = reader.IsDBNull("vd_estimatedcurrentmileage") ? null : reader.GetInt32("vd_estimatedcurrentmileage"),
                    u_employeenumber = reader.IsDBNull("u_employeenumber") ? null : reader.GetString("u_employeenumber")
                };

                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "GetVehicleMaintenanceByEmployeeNumber",
                    Detail = $"Retrieved vehicle maintenance data for employee {employeeNumber}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return result;
            }

            // No data found for this employee
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetVehicleMaintenanceByEmployeeNumber",
                Detail = $"No vehicle maintenance data found for employee {employeeNumber}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return null;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error retrieving vehicle maintenance data for employee {EmployeeNumber}", employeeNumber);
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetVehicleMaintenanceByEmployeeNumber",
                Detail = $"Error retrieving vehicle maintenance data for employee {employeeNumber}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<int> UploadVehicleMaintenanceRecordsAsync(List<VehicleMaintenanceUploadDto> records)
    {
        var stopwatch = Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("Database connection string is not configured");
        }

        try
        {
            var uploadDate = DateTime.Today;
            
            // Log sample of incoming data for debugging
            if (records.Count > 0)
            {
                var firstRecord = records[0];
            }
            
            DateTime? ParseDateTest(string? dateStr)
            {
                if (string.IsNullOrWhiteSpace(dateStr)) return null;
                
                // Try standard parsing first
                if (DateTime.TryParse(dateStr, out var date))
                    return date;
                
                // Try specific common formats for Excel data
                string[] formats = {
                    "M/d/yyyy",     // 4/2/2025
                    "MM/dd/yyyy",   // 04/02/2025
                    "M/d/yy",       // 4/2/25
                    "MM/dd/yy",     // 04/02/25
                    "yyyy-MM-dd",   // 2025-04-02
                    "M-d-yyyy",     // 4-2-2025
                    "MM-dd-yyyy",   // 04-02-2025
                    "d/M/yyyy",     // 2/4/2025 (day/month format)
                    "dd/MM/yyyy"    // 02/04/2025 (day/month format)
                };
                
                foreach (var format in formats)
                {
                    if (DateTime.TryParseExact(dateStr, format, System.Globalization.CultureInfo.InvariantCulture, 
                        System.Globalization.DateTimeStyles.None, out date))
                    {
                        return date;
                    }
                }
                
                return null;
            }
            var insertedCount = 0;

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            // Clear existing records for today (rip and replace)
            const string deleteSql = @"DELETE FROM VehicleDetail WHERE CAST(vd_dateupload AS DATE) = @uploadDate";
            using var deleteCommand = new SqlCommand(deleteSql, connection);
            deleteCommand.Parameters.AddWithValue("@uploadDate", uploadDate);
            await deleteCommand.ExecuteNonQueryAsync();

            // Insert new records
            const string insertSql = @"
                INSERT INTO VehicleDetail (
                    vd_dateupload, vd_maintproduct, vd_monthsoncurrentservice, vd_custname, vd_driver,
                    vd_vin, vd_maintcostcode, vd_customervehicleid, vd_year, vd_make, vd_model,
                    vd_series, vd_vehicle, vd_openrecall, vd_oilchangedate, vd_oilchangemileage,
                    vd_estmileagesinceoilchange, vd_contractedbrakesets, vd_availablebrakesets,
                    vd_brakereplacementdate, vd_frontrearboth, vd_brakereplacementmileage,
                    vd_estmileagesincebrakereplacement, vd_contractedtires, vd_availabletires,
                    vd_tirereplacementdate, vd_tirereplacementmileage, vd_estmileagesincetirereplacement,
                    vd_estimatedcurrentmileage, u_employeenumber
                ) VALUES (
                    @uploadDate, @maintProduct, @monthsOnCurrentService, @custName, @driver,
                    @vin, @maintCostCode, @customerVehicleId, @year, @make, @model,
                    @series, @vehicle, @openRecall, @oilChangeDate, @oilChangeMileage,
                    @estMileageSinceOilChange, @contractedBrakeSets, @availableBrakeSets,
                    @brakeReplacementDate, @frontRearBoth, @brakeReplacementMileage,
                    @estMileageSinceBrakeReplacement, @contractedTires, @availableTires,
                    @tireReplacementDate, @tireReplacementMileage, @estMileageSinceTireReplacement,
                    @estimatedCurrentMileage, @employeeNumber
                )";

            foreach (var record in records)
            {
                using var insertCommand = new SqlCommand(insertSql, connection);
                
                // Helper function to parse dates with multiple format support
                DateTime? ParseDate(string? dateStr)
                {
                    if (string.IsNullOrWhiteSpace(dateStr)) return null;
                    
                    // Try standard parsing first
                    if (DateTime.TryParse(dateStr, out var date))
                        return date;
                    
                    // Try specific common formats for Excel data
                    string[] formats = {
                        "M/d/yyyy",     // 4/2/2025
                        "MM/dd/yyyy",   // 04/02/2025
                        "M/d/yy",       // 4/2/25
                        "MM/dd/yy",     // 04/02/25
                        "yyyy-MM-dd",   // 2025-04-02
                        "M-d-yyyy",     // 4-2-2025
                        "MM-dd-yyyy",   // 04-02-2025
                        "d/M/yyyy",     // 2/4/2025 (day/month format)
                        "dd/MM/yyyy"    // 02/04/2025 (day/month format)
                    };
                    
                    foreach (var format in formats)
                    {
                        if (DateTime.TryParseExact(dateStr, format, System.Globalization.CultureInfo.InvariantCulture, 
                            System.Globalization.DateTimeStyles.None, out date))
                        {
                            return date;
                        }
                    }
                    
                    return null;
                }

                // Helper function to safely truncate strings to prevent SQL truncation errors
                string? SafeTruncate(string? value, int maxLength)
                {
                    if (string.IsNullOrEmpty(value)) return value;
                    return value.Length > maxLength ? value.Substring(0, maxLength) : value;
                }

                insertCommand.Parameters.AddWithValue("@uploadDate", uploadDate);
                insertCommand.Parameters.AddWithValue("@maintProduct", (object?)SafeTruncate(record.vdMaintProduct, 255) ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@monthsOnCurrentService", (object?)record.vdMonthsOnCurrentService ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@custName", (object?)SafeTruncate(record.vdCustName, 255) ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@driver", (object?)SafeTruncate(record.vdDriver, 255) ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@vin", (object?)SafeTruncate(record.vdVin, 50) ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@maintCostCode", (object?)SafeTruncate(record.vdMaintCostCode, 50) ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@customerVehicleId", (object?)SafeTruncate(record.vdCustomerVehicleId, 100) ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@year", (object?)record.vdYear ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@make", (object?)SafeTruncate(record.vdMake, 100) ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@model", (object?)SafeTruncate(record.vdModel, 100) ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@series", (object?)SafeTruncate(record.vdSeries, 100) ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@vehicle", (object?)SafeTruncate(record.vdVehicle, 255) ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@openRecall", (object?)SafeTruncate(record.vdOpenRecall, 255) ?? DBNull.Value);
                
                // Parse and add date fields
                var oilChangeDate = ParseDate(record.vdOilChangeDate);
                if (oilChangeDate.HasValue)
                    insertCommand.Parameters.AddWithValue("@oilChangeDate", oilChangeDate.Value);
                else
                    insertCommand.Parameters.AddWithValue("@oilChangeDate", DBNull.Value);
                
                insertCommand.Parameters.AddWithValue("@oilChangeMileage", (object?)record.vdOilChangeMileage ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@estMileageSinceOilChange", (object?)record.vdEstMileageSinceOilChange ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@contractedBrakeSets", (object?)record.vdContractedBrakeSets ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@availableBrakeSets", (object?)record.vdAvailableBrakeSets ?? DBNull.Value);
                
                var brakeReplacementDate = ParseDate(record.vdBrakeReplacementDate);
                if (brakeReplacementDate.HasValue)
                    insertCommand.Parameters.AddWithValue("@brakeReplacementDate", brakeReplacementDate.Value);
                else
                    insertCommand.Parameters.AddWithValue("@brakeReplacementDate", DBNull.Value);
                insertCommand.Parameters.AddWithValue("@frontRearBoth", (object?)SafeTruncate(record.vdFrontRearBoth, 50) ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@brakeReplacementMileage", (object?)record.vdBrakeReplacementMileage ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@estMileageSinceBrakeReplacement", (object?)record.vdEstMileageSinceBrakeReplacement ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@contractedTires", (object?)record.vdContractedTires ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@availableTires", (object?)record.vdAvailableTires ?? DBNull.Value);
                
                var tireReplacementDate = ParseDate(record.vdTireReplacementDate);
                if (tireReplacementDate.HasValue)
                    insertCommand.Parameters.AddWithValue("@tireReplacementDate", tireReplacementDate.Value);
                else
                    insertCommand.Parameters.AddWithValue("@tireReplacementDate", DBNull.Value);
                insertCommand.Parameters.AddWithValue("@tireReplacementMileage", (object?)record.vdTireReplacementMileage ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@estMileageSinceTireReplacement", (object?)record.vdEstMileageSinceTireReplacement ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@estimatedCurrentMileage", (object?)record.vdEstimatedCurrentMileage ?? DBNull.Value);
                insertCommand.Parameters.AddWithValue("@employeeNumber", (object?)SafeTruncate(record.uEmployeeNumber, 50) ?? DBNull.Value);

                try
                {
                    await insertCommand.ExecuteNonQueryAsync();
                    insertedCount++;
                }
                catch (SqlException sqlEx) when (sqlEx.Message.Contains("truncated"))
                {
                    // Log detailed information about the problematic record
                    var problemFields = new List<string>();
                    if (!string.IsNullOrEmpty(record.vdMaintProduct) && record.vdMaintProduct.Length > 255) 
                        problemFields.Add($"MaintProduct: {record.vdMaintProduct.Length} chars");
                    if (!string.IsNullOrEmpty(record.vdCustName) && record.vdCustName.Length > 255) 
                        problemFields.Add($"CustName: {record.vdCustName.Length} chars");
                    if (!string.IsNullOrEmpty(record.vdDriver) && record.vdDriver.Length > 255) 
                        problemFields.Add($"Driver: {record.vdDriver.Length} chars");
                    if (!string.IsNullOrEmpty(record.vdVin) && record.vdVin.Length > 50) 
                        problemFields.Add($"VIN: {record.vdVin.Length} chars");
                    if (!string.IsNullOrEmpty(record.vdCustomerVehicleId) && record.vdCustomerVehicleId.Length > 100) 
                        problemFields.Add($"CustomerVehicleId: {record.vdCustomerVehicleId.Length} chars");
                    if (!string.IsNullOrEmpty(record.uEmployeeNumber) && record.uEmployeeNumber.Length > 50) 
                        problemFields.Add($"EmployeeNumber: {record.uEmployeeNumber.Length} chars");
                    
                    await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
                    {
                        Name = "DataService",
                        Description = "UploadVehicleMaintenanceRecords - Data Truncation",
                        Detail = $"Record caused truncation error. Problematic fields: {string.Join(", ", problemFields)}. VIN: {record.vdVin}. Error: {sqlEx.Message}",
                        MachineName = Environment.MachineName
                    });
                    
                    // Skip this record and continue with the next one
                    continue;
                }
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UploadVehicleMaintenanceRecords",
                Detail = $"Uploaded {insertedCount} vehicle maintenance records",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return insertedCount;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error uploading vehicle maintenance records");
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UploadVehicleMaintenanceRecords",
                Detail = $"Error uploading vehicle maintenance records: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<bool> InsertTimeTrackingDetailAsync(int userId, int tttId, int? woId, decimal? latBrowser = null, decimal? lonBrowser = null, string? ttdType = null)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            using var connection = new SqlConnection(_configuration.GetConnectionString("DefaultConnection"));
            await connection.OpenAsync();

            // If wo_id is not provided, query for the most recent/current work order for this user
            int? actualWoId = woId;
            DateTime? woStartDateTime = null;
            DateTime? woEndDateTime = null;

            // Get work order location for distance/travel time calculations
            string? woAddress = null;

            if (!woId.HasValue || woId.Value == 0)
            {
                const string findWoSql = @"
                    SELECT TOP 1 wo.wo_id, wo.wo_startdatetime, wo.wo_enddatetime, 
                           LTRIM(RTRIM(COALESCE(a.a_address1, ''))) + ', ' + 
                           LTRIM(RTRIM(COALESCE(a.a_city, ''))) + ', ' + 
                           LTRIM(RTRIM(COALESCE(a.a_state, ''))) + ' ' + 
                           LTRIM(RTRIM(COALESCE(a.a_zip, ''))) as wo_address
                    FROM servicerequest sr
                    INNER JOIN workorder wo ON sr.sr_id = wo.sr_id
                    INNER JOIN xrefworkorderuser xwou ON wo.wo_id = xwou.wo_id
                    LEFT JOIN location l ON sr.l_id = l.l_id
                    LEFT JOIN address a ON l.a_id = a.a_id
                    WHERE xwou.u_id = @u_id
                      AND wo.wo_startdatetime >= DATEADD(HOUR, -12, GETDATE())
                      AND wo.wo_startdatetime <= DATEADD(HOUR, 24, GETDATE())
                    ORDER BY ABS(DATEDIFF(SECOND, wo.wo_startdatetime, GETDATE()))";

                using var findWoCommand = new SqlCommand(findWoSql, connection);
                findWoCommand.Parameters.Add("@u_id", System.Data.SqlDbType.Int).Value = userId;

                using var reader = await findWoCommand.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    actualWoId = reader.GetInt32(0);
                    woStartDateTime = !reader.IsDBNull(1) ? reader.GetDateTime(1) : (DateTime?)null;
                    woEndDateTime = !reader.IsDBNull(2) ? reader.GetDateTime(2) : (DateTime?)null;
                    woAddress = !reader.IsDBNull(3) ? reader.GetString(3) : null;
                    
                    _logger.LogInformation("Found work order {WoId} for user {UserId} with start time {StartTime}, location: {WoAddress}", 
                        actualWoId, userId, woStartDateTime, woAddress);
                }
                else
                {
                    _logger.LogWarning("No work order found for user {UserId} within time window", userId);
                }
            }
            else
            {
                // Work order was provided, get its location details
                const string getWoLocationSql = @"
                    SELECT LTRIM(RTRIM(COALESCE(a.a_address1, ''))) + ', ' + 
                           LTRIM(RTRIM(COALESCE(a.a_city, ''))) + ', ' + 
                           LTRIM(RTRIM(COALESCE(a.a_state, ''))) + ' ' + 
                           LTRIM(RTRIM(COALESCE(a.a_zip, ''))) as wo_address,
                           wo.wo_startdatetime, wo.wo_enddatetime
                    FROM workorder wo
                    INNER JOIN servicerequest sr ON wo.sr_id = sr.sr_id
                    LEFT JOIN location l ON sr.l_id = l.l_id
                    LEFT JOIN address a ON l.a_id = a.a_id
                    WHERE wo.wo_id = @wo_id";

                using var getWoCommand = new SqlCommand(getWoLocationSql, connection);
                getWoCommand.Parameters.Add("@wo_id", System.Data.SqlDbType.Int).Value = woId.Value;

                using var reader = await getWoCommand.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    woAddress = !reader.IsDBNull(0) ? reader.GetString(0) : null;
                    woStartDateTime = !reader.IsDBNull(1) ? reader.GetDateTime(1) : (DateTime?)null;
                    woEndDateTime = !reader.IsDBNull(2) ? reader.GetDateTime(2) : (DateTime?)null;
                    actualWoId = woId;
                }
            }

            // Retrieve Fleetmatics GPS coordinates
            decimal? latFleetmatics = null;
            decimal? lonFleetmatics = null;
            try
            {
                // Query for user's vehicle number
                const string vehicleSql = "SELECT u_vehiclenumber FROM [user] WHERE u_id = @u_id";
                using var vehicleCommand = new SqlCommand(vehicleSql, connection);
                vehicleCommand.Parameters.Add("@u_id", System.Data.SqlDbType.Int).Value = userId;

                var vehicleNumber = await vehicleCommand.ExecuteScalarAsync() as string;

                if (!string.IsNullOrWhiteSpace(vehicleNumber))
                {
                    _logger.LogInformation("Retrieving Fleetmatics GPS for user {UserId}, vehicle {VehicleNumber}", userId, vehicleNumber);

                    // Call Fleetmatics API to get vehicle location
                    var vehicleLocations = await _fleetmaticsService.GetVehicleLocationsAsync(new List<string> { vehicleNumber });

                    if (vehicleLocations != null && vehicleLocations.Count > 0)
                    {
                        var location = vehicleLocations[0];
                        latFleetmatics = (decimal?)location.Latitude;
                        lonFleetmatics = (decimal?)location.Longitude;

                        _logger.LogInformation("Retrieved Fleetmatics GPS for vehicle {VehicleNumber}: Lat {Lat}, Lon {Lon}", 
                            vehicleNumber, latFleetmatics, lonFleetmatics);
                    }
                    else
                    {
                        _logger.LogWarning("No Fleetmatics location data returned for vehicle {VehicleNumber}", vehicleNumber);
                    }
                }
                else
                {
                    _logger.LogInformation("No vehicle number assigned for user {UserId}, skipping Fleetmatics lookup", userId);
                }
            }
            catch (Exception fleetEx)
            {
                // Log error but don't fail the entire operation
                _logger.LogError(fleetEx, "Error retrieving Fleetmatics GPS for user {UserId}, continuing with time tracking insert", userId);
            }

            // Calculate time available based on ttd_type
            int? timeAvailableMinutes = null;
            
            if (!string.IsNullOrEmpty(ttdType))
            {
                var currentUtcTime = DateTime.UtcNow;
                
                if ((ttdType == "CheckIn" || ttdType == "ClockIn" || ttdType == "CheckedInPeriodic" || ttdType == "ClockedInPeriodic") && woStartDateTime.HasValue)
                {
                    // Calculate difference: WO Start - Current (positive = early, negative = late)
                    var timeDiff = woStartDateTime.Value - currentUtcTime;
                    timeAvailableMinutes = (int)Math.Round(timeDiff.TotalMinutes);
                    
                    _logger.LogInformation("Time available calculation for {Type}: {Minutes} minutes (WO Start: {WoStart}, Current: {Current})", 
                        ttdType, timeAvailableMinutes, woStartDateTime.Value, currentUtcTime);
                }
                else if ((ttdType == "CheckOut" || ttdType == "ClockOut") && woEndDateTime.HasValue)
                {
                    // Calculate difference: Current - WO End (positive = late, negative = early)
                    var timeDiff = currentUtcTime - woEndDateTime.Value;
                    timeAvailableMinutes = (int)Math.Round(timeDiff.TotalMinutes);
                    
                    _logger.LogInformation("Time available calculation for {Type}: {Minutes} minutes (Current: {Current}, WO End: {WoEnd})", 
                        ttdType, timeAvailableMinutes, currentUtcTime, woEndDateTime.Value);
                }
            }

            // Calculate distances and travel times using Google Maps
            decimal? distanceBrowserMiles = null;
            decimal? distanceFleetmaticsMiles = null;
            int? travelTimeBrowserMinutes = null;
            int? travelTimeFleetmaticsMinutes = null;

            try
            {
                // Only calculate if we have work order address and at least one set of coordinates
                if (!string.IsNullOrWhiteSpace(woAddress) && (latBrowser.HasValue || latFleetmatics.HasValue))
                {
                    _logger.LogInformation("Calculating distances to work order address: {WoAddress}", woAddress);

                    // Calculate browser-based distance and travel time
                    if (latBrowser.HasValue && lonBrowser.HasValue)
                    {
                        try
                        {
                            string browserLatLon = $"{latBrowser.Value},{lonBrowser.Value}";
                            var browserResult = await _googleMapsService.GetDistanceAndDurationAsync(browserLatLon, woAddress);

                            if (browserResult != null && browserResult.Status == "OK")
                            {
                                // Convert meters to miles (1 meter = 0.000621371 miles)
                                distanceBrowserMiles = (decimal)(browserResult.DistanceMeters * 0.000621371);
                                // Convert seconds to minutes (rounded)
                                travelTimeBrowserMinutes = (int)Math.Round(browserResult.DurationSeconds / 60.0);

                                _logger.LogInformation("Browser distance calculation: {Distance} miles, {Duration} minutes (from {Origin} to {Destination})", 
                                    distanceBrowserMiles, travelTimeBrowserMinutes, browserLatLon, woAddress);
                            }
                            else
                            {
                                _logger.LogWarning("Google Maps API returned status {Status} for browser coordinates", browserResult?.Status);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error calculating browser distance and travel time");
                        }
                    }

                    // Calculate Fleetmatics-based distance and travel time
                    if (latFleetmatics.HasValue && lonFleetmatics.HasValue)
                    {
                        try
                        {
                            string fleetmaticsLatLon = $"{latFleetmatics.Value},{lonFleetmatics.Value}";
                            var fleetmaticsResult = await _googleMapsService.GetDistanceAndDurationAsync(fleetmaticsLatLon, woAddress);

                            if (fleetmaticsResult != null && fleetmaticsResult.Status == "OK")
                            {
                                // Convert meters to miles (1 meter = 0.000621371 miles)
                                distanceFleetmaticsMiles = (decimal)(fleetmaticsResult.DistanceMeters * 0.000621371);
                                // Convert seconds to minutes (rounded)
                                travelTimeFleetmaticsMinutes = (int)Math.Round(fleetmaticsResult.DurationSeconds / 60.0);

                                _logger.LogInformation("Fleetmatics distance calculation: {Distance} miles, {Duration} minutes (from {Origin} to {Destination})", 
                                    distanceFleetmaticsMiles, travelTimeFleetmaticsMinutes, fleetmaticsLatLon, woAddress);
                            }
                            else
                            {
                                _logger.LogWarning("Google Maps API returned status {Status} for Fleetmatics coordinates", fleetmaticsResult?.Status);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error calculating Fleetmatics distance and travel time");
                        }
                    }
                }
                else
                {
                    _logger.LogInformation("Skipping distance calculations - work order address or GPS coordinates not available. WO Address: {WoAddress}, Browser GPS: {BrowserGps}, Fleetmatics GPS: {FleetmaticsGps}", 
                        woAddress, latBrowser.HasValue && lonBrowser.HasValue, latFleetmatics.HasValue && lonFleetmatics.HasValue);
                }
            }
            catch (Exception distanceEx)
            {
                // Log error but don't fail the entire operation
                _logger.LogError(distanceEx, "Error calculating distances and travel times, continuing with time tracking insert");
            }

            // Insert time tracking detail with the resolved work order information, GPS data, and distance/travel time data
            const string sql = @"
                INSERT INTO timetrackingdetail (u_id, ttt_id, wo_id, ttd_insertdatetime, ttd_lat_browser, ttd_lon_browser, ttd_lat_fleetmatics, ttd_lon_fleetmatics, ttd_type, wo_startdatetime, wo_enddatetime, ttd_distanceinmilesbrowser, ttd_distanceinmilesfleetmatics, ttd_traveltimeinminutesbrowser, ttd_traveltimeinminutesfleetmatics)
                VALUES (@u_id, @ttt_id, @wo_id, @ttd_insertdatetime, @ttd_lat_browser, @ttd_lon_browser, @ttd_lat_fleetmatics, @ttd_lon_fleetmatics, @ttd_type, @wo_startdatetime, @wo_enddatetime, @ttd_distanceinmilesbrowser, @ttd_distanceinmilesfleetmatics, @ttd_traveltimeinminutesbrowser, @ttd_traveltimeinminutesfleetmatics)";

            using var command = new SqlCommand(sql, connection);
            
            // Add parameters with proper types
            command.Parameters.Add("@u_id", System.Data.SqlDbType.Int).Value = userId;
            command.Parameters.Add("@ttt_id", System.Data.SqlDbType.Int).Value = tttId;
            command.Parameters.Add("@wo_id", System.Data.SqlDbType.Int).Value = actualWoId.HasValue ? (object)actualWoId.Value : DBNull.Value;
            command.Parameters.Add("@ttd_insertdatetime", System.Data.SqlDbType.DateTime).Value = DateTime.UtcNow;
            command.Parameters.Add("@ttd_lat_browser", System.Data.SqlDbType.Decimal).Value = latBrowser.HasValue ? (object)latBrowser.Value : DBNull.Value;
            command.Parameters.Add("@ttd_lon_browser", System.Data.SqlDbType.Decimal).Value = lonBrowser.HasValue ? (object)lonBrowser.Value : DBNull.Value;
            command.Parameters.Add("@ttd_lat_fleetmatics", System.Data.SqlDbType.Decimal).Value = latFleetmatics.HasValue ? (object)latFleetmatics.Value : DBNull.Value;
            command.Parameters.Add("@ttd_lon_fleetmatics", System.Data.SqlDbType.Decimal).Value = lonFleetmatics.HasValue ? (object)lonFleetmatics.Value : DBNull.Value;
            command.Parameters.Add("@ttd_type", System.Data.SqlDbType.NVarChar, 50).Value = !string.IsNullOrEmpty(ttdType) ? (object)ttdType : DBNull.Value;
            command.Parameters.Add("@wo_startdatetime", System.Data.SqlDbType.DateTime).Value = woStartDateTime.HasValue ? (object)woStartDateTime.Value : DBNull.Value;
            command.Parameters.Add("@wo_enddatetime", System.Data.SqlDbType.DateTime).Value = woEndDateTime.HasValue ? (object)woEndDateTime.Value : DBNull.Value;
            command.Parameters.Add("@ttd_distanceinmilesbrowser", System.Data.SqlDbType.Decimal).Value = distanceBrowserMiles.HasValue ? (object)distanceBrowserMiles.Value : DBNull.Value;
            command.Parameters.Add("@ttd_distanceinmilesfleetmatics", System.Data.SqlDbType.Decimal).Value = distanceFleetmaticsMiles.HasValue ? (object)distanceFleetmaticsMiles.Value : DBNull.Value;
            command.Parameters.Add("@ttd_traveltimeinminutesbrowser", System.Data.SqlDbType.Int).Value = travelTimeBrowserMinutes.HasValue ? (object)travelTimeBrowserMinutes.Value : DBNull.Value;
            command.Parameters.Add("@ttd_traveltimeinminutesfleetmatics", System.Data.SqlDbType.Int).Value = travelTimeFleetmaticsMinutes.HasValue ? (object)travelTimeFleetmaticsMinutes.Value : DBNull.Value;

            var rowsAffected = await command.ExecuteNonQueryAsync();
            stopwatch.Stop();

            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "InsertTimeTrackingDetail",
                Detail = $"Inserted time tracking detail for User {userId}, TTT_ID {tttId}, WO_ID {actualWoId} (original: {woId}), Browser: {latBrowser}/{lonBrowser} ({distanceBrowserMiles}mi, {travelTimeBrowserMinutes}min), Fleetmatics: {latFleetmatics}/{lonFleetmatics} ({distanceFleetmaticsMiles}mi, {travelTimeFleetmaticsMinutes}min), Type: {ttdType}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return rowsAffected > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error inserting time tracking detail for User {UserId}, TTT_ID {TttId}, WO_ID {WoId}", 
                userId, tttId, woId);
            
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "InsertTimeTrackingDetail",
                Detail = $"Error inserting time tracking detail for User {userId}, TTT_ID {tttId}, WO_ID {woId}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            throw;
        }
    }
}
