using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.SqlClient;

namespace EvoAPI.Core.Services;

public class DataService : IDataService
{
    private readonly ILogger<DataService> _logger;
    private readonly IConfiguration _configuration;
    private readonly IAuditService _auditService;

    public DataService(ILogger<DataService> logger, IConfiguration configuration, IAuditService auditService)
    {
        _logger = logger;
        _configuration = configuration;
        _auditService = auditService;
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
                        z.z_number            AS Zone,
                        u_createdby.u_firstname + ' ' + u_createdby.u_lastname AS CreatedBy,
                        sr.sr_escalated       AS Escalated,
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
                    Zone,
                    CreatedBy,
                    Escalated
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

    public async Task<DataTable> GetWorkOrdersScheduleAsync(int numberOfDays)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            if (numberOfDays > 1500) numberOfDays = 180;

            const string sql = @" 
                WITH RankedOrders AS (
                    SELECT
                        sr.sr_id              AS sr_id,
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
                        z.z_number            AS Zone,
                            u_createdby.u_firstname + ' ' + u_createdby.u_lastname AS CreatedBy,
                        sr.sr_escalated       AS Escalated,
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
                )
                SELECT
                    sr_id,
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
                    Zone,
                    CreatedBy,
                    Escalated
                FROM RankedOrders
                ORDER BY CallCenter, Company, Trade, requestnumber;";

            var parameters = new Dictionary<string, object> { { "@numberOfDays", numberOfDays } };

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

    public async Task<DataTable> ExecuteQueryAsync(string sql, Dictionary<string, object>? parameters = null)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        var dataTable = new DataTable();
        
        // Log the exact SQL being executed
        _logger.LogInformation("=== EXECUTING SQL ===");
        _logger.LogInformation("SQL: {Sql}", sql);
        
        using var connection = new SqlConnection(connectionString);
        connection.ConnectionString += ";Connection Timeout=30;";
        
        using var command = new SqlCommand(sql, connection);
        command.CommandTimeout = 30;
        
        if (parameters != null)
        {
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value ?? DBNull.Value);
                _logger.LogInformation("Parameter: {Key} = {Value}", param.Key, param.Value);
            }
        }
        
        await connection.OpenAsync();
        using var adapter = new SqlDataAdapter(command);
        adapter.Fill(dataTable);
        
        _logger.LogInformation("Query completed successfully. Rows returned: {Count}", dataTable.Rows.Count);
        
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
        _logger.LogInformation("=== EXECUTING NON-QUERY SQL ===");
        _logger.LogInformation("SQL: {Sql}", sql);
        
        using var connection = new SqlConnection(connectionString);
        connection.ConnectionString += ";Connection Timeout=30;";
        
        using var command = new SqlCommand(sql, connection);
        command.CommandTimeout = 30;
        
        if (parameters != null)
        {
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value ?? DBNull.Value);
                _logger.LogInformation("Parameter: {Key} = {Value}", param.Key, param.Value);
            }
        }
        
        await connection.OpenAsync();
        var rowsAffected = await command.ExecuteNonQueryAsync();
        
        _logger.LogInformation("Non-query completed successfully. Rows affected: {Count}", rowsAffected);
        
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
                    u.u_phonemobile as PhoneMobile 
                FROM [user] u, role r, xrefUserRole x 
                WHERE u.u_id = x.u_id 
                    AND r.r_id = x.r_id 
                    AND r.r_role = 'Technician' 
                    AND u.u_active = 1 
                ORDER BY u_lastname";

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
                WITH ranked_results AS (
                    select sr.sr_id, 
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
                            WHEN latest_note.won_insertdatetime IS NULL THEN NULL
                            ELSE DATEDIFF(HOUR, latest_note.won_insertdatetime, GETDATE())
                        END as hours_since_last_note,
                        CASE 
                            WHEN latest_status_change.ssc_insertdatetime IS NULL THEN 0
                            ELSE DATEDIFF(DAY, latest_status_change.ssc_insertdatetime, GETDATE())
                        END as days_in_current_status,
                        cc.cc_attack as AttackCallCenter,
                        p.p_attack AttackPriority, 
                        ss.ss_attack AttackStatusSecondary,
                        ISNULL(apn_lookup.apn_attack, 0) as AttackHoursSinceLastNote,
                        ISNULL(aps_lookup.aps_attack, 0) as AttackDaysInStatus,
                        ISNULL(apad_lookup.apad_attack, 0) as AttackActionableDate,
                        (p.p_attack + ss.ss_attack + ISNULL(aps_lookup.aps_attack, 0) + ISNULL(apn_lookup.apn_attack, 0) + cc.cc_attack + ISNULL(apad_lookup.apad_attack, 0)) as AttackPoints,
                        admin_user.u_id as admin_u_id,
                        admin_user.u_firstname as admin_firstname,
                        admin_user.u_lastname as admin_lastname,
                        -- Add escalated flag for prioritization
                        CASE WHEN sr.sr_escalated IS NOT NULL THEN 1 ELSE 0 END as is_escalated,
                        ROW_NUMBER() OVER (
                            PARTITION BY ISNULL(admin_user.u_id, -1) 
                            ORDER BY 
                                CASE WHEN sr.sr_escalated IS NOT NULL THEN 0 ELSE 1 END, -- Escalated records first
                                (p.p_attack + ss.ss_attack + ISNULL(aps_lookup.aps_attack, 0) + ISNULL(apn_lookup.apn_attack, 0) + cc.cc_attack + ISNULL(apad_lookup.apad_attack, 0)) DESC
                        ) as rn
                    from servicerequest sr
                    inner join xrefCompanyCallCenter xccc on sr.xccc_id = xccc.xccc_id
                    inner join Company c on xccc.c_id = c.c_id
                    inner join callcenter cc on xccc.cc_id = cc.cc_id
                    inner join workorder wo on sr.wo_id_primary = wo.wo_id
                    inner join xrefWorkOrderUser xwou on xwou.wo_id = wo.wo_id
                    inner join [user] u on xwou.u_id = u.u_id
                    -- New zone logic based on location-to-zone relationship
                    inner join location l on sr.l_id = l.l_id
                    inner join address a on l.a_id = a.a_id
                    inner join tax on left(a.a_zip,5) = tax.tax_zip
                    inner join ZoneMicro zm on tax.zm_id = zm.zm_id
                    inner join zone z on CASE 
                        WHEN cc.cc_name = 'Residential' THEN (SELECT z_id FROM zone WHERE z_acronym = 'Residential')
                        ELSE zm.z_id 
                    END = z.z_id
                    inner join statussecondary ss on wo.ss_id = ss.ss_id
                    inner join Priority p on sr.p_id = p.p_id
                    left join trade t on sr.t_id = t.t_id
                    inner join xrefadminzonestatussecondary xazss on z.z_id = xazss.z_id and ss.ss_id = xazss.ss_id
                    inner join [user] admin_user on xazss.u_id = admin_user.u_id
                    left join (
                        -- Get the most recent note for any work order related to each service request
                        select sr_inner.sr_id,
                            won.won_insertdatetime,
                            row_number() over (partition by sr_inner.sr_id order by won.won_insertdatetime desc) as rn
                        from servicerequest sr_inner
                        inner join xrefCompanyCallCenter xccc_inner on sr_inner.xccc_id = xccc_inner.xccc_id
                        inner join Company c_inner on xccc_inner.c_id = c_inner.c_id
                        inner join workorder wo_inner on wo_inner.sr_id = sr_inner.sr_id
                        inner join workordernote won on won.wo_id = wo_inner.wo_id
                        where sr_inner.s_id not in (9, 6) -- Apply main filters in subquery
                        and c_inner.c_name NOT IN ('Metro Pipe Program')
                        and (wo_inner.wo_startdatetime BETWEEN DATEADD(DAY, -730, GETDATE()) AND DATEADD(DAY, 180, GETDATE()) 
                            or wo_inner.wo_startdatetime is null)
                    ) latest_note on latest_note.sr_id = sr.sr_id and latest_note.rn = 1
                    left join (
                        -- Get the most recent status change for the primary work order
                        select sr_inner.sr_id,
                            wo_inner.wo_id,
                            ssc.ssc_insertdatetime,
                            row_number() over (partition by wo_inner.wo_id order by ssc.ssc_insertdatetime desc) as rn
                        from servicerequest sr_inner
                        inner join xrefCompanyCallCenter xccc_inner on sr_inner.xccc_id = xccc_inner.xccc_id
                        inner join Company c_inner on xccc_inner.c_id = c_inner.c_id
                        inner join workorder wo_inner on sr_inner.wo_id_primary = wo_inner.wo_id
                        inner join statussecondarychange ssc on ssc.wo_id = wo_inner.wo_id
                        where sr_inner.s_id not in (9, 6) -- Apply main filters in subquery
                        and c_inner.c_name NOT IN ('Metro Pipe Program')
                        and (wo_inner.wo_startdatetime BETWEEN DATEADD(DAY, -730, GETDATE()) AND DATEADD(DAY, 180, GETDATE()) 
                            or wo_inner.wo_startdatetime is null)
                    ) latest_status_change on latest_status_change.wo_id = wo.wo_id and latest_status_change.rn = 1
                    -- Optimized AttackPointStatus lookup using OUTER APPLY
                    OUTER APPLY (
                        SELECT TOP 1 aps_attack
                        FROM AttackPointStatus 
                        WHERE CASE 
                                WHEN latest_status_change.ssc_insertdatetime IS NULL THEN 0
                                ELSE DATEDIFF(DAY, latest_status_change.ssc_insertdatetime, GETDATE())
                            END >= aps_daysinstatus
                        ORDER BY aps_daysinstatus DESC, aps_id DESC  -- Get the highest threshold that applies
                    ) aps_lookup
                    -- Fixed AttackPointNote lookup using OUTER APPLY
                    OUTER APPLY (
                        SELECT TOP 1 
                            CASE 
                                WHEN CAST(wo.wo_startdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS DATE) >= CAST(GETDATE() AT TIME ZONE 'Central Standard Time' AS DATE) THEN 0  -- If work order start is today or future in Central Time, return 0
                                ELSE apn_attack  -- Otherwise use normal attack points
                            END as apn_attack
                        FROM AttackPointNote 
                        WHERE 
                            -- Case 1: Truly no notes exist 
                            (latest_note.won_insertdatetime IS NULL AND apn_id = 1)  
                            OR 
                            -- Case 2: Notes exist and meet the time threshold criteria
                            (latest_note.won_insertdatetime IS NOT NULL 
                            AND DATEDIFF(HOUR, latest_note.won_insertdatetime, GETDATE()) >= apn_hours
                            AND apn_id > 1)  
                        ORDER BY 
                            CASE
                                WHEN latest_note.won_insertdatetime IS NULL THEN 0  -- NO NOTES gets highest priority
                                ELSE apn_hours  -- Otherwise order by hours threshold descending
                            END DESC
                    ) apn_lookup
                    -- New AttackPointActionableDate lookup using OUTER APPLY
                    OUTER APPLY (
                        SELECT TOP 1 apad_attack
                        FROM AttackPointActionableDate 
                        WHERE 
                            -- Case 1: No next step date exists (NULL date)
                            (sr.sr_datenextstep IS NULL AND apad_id = 1)  
                            OR 
                            -- Case 2: Next step date exists and meets the day threshold criteria
                            (sr.sr_datenextstep IS NOT NULL 
                            AND DATEDIFF(DAY, GETDATE(), sr.sr_datenextstep) <= apad_days
                            AND apad_id > 1)  
                        ORDER BY 
                            CASE
                                WHEN sr.sr_datenextstep IS NULL THEN 0  -- NULL DATE gets highest priority
                                ELSE apad_days  -- Otherwise order by days threshold ascending (since we're using <=)
                            END ASC
                    ) apad_lookup
                    where 1=1
                    and sr.s_id not in (9) --Paid
                    and sr.s_id not in (6) --Rejected
                    and c.c_name NOT IN ('Metro Pipe Program')
                    and cc.cc_name NOT IN ('Administrative')
                    and (wo.wo_startdatetime BETWEEN DATEADD(DAY, -730, GETDATE()) AND DATEADD(DAY, 180, GETDATE()) or wo.wo_startdatetime is null)
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
                FROM ranked_results 
                WHERE rn <= @TopCount OR is_escalated = 1  -- Include escalated records regardless of ranking
                ORDER BY ISNULL(admin_u_id, -1), AttackPoints DESC;
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

    public async Task<DataTable> GetTechActivityDashboardAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            
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
                WHERE tt.tt_begin >= DATEADD(DAY, -30, GETDATE()) 
                    AND ttt.ttt_id NOT IN (1)
                ORDER BY tt.tt_id DESC;
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
                        Description = "GetTechActivityDashboard",
                        Detail = $"Retrieved {dataTable.Rows.Count} tech activity records",
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
}
