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
                    Escalated,
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
                SELECT u_id as Id, o_id as OId, a_id as AId, v_id as VId, supervisor_id as SupervisorId,
                       u_insertdatetime as InsertDateTime, u_modifieddatetime as ModifiedDateTime,
                       u_username as Username, u_password as Password, u_firstname as FirstName, u_lastname as LastName,
                       u_employeenumber as EmployeeNumber, u_email as Email, u_phonehome as PhoneHome, u_phonemobile as PhoneMobile,
                       u_active as Active, u_picture as Picture, u_ssn as SSN, u_dateofhire as DateOfHire,
                       u_dateeligiblepto as DateEligiblePTO, u_dateeligiblevacation as DateEligibleVacation,
                       u_daysavailablepto as DaysAvailablePTO, u_daysavailablevacation as DaysAvailableVacation,
                       u_clothingshirt as ClothingShirt, u_clothingjacket as ClothingJacket, u_clothingpants as ClothingPants,
                       u_wirelessprovider as WirelessProvider, u_preferrednotification as PreferredNotification,
                       u_quickbooksname as QuickBooksName, u_passwordchanged as PasswordChanged, u_2fa as U_2FA,
                       z_id as ZoneId, u_covidvaccinedate as CovidVaccineDate, u_note as Note, u_notedashboard as NoteDashboard
                FROM dbo.[User]
                WHERE u_id = @UserId";

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
                 u_firstname, u_lastname, u_employeenumber, u_email, u_phonehome, u_phonemobile, u_active, 
                 u_picture, u_ssn, u_dateofhire, u_dateeligiblepto, u_dateeligiblevacation, u_daysavailablepto, 
                 u_daysavailablevacation, u_clothingshirt, u_clothingjacket, u_clothingpants, u_wirelessprovider, 
                 u_preferrednotification, u_quickbooksname, u_passwordchanged, u_2fa, z_id, u_covidvaccinedate, 
                 u_note, u_notedashboard)
                VALUES 
                (@OId, @AId, @VId, @SupervisorId, GETDATE(), GETDATE(), @Username, @Password,
                 @FirstName, @LastName, @EmployeeNumber, @Email, @PhoneHome, @PhoneMobile, @Active,
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
                    u.u_phonemobile as PhoneMobile,
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
                        CASE WHEN sr.sr_escalated IS NOT NULL THEN 1 ELSE 0 END as is_escalated,
                        -- Separate ranking for non-escalated records only
                        CASE 
                            WHEN sr.sr_escalated IS NOT NULL THEN NULL  -- Don't rank escalated records
                            WHEN cc.cc_name = 'Administrative' THEN NULL  -- Don't rank Administrative non-escalated 
                            ELSE ROW_NUMBER() OVER (
                                PARTITION BY admin_user.u_id
                                ORDER BY (p.p_attack + ss.ss_attack + ISNULL(aps_lookup.aps_attack, 0) + ISNULL(apn_lookup.apn_attack, 0) + cc.cc_attack + ISNULL(apad_lookup.apad_attack, 0)) DESC
                            )
                        END as rn_non_escalated
                    FROM servicerequest sr
                    INNER JOIN xrefCompanyCallCenter xccc ON sr.xccc_id = xccc.xccc_id
                    INNER JOIN Company c ON xccc.c_id = c.c_id
                    INNER JOIN callcenter cc ON xccc.cc_id = cc.cc_id
                    INNER JOIN workorder wo ON sr.wo_id_primary = wo.wo_id
                    LEFT JOIN xrefWorkOrderUser xwou ON xwou.wo_id = wo.wo_id AND (sr.sr_escalated IS NOT NULL OR xwou.wo_id IS NOT NULL)
                    LEFT JOIN [user] u ON xwou.u_id = u.u_id
                    INNER JOIN location l ON sr.l_id = l.l_id
                    INNER JOIN address a ON l.a_id = a.a_id
                    INNER JOIN tax ON LEFT(a.a_zip,5) = tax.tax_zip
                    INNER JOIN ZoneMicro zm ON tax.zm_id = zm.zm_id
                    INNER JOIN zone z ON CASE 
                        WHEN cc.cc_name = 'Residential' THEN (SELECT z_id FROM zone WHERE z_acronym = 'Residential')
                        ELSE zm.z_id 
                    END = z.z_id
                    INNER JOIN statussecondary ss ON wo.ss_id = ss.ss_id
                    INNER JOIN Priority p ON sr.p_id = p.p_id
                    LEFT JOIN trade t ON sr.t_id = t.t_id
                    INNER JOIN xrefadminzonestatussecondary xazss ON z.z_id = xazss.z_id AND ss.ss_id = xazss.ss_id
                    INNER JOIN [user] admin_user ON xazss.u_id = admin_user.u_id
                    LEFT JOIN (
                        SELECT sr_inner.sr_id,
                            won.won_insertdatetime,
                            ROW_NUMBER() OVER (PARTITION BY sr_inner.sr_id ORDER BY won.won_insertdatetime DESC) as rn
                        FROM servicerequest sr_inner
                        INNER JOIN xrefCompanyCallCenter xccc_inner ON sr_inner.xccc_id = xccc_inner.xccc_id
                        INNER JOIN Company c_inner ON xccc_inner.c_id = c_inner.c_id
                        INNER JOIN workorder wo_inner ON wo_inner.sr_id = sr_inner.sr_id
                        INNER JOIN workordernote won ON won.wo_id = wo_inner.wo_id
                        WHERE sr_inner.s_id NOT IN (9, 6)
                        AND c_inner.c_name NOT IN ('Metro Pipe Program')
                        AND (wo_inner.wo_startdatetime BETWEEN DATEADD(DAY, -730, GETDATE()) AND DATEADD(DAY, 180, GETDATE()) 
                            OR wo_inner.wo_startdatetime IS NULL)
                    ) latest_note ON latest_note.sr_id = sr.sr_id AND latest_note.rn = 1
                    LEFT JOIN (
                        SELECT sr_inner.sr_id,
                            wo_inner.wo_id,
                            ssc.ssc_insertdatetime,
                            ROW_NUMBER() OVER (PARTITION BY wo_inner.wo_id ORDER BY ssc.ssc_insertdatetime DESC) as rn
                        FROM servicerequest sr_inner
                        INNER JOIN xrefCompanyCallCenter xccc_inner ON sr_inner.xccc_id = xccc_inner.xccc_id
                        INNER JOIN Company c_inner ON xccc_inner.c_id = c_inner.c_id
                        INNER JOIN workorder wo_inner ON sr_inner.wo_id_primary = wo_inner.wo_id
                        INNER JOIN statussecondarychange ssc ON ssc.wo_id = wo_inner.wo_id
                        WHERE sr_inner.s_id NOT IN (9, 6)
                        AND c_inner.c_name NOT IN ('Metro Pipe Program')
                        AND (wo_inner.wo_startdatetime BETWEEN DATEADD(DAY, -730, GETDATE()) AND DATEADD(DAY, 180, GETDATE()) 
                            OR wo_inner.wo_startdatetime IS NULL)
                    ) latest_status_change ON latest_status_change.wo_id = wo.wo_id AND latest_status_change.rn = 1
                    OUTER APPLY (
                        SELECT TOP 1 aps_attack
                        FROM AttackPointStatus 
                        WHERE CASE 
                                WHEN latest_status_change.ssc_insertdatetime IS NULL THEN 0
                                ELSE DATEDIFF(DAY, latest_status_change.ssc_insertdatetime, GETDATE())
                            END >= aps_daysinstatus
                        ORDER BY aps_daysinstatus DESC, aps_id DESC
                    ) aps_lookup
                    OUTER APPLY (
                        SELECT TOP 1 
                            CASE 
                                WHEN CAST(wo.wo_startdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS DATE) >= CAST(GETDATE() AT TIME ZONE 'Central Standard Time' AS DATE) THEN 0
                                ELSE apn_attack
                            END as apn_attack
                        FROM AttackPointNote 
                        WHERE 
                            (latest_note.won_insertdatetime IS NULL AND apn_id = 1)  
                            OR 
                            (latest_note.won_insertdatetime IS NOT NULL 
                            AND DATEDIFF(HOUR, latest_note.won_insertdatetime, GETDATE()) >= apn_hours
                            AND apn_id > 1)  
                        ORDER BY 
                            CASE
                                WHEN latest_note.won_insertdatetime IS NULL THEN 0
                                ELSE apn_hours
                            END DESC
                    ) apn_lookup
                    OUTER APPLY (
                        SELECT TOP 1 apad_attack
                        FROM AttackPointActionableDate 
                        WHERE 
                            (sr.sr_datenextstep IS NULL AND apad_id = 1)  
                            OR 
                            (sr.sr_datenextstep IS NOT NULL 
                            AND DATEDIFF(DAY, GETDATE(), sr.sr_datenextstep) <= apad_days
                            AND apad_id > 1)  
                        ORDER BY 
                            CASE
                                WHEN sr.sr_datenextstep IS NULL THEN 0
                                ELSE apad_days
                            END ASC
                    ) apad_lookup
                    WHERE sr.s_id NOT IN (9, 6)  -- Exclude Paid, Rejected
                    AND c.c_name NOT IN ('Metro Pipe Program')
                    AND (wo.wo_startdatetime BETWEEN DATEADD(DAY, -730, GETDATE()) AND DATEADD(DAY, 180, GETDATE()) 
                        OR wo.wo_startdatetime IS NULL)
                    -- NO call center filtering here - handled by ranking logic instead
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
                WHERE (rn_non_escalated <= @TopCount) OR (is_escalated = 1)  -- Top 15 non-escalated per admin + ALL escalated
                ORDER BY ISNULL(admin_u_id, -1), is_escalated DESC, AttackPoints DESC;
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
                
                _logger.LogInformation("Deleted {DeletedRows} existing records for today", deletedRows);

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
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCachedDistance",
                Detail = $"Retrieved cached distance from '{fromAddress}' to '{toAddress}', found: {result.Rows.Count > 0}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

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
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "SaveCachedDistance",
                    Detail = $"Updated cached distance from '{request.FromAddress}' to '{request.ToAddress}'",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

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
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "SaveCachedDistance",
                    Detail = $"Inserted new cached distance from '{request.FromAddress}' to '{request.ToAddress}'",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

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

    private static int ConvertToInt(object value)
    {
        if (value == null || value == DBNull.Value)
            return 0;
        
        if (int.TryParse(value.ToString(), out var result))
            return result;
            
        return 0;
    }
}
