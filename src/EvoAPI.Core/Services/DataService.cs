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
                        u.u_firstname         AS AssignedFirstName,
                        u.u_lastname          AS AssignedLastName,
                        l.l_location          AS Location,
                        a.a_address1          AS Address,
                        a.a_city              AS City,
                        z.z_number            AS Zone,
                        u_createdby.u_firstname + ' ' + u_createdby.u_lastname AS CreatedBy,
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
                        (wo.wo_startdatetime BETWEEN DATEADD(DAY, -@numberOfDays, GETDATE()) AND DATEADD(DAY, 180, GETDATE()) or (wo.wo_startdatetime is null AND not s.s_status in ('Rejected', 'Paid', 'Invoiced')))
                        AND c.c_name NOT IN ('Metro Pipe Program')
                        AND (r.r_role = 'Technician' or r.r_role is null)
               )
                SELECT
                    sr_id,
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
                    AssignedFirstName,
                    AssignedLastName,
                    Location,
                    Address,
                    City,
                    Zone,
                    CreatedBy
                FROM RankedOrders
                ORDER BY CallCenter, Company, Trade, requestnumber;";

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
                        u.u_firstname         AS AssignedFirstName,
                        u.u_lastname          AS AssignedLastName,
                        l.l_location          AS Location,
                        a.a_address1          AS Address,
                        a.a_city              AS City,
                        z.z_number            AS Zone,
                            u_createdby.u_firstname + ' ' + u_createdby.u_lastname AS CreatedBy,
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
                    AssignedFirstName,
                    AssignedLastName,
                    Location,
                    Address,
                    City,
                    Zone,
                    CreatedBy
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
                           z.z_number + '-' + z.z_acronym zone, 
                           cc.cc_name,
                           c.c_name,
                           p.p_priority,
                           ss.ss_statussecondary,
                           t.t_trade, 
                           CASE 
                               WHEN latest_note.won_insertdatetime IS NULL THEN 0
                               ELSE DATEDIFF(HOUR, latest_note.won_insertdatetime, GETDATE())
                           END as hours_since_last_note,
                           CASE 
                               WHEN latest_status_change.ssc_insertdatetime IS NULL THEN 0
                               ELSE DATEDIFF(DAY, latest_status_change.ssc_insertdatetime, GETDATE())
                           END as days_in_current_status,
                           cc.cc_attack as AttackCallCenter,
                           p.p_attack AttackPriority, 
                           ss.ss_attack AttackStatusSecondary,
                           ISNULL(apn.apn_attack, 0) as AttackHoursSinceLastNote,
                           ISNULL(aps.aps_attack, 0) as AttackDaysInStatus,
                           (p.p_attack + ss.ss_attack + ISNULL(aps.aps_attack, 0) + ISNULL(apn.apn_attack, 0) + cc.cc_attack) as AttackPoints,
                           admin_user.u_id as admin_u_id,
                           admin_user.u_firstname as admin_firstname,
                           admin_user.u_lastname as admin_lastname,
                           ROW_NUMBER() OVER (PARTITION BY ISNULL(admin_user.u_id, -1) ORDER BY (p.p_attack + ss.ss_attack + ISNULL(aps.aps_attack, 0) + ISNULL(apn.apn_attack, 0) + cc.cc_attack) DESC) as rn
                    from servicerequest sr
                    inner join xrefCompanyCallCenter xccc on sr.xccc_id = xccc.xccc_id
                    inner join Company c on xccc.c_id = c.c_id
                    inner join callcenter cc on xccc.cc_id = cc.cc_id
                    inner join workorder wo on sr.wo_id_primary = wo.wo_id
                    inner join xrefWorkOrderUser xwou on xwou.wo_id = wo.wo_id
                    inner join [user] u on xwou.u_id = u.u_id
                    inner join zone z on u.z_id = z.z_id
                    inner join statussecondary ss on wo.ss_id = ss.ss_id
                    inner join Priority p on sr.p_id = p.p_id
                    left join trade t on sr.t_id = t.t_id
                    left join xrefadminzonestatussecondary xazss on z.z_id = xazss.z_id and ss.ss_id = xazss.ss_id
                    left join [user] admin_user on xazss.u_id = admin_user.u_id
                    left join (
                        -- Get the most recent note for any work order related to each service request
                        select sr_inner.sr_id,
                               won.won_insertdatetime,
                               row_number() over (partition by sr_inner.sr_id order by won.won_insertdatetime desc) as rn
                        from servicerequest sr_inner
                        inner join workorder wo_inner on wo_inner.sr_id = sr_inner.sr_id  -- All work orders for this service request
                        inner join workordernote won on won.wo_id = wo_inner.wo_id
                    ) latest_note on latest_note.sr_id = sr.sr_id and latest_note.rn = 1
                    left join (
                        -- Get the most recent status change for the primary work order
                        select wo_id,
                               ssc_insertdatetime,
                               row_number() over (partition by wo_id order by ssc_insertdatetime desc) as rn
                        from statussecondarychange
                    ) latest_status_change on latest_status_change.wo_id = wo.wo_id and latest_status_change.rn = 1
                    left join AttackPointStatus aps on (
                        CASE 
                            WHEN latest_status_change.ssc_insertdatetime IS NULL THEN 0
                            ELSE DATEDIFF(DAY, latest_status_change.ssc_insertdatetime, GETDATE())
                        END
                    ) >= aps.aps_daysinstatus
                    and aps.aps_id = (
                        select MAX(aps2.aps_id) 
                        from AttackPointStatus aps2 
                        where (
                            CASE 
                                WHEN latest_status_change.ssc_insertdatetime IS NULL THEN 0
                                ELSE DATEDIFF(DAY, latest_status_change.ssc_insertdatetime, GETDATE())
                            END
                        ) >= aps2.aps_daysinstatus
                    )
                    left join AttackPointNote apn on apn.apn_id = (
                        CASE 
                            WHEN latest_note.won_insertdatetime IS NULL THEN 1  -- NO NOTES case (apn_id = 1)
                            ELSE (
                                select TOP 1 apn2.apn_id 
                                from AttackPointNote apn2 
                                where apn2.apn_hours <= DATEDIFF(HOUR, latest_note.won_insertdatetime, GETDATE())
                                  AND apn2.apn_id > 1  -- Exclude NO NOTES record when there are actual notes
                                order by apn2.apn_hours desc
                            )
                        END
                    )
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
                       AttackPoints
                FROM ranked_results 
                WHERE rn <= @TopCount
                ORDER BY ISNULL(admin_u_id, -1), AttackPoints DESC";
            
            var parameters = new Dictionary<string, object>
            {
                { "@TopCount", topCount }
            };
            
            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAttackPoints",
                Detail = $"Retrieved attack points with top {topCount} results per admin",
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
                Description = "GetAttackPoints",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving attack points with top {TopCount} results", topCount);
            throw;
        }
    }
}
