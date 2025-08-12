using EvoAPI.Core.Interfaces;
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
}
