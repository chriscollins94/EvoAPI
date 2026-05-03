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

    public async Task<ConfigSettingDto?> GetConfigSettingAsync(string identifier)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string sql = @"
                SELECT TOP 1
                    cs_id,
                    cs_type,
                    cs_identifier,
                    cs_value,
                    cs_insertdatetime,
                    cs_modifieddatetime,
                    cs_description
                FROM ConfigSetting
                WHERE cs_identifier = @identifier";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.AddWithValue("@identifier", identifier);
                await connection.OpenAsync();
                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        stopwatch.Stop();
                        return new ConfigSettingDto
                        {
                            CsId = reader.GetInt32(0),
                            CsType = reader.IsDBNull(1) ? null : reader.GetString(1),
                            CsIdentifier = reader.GetString(2),
                            CsValue = reader.IsDBNull(3) ? null : reader.GetString(3),
                            CsInsertDateTime = reader.GetDateTime(4),
                            CsModifiedDateTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                            CsDescription = reader.IsDBNull(6) ? null : reader.GetString(6)
                        };
                    }
                }
            }

            stopwatch.Stop();
            return null;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error getting config setting {Identifier}", identifier);
            throw;
        }
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
                        p.p_color             AS PriorityColor,
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
                        ISNULL(CAST(sr.sr_quickbooks_docnumber AS varchar(20)), '') AS InvoiceNumber,
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
                        AND c.c_name NOT IN ('Metro Pipe Program 2')
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
                    PriorityColor,
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
                    ActionableNote,
                    InvoiceNumber
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
                        p.p_color             AS PriorityColor,
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
                    PriorityColor,
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
                    cc_attack as Attack,
                    cc_portalurl as PortalUrl,
                    cc_portalname as PortalName,
                    cc_portalcredentials as PortalCredentials
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
                    cc_portalurl = @PortalUrl,
                    cc_portalname = @PortalName,
                    cc_portalcredentials = @PortalCredentials,
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
                { "@Attack", request.Attack },
                { "@PortalUrl", request.PortalUrl ?? (object)DBNull.Value },
                { "@PortalName", request.PortalName ?? (object)DBNull.Value },
                { "@PortalCredentials", request.PortalCredentials ?? (object)DBNull.Value }
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
                (o_id, cc_name, cc_active, cc_tempid, cc_note, cc_attack, cc_portalurl, cc_portalname, cc_portalcredentials, cc_insertdatetime, cc_modifieddatetime)
                VALUES 
                (@OId, @Name, @Active, @TempId, @Note, @Attack, @PortalUrl, @PortalName, @PortalCredentials, GETDATE(), GETDATE());
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@OId", request.O_id },
                { "@Name", request.Name },
                { "@Active", request.Active },
                { "@TempId", (object)DBNull.Value },
                { "@Note", request.Note ?? (object)DBNull.Value },
                { "@Attack", request.Attack },
                { "@PortalUrl", request.PortalUrl ?? (object)DBNull.Value },
                { "@PortalName", request.PortalName ?? (object)DBNull.Value },
                { "@PortalCredentials", request.PortalCredentials ?? (object)DBNull.Value }
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
                    a.a_zip as Zip,
                    u.u_licensenumber as LicenseNumber,
                    u.u_licensestate as LicenseState,
                    u.u_licenseexpiration as LicenseExpiration
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
                    u.u_licensenumber as LicenseNumber,
                    u.u_licensestate as LicenseState,
                    u.u_licenseexpiration as LicenseExpiration,
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
                    AAddress1 = request.Address1,
                    AAddress2 = request.Address2,
                    ACity = request.City,
                    AState = request.State,
                    AZip = request.Zip,
                    AtId = 1
                };
                addressId = await CreateAddressAsync(addressRequest);
            }

            // Create the user record
            const string userSql = @"
                INSERT INTO dbo.[User] (
                    o_id, a_id, u_insertdatetime, u_username, u_password, u_firstname, u_lastname,
                    u_employeenumber, u_email, u_phonemobile, u_phonehome, u_phonedesk, u_extension,
                    u_active, u_directoryonly, u_daysavailablepto, u_daysavailablevacation, u_note, u_picture, z_id,
                    uc_id_shirt, uc_id_jacket, upw_id, upl_id,
                    u_licensenumber, u_licensestate, u_licenseexpiration
                )
                OUTPUT INSERTED.u_id
                VALUES (
                    1, @AddressId, GETDATE(), @Username, @Password, @FirstName, @LastName,
                    @EmployeeNumber, @Email, @PhoneMobile, @PhoneHome, @PhoneDesk, @Extension,
                    @Active, @DirectoryOnly, @DaysAvailablePTO, @DaysAvailableVacation, @Note, @Picture, @ZoneId,
                    @ShirtSizeId, @JacketSizeId, @PantsWaistId, @PantsLengthId,
                    @LicenseNumber, @LicenseState, @LicenseExpiration
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
            command.Parameters.AddWithValue("@DirectoryOnly", request.DirectoryOnly);
            command.Parameters.AddWithValue("@DaysAvailablePTO", request.DaysAvailablePTO ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@DaysAvailableVacation", request.DaysAvailableVacation ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Note", request.Note ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@Picture", request.Picture ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@ZoneId", request.ZoneId ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@ShirtSizeId", request.ShirtSizeId.HasValue && request.ShirtSizeId.Value > 0 ? (object)request.ShirtSizeId.Value : DBNull.Value);
            command.Parameters.AddWithValue("@JacketSizeId", request.JacketSizeId.HasValue && request.JacketSizeId.Value > 0 ? (object)request.JacketSizeId.Value : DBNull.Value);
            command.Parameters.AddWithValue("@PantsWaistId", request.PantsWaistId.HasValue && request.PantsWaistId.Value > 0 ? (object)request.PantsWaistId.Value : DBNull.Value);
            command.Parameters.AddWithValue("@PantsLengthId", request.PantsLengthId.HasValue && request.PantsLengthId.Value > 0 ? (object)request.PantsLengthId.Value : DBNull.Value);
            command.Parameters.AddWithValue("@LicenseNumber", request.LicenseNumber ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@LicenseState", request.LicenseState ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@LicenseExpiration", request.LicenseExpiration.HasValue ? (object)request.LicenseExpiration.Value : DBNull.Value);

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
                        AAddress1 = request.Address1,
                        AAddress2 = request.Address2,
                        ACity = request.City,
                        AState = request.State,
                        AZip = request.Zip,
                        AtId = 1
                    };
                    await UpdateEmployeeAddressAsync(addressId.Value, addressRequest);
                }
                else
                {
                    var addressRequest = new CreateAddressRequest
                    {
                        AAddress1 = request.Address1,
                        AAddress2 = request.Address2,
                        ACity = request.City,
                        AState = request.State,
                        AZip = request.Zip,
                        AtId = 1
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
                    u_directoryonly = @DirectoryOnly,
                    u_daysavailablepto = @DaysAvailablePTO,
                    u_daysavailablevacation = @DaysAvailableVacation,
                    u_note = @Note,
                    u_picture = @Picture,
                    z_id = @ZoneId,
                    uc_id_shirt = @ShirtSizeId,
                    uc_id_jacket = @JacketSizeId,
                    upw_id = @PantsWaistId,
                    upl_id = @PantsLengthId,
                    u_licensenumber = @LicenseNumber,
                    u_licensestate = @LicenseState,
                    u_licenseexpiration = @LicenseExpiration";

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
            command.Parameters.AddWithValue("@DirectoryOnly", request.DirectoryOnly);
            command.Parameters.AddWithValue("@DaysAvailablePTO", request.DaysAvailablePTO ?? 0);
            command.Parameters.AddWithValue("@DaysAvailableVacation", request.DaysAvailableVacation ?? 0);
            command.Parameters.AddWithValue("@Note", request.Note ?? "");
            command.Parameters.AddWithValue("@Picture", request.Picture ?? "");
            command.Parameters.AddWithValue("@ZoneId", request.ZoneId ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@ShirtSizeId", request.ShirtSizeId.HasValue && request.ShirtSizeId.Value > 0 ? request.ShirtSizeId.Value : (object)DBNull.Value);
            command.Parameters.AddWithValue("@JacketSizeId", request.JacketSizeId.HasValue && request.JacketSizeId.Value > 0 ? request.JacketSizeId.Value : (object)DBNull.Value);
            command.Parameters.AddWithValue("@PantsWaistId", request.PantsWaistId.HasValue && request.PantsWaistId.Value > 0 ? request.PantsWaistId.Value : (object)DBNull.Value);
            command.Parameters.AddWithValue("@PantsLengthId", request.PantsLengthId.HasValue && request.PantsLengthId.Value > 0 ? request.PantsLengthId.Value : (object)DBNull.Value);
            command.Parameters.AddWithValue("@LicenseNumber", request.LicenseNumber ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@LicenseState", request.LicenseState ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@LicenseExpiration", request.LicenseExpiration.HasValue ? (object)request.LicenseExpiration.Value : DBNull.Value);

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
                INSERT INTO address (
                    o_id, at_id, a_insertdatetime, a_address1, a_address2, a_city, a_state, a_zip, a_active
                )
                OUTPUT INSERTED.a_id
                VALUES (
                    1, @AtId, GETDATE(), @AAddress1, @AAddress2, @ACity, @AState, @AZip, 1
                )";

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            
            using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@AtId", request.AtId);
            command.Parameters.AddWithValue("@AAddress1", request.AAddress1 ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@AAddress2", request.AAddress2 ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@ACity", request.ACity ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@AState", request.AState ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@AZip", request.AZip ?? (object)DBNull.Value);

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

    // Simple update for employee address management (returns bool)
    private async Task<bool> UpdateEmployeeAddressAsync(int addressId, UpdateAddressRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE address 
                SET 
                    at_id = @AtId,
                    a_modifieddatetime = GETDATE(),
                    a_address1 = @AAddress1,
                    a_address2 = @AAddress2,
                    a_city = @ACity,
                    a_state = @AState,
                    a_zip = @AZip
                WHERE a_id = @AddressId";

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            
            using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@AddressId", addressId);
            command.Parameters.AddWithValue("@AtId", request.AtId);
            command.Parameters.AddWithValue("@AAddress1", request.AAddress1 ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@AAddress2", request.AAddress2 ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@ACity", request.ACity ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@AState", request.AState ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@AZip", request.AZip ?? (object)DBNull.Value);

            var rowsAffected = await command.ExecuteNonQueryAsync();
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateEmployeeAddress",
                Detail = $"Updated employee address with ID {addressId}",
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
                Description = "UpdateEmployeeAddress",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating employee address with ID {AddressId}", addressId);
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
                    u.u_directoryonly as DirectoryOnly,
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
                    u.u_licensenumber as LicenseNumber,
                    u.u_licensestate as LicenseState,
                    u.u_licenseexpiration as LicenseExpiration,
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
                    u.u_directoryonly as DirectoryOnly,
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
                    -- Clothing Size IDs and Text
                    u.uc_id_shirt as ShirtSizeId,
                    u.uc_id_jacket as JacketSizeId,
                    uc_shirt.uc_clothingsize as ShirtSize,
                    uc_jacket.uc_clothingsize as JacketSize,
                    -- Pants Size IDs and Text
                    u.upw_id as PantsWaistId,
                    u.upl_id as PantsLengthId,
                    upw.upw_size as PantsWaistSize,
                    upl.upl_size as PantsLengthSize,
                    -- License Information
                    u.u_licensenumber as LicenseNumber,
                    u.u_licensestate as LicenseState,
                    u.u_licenseexpiration as LicenseExpiration,
                    -- Role information (nullable since LEFT JOIN)
                    xur.r_id as RoleId,
                    r.r_role as RoleName,
                    r.r_description as RoleDescription,
                    -- Trade General information (nullable since LEFT JOIN)
                    xutg.xutg_id as UserTradeGeneralId,
                    xutg.tg_id as TradeGeneralId,
                    tg.tg_trade as Trade,
                    tg.tg_type as TradeType,
                    -- Facility Manager flags
                    CASE WHEN zfm.z_id IS NOT NULL THEN 1 ELSE 0 END as IsZoneFacilityManager,
                    CASE WHEN rfm.reg_id IS NOT NULL THEN 1 ELSE 0 END as IsRegionFacilityManager
                FROM dbo.[User] u
                LEFT JOIN dbo.Zone z ON u.z_id = z.z_id
                LEFT JOIN dbo.Address a ON u.a_id = a.a_id
                LEFT JOIN dbo.userclothing uc_shirt ON u.uc_id_shirt = uc_shirt.uc_id
                LEFT JOIN dbo.userclothing uc_jacket ON u.uc_id_jacket = uc_jacket.uc_id
                LEFT JOIN dbo.UserPantsWaist upw ON u.upw_id = upw.upw_id
                LEFT JOIN dbo.UserPantsLength upl ON u.upl_id = upl.upl_id
                LEFT JOIN dbo.XRefUserRole xur ON u.u_id = xur.u_id
                LEFT JOIN dbo.Role r ON xur.r_id = r.r_id
                LEFT JOIN dbo.xrefUserTradeGeneral xutg ON u.u_id = xutg.u_id
                LEFT JOIN dbo.TradeGeneral tg ON xutg.tg_id = tg.tg_id
                -- Check if this user manages any zone
                LEFT JOIN dbo.Zone zfm ON u.u_id = zfm.u_id
                -- Check if this user manages any region
                LEFT JOIN dbo.Region rfm ON u.u_id = rfm.u_id
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

    public async Task<DataTable> GetBacklogItemsAsync(bool includeInactive = false)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var sql = $@"
                {GetBacklogSelectSql()}
                WHERE (@includeInactive = 1 OR bi.bi_active = 1)
                ORDER BY
                    CASE bi.bi_priority
                        WHEN 'Critical' THEN 1
                        WHEN 'High' THEN 2
                        WHEN 'Medium' THEN 3
                        WHEN 'Low' THEN 4
                        ELSE 5
                    END,
                    bi.bi_date DESC,
                    bi.bi_id DESC;";

            var result = await ExecuteQueryAsync(sql, new Dictionary<string, object>
            {
                { "@includeInactive", includeInactive ? 1 : 0 }
            });

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetBacklogItems",
                Detail = $"Retrieved backlog items. IncludeInactive: {includeInactive}",
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
                Description = "GetBacklogItems",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            _logger.LogError(ex, "Failed to retrieve backlog items");
            throw;
        }
    }

    public async Task<DataTable> GetBacklogItemByIdAsync(int backlogItemId)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var sql = $@"
                {GetBacklogSelectSql()}
                WHERE bi.bi_id = @backlogItemId;";

            var result = await ExecuteQueryAsync(sql, new Dictionary<string, object>
            {
                { "@backlogItemId", backlogItemId }
            });

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetBacklogItemById",
                Detail = $"Retrieved backlog item {backlogItemId}",
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
                Description = "GetBacklogItemById",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            _logger.LogError(ex, "Failed to retrieve backlog item {BacklogItemId}", backlogItemId);
            throw;
        }
    }

    public async Task<int?> CreateBacklogItemAsync(CreateBacklogItemRequest request, int userId, string username)
    {
        var stopwatch = Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");

        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            var history = AppendHistoryEntry(null, username, new[] { "Item created." });

            const string sql = @"
                INSERT INTO backlogitem
                (
                    bi_code,
                    bi_version,
                    bi_title,
                    bi_source,
                    bi_author,
                    bi_date,
                    bi_category,
                    bi_type,
                    bi_priority,
                    bi_status,
                    bi_effort,
                    bi_desc,
                    bi_notes,
                    bi_detail_markdown,
                    bi_history,
                    bi_active,
                    bi_u_id_createdby,
                    bi_u_id_lastupdatedby,
                    bi_insertdatetime,
                    bi_lastupdated
                )
                VALUES
                (
                    @Code,
                    @Version,
                    @Title,
                    @Source,
                    @Author,
                    @Date,
                    @Category,
                    @Type,
                    @Priority,
                    @Status,
                    @Effort,
                    @Desc,
                    @Notes,
                    @DetailMarkdown,
                    @History,
                    @Active,
                    @CreatedByUserId,
                    @LastUpdatedByUserId,
                    GETUTCDATE(),
                    GETUTCDATE()
                );

                SELECT CAST(SCOPE_IDENTITY() AS INT);";

            using var connection = new SqlConnection(connectionString);
            using var command = new SqlCommand(sql, connection);

            command.Parameters.AddWithValue("@Code", request.Code.Trim());
            command.Parameters.AddWithValue("@Version", ToDbValue(request.Version));
            command.Parameters.AddWithValue("@Title", request.Title.Trim());
            command.Parameters.AddWithValue("@Source", ToDbValue(request.Source));
            command.Parameters.AddWithValue("@Author", ToDbValue(request.Author));
            command.Parameters.AddWithValue("@Date", ToDbDateValue(request.Date));
            command.Parameters.AddWithValue("@Category", ToDbValue(request.Category));
            command.Parameters.AddWithValue("@Type", string.IsNullOrWhiteSpace(request.Type) ? "Feature" : request.Type.Trim());
            command.Parameters.AddWithValue("@Priority", string.IsNullOrWhiteSpace(request.Priority) ? "Medium" : request.Priority.Trim());
            command.Parameters.AddWithValue("@Status", string.IsNullOrWhiteSpace(request.Status) ? "New" : request.Status.Trim());
            command.Parameters.AddWithValue("@Effort", ToDbValue(request.Effort));
            command.Parameters.AddWithValue("@Desc", ToDbValue(request.Desc));
            command.Parameters.AddWithValue("@Notes", ToDbValue(request.Notes));
            command.Parameters.AddWithValue("@DetailMarkdown", ToDbValue(request.DetailMarkdown));
            command.Parameters.AddWithValue("@History", history);
            command.Parameters.AddWithValue("@Active", request.Active);
            command.Parameters.AddWithValue("@CreatedByUserId", userId > 0 ? userId : DBNull.Value);
            command.Parameters.AddWithValue("@LastUpdatedByUserId", userId > 0 ? userId : DBNull.Value);

            await connection.OpenAsync();
            var result = await command.ExecuteScalarAsync();

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateBacklogItem",
                Detail = $"Created backlog item '{request.Code} - {request.Title}'",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return result == null || result == DBNull.Value ? null : Convert.ToInt32(result);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateBacklogItem",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            _logger.LogError(ex, "Failed to create backlog item {Code} - {Title}", request.Code, request.Title);
            throw;
        }
    }

    public async Task<bool> UpdateBacklogItemAsync(int backlogItemId, UpdateBacklogItemRequest request, int userId, string username)
    {
        var stopwatch = Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");

        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            var existingData = await GetBacklogItemByIdAsync(backlogItemId);
            if (existingData.Rows.Count == 0)
            {
                return false;
            }

            var row = existingData.Rows[0];
            var changes = new List<string>();

            AddChange(changes, "Code", row["code"]?.ToString(), request.Code);
            AddChange(changes, "Version", row["version"]?.ToString(), request.Version);
            AddChange(changes, "Title", row["title"]?.ToString(), request.Title);
            AddChange(changes, "Source", row["source"]?.ToString(), request.Source);
            AddChange(changes, "Author", row["author"]?.ToString(), request.Author);
            AddChange(changes, "Date", row["date"]?.ToString(), request.Date);
            AddChange(changes, "Category", row["category"]?.ToString(), request.Category);
            AddChange(changes, "Type", row["type"]?.ToString(), request.Type);
            AddChange(changes, "Priority", row["priority"]?.ToString(), request.Priority);
            AddChange(changes, "Status", row["status"]?.ToString(), request.Status);
            AddChange(changes, "Effort", row["effort"]?.ToString(), request.Effort);
            AddChange(changes, "Summary", row["desc"]?.ToString(), request.Desc);
            AddChange(changes, "Notes", row["notes"]?.ToString(), request.Notes);
            AddChange(changes, "Detail", row["detailMarkdown"]?.ToString(), request.DetailMarkdown);

            var existingActive = row["active"] != DBNull.Value && (row["active"]?.ToString() == "True" || row["active"]?.ToString() == "1");
            if (existingActive != request.Active)
            {
                changes.Add($"Active changed: {existingActive} -> {request.Active}");
            }

            var history = row["history"] == DBNull.Value ? null : row["history"]?.ToString();
            if (changes.Count > 0)
            {
                history = AppendHistoryEntry(history, username, changes);
            }

            const string sql = @"
                UPDATE backlogitem
                SET
                    bi_code = @Code,
                    bi_version = @Version,
                    bi_title = @Title,
                    bi_source = @Source,
                    bi_author = @Author,
                    bi_date = @Date,
                    bi_category = @Category,
                    bi_type = @Type,
                    bi_priority = @Priority,
                    bi_status = @Status,
                    bi_effort = @Effort,
                    bi_desc = @Desc,
                    bi_notes = @Notes,
                    bi_detail_markdown = @DetailMarkdown,
                    bi_history = @History,
                    bi_active = @Active,
                    bi_u_id_lastupdatedby = @LastUpdatedByUserId,
                    bi_lastupdated = GETUTCDATE()
                WHERE bi_id = @BacklogItemId;";

            using var connection = new SqlConnection(connectionString);
            using var command = new SqlCommand(sql, connection);

            command.Parameters.AddWithValue("@BacklogItemId", backlogItemId);
            command.Parameters.AddWithValue("@Code", request.Code.Trim());
            command.Parameters.AddWithValue("@Version", ToDbValue(request.Version));
            command.Parameters.AddWithValue("@Title", request.Title.Trim());
            command.Parameters.AddWithValue("@Source", ToDbValue(request.Source));
            command.Parameters.AddWithValue("@Author", ToDbValue(request.Author));
            command.Parameters.AddWithValue("@Date", ToDbDateValue(request.Date));
            command.Parameters.AddWithValue("@Category", ToDbValue(request.Category));
            command.Parameters.AddWithValue("@Type", string.IsNullOrWhiteSpace(request.Type) ? "Feature" : request.Type.Trim());
            command.Parameters.AddWithValue("@Priority", string.IsNullOrWhiteSpace(request.Priority) ? "Medium" : request.Priority.Trim());
            command.Parameters.AddWithValue("@Status", string.IsNullOrWhiteSpace(request.Status) ? "New" : request.Status.Trim());
            command.Parameters.AddWithValue("@Effort", ToDbValue(request.Effort));
            command.Parameters.AddWithValue("@Desc", ToDbValue(request.Desc));
            command.Parameters.AddWithValue("@Notes", ToDbValue(request.Notes));
            command.Parameters.AddWithValue("@DetailMarkdown", ToDbValue(request.DetailMarkdown));
            command.Parameters.AddWithValue("@History", ToDbValue(history));
            command.Parameters.AddWithValue("@Active", request.Active);
            command.Parameters.AddWithValue("@LastUpdatedByUserId", userId > 0 ? userId : DBNull.Value);

            await connection.OpenAsync();
            var rowsAffected = await command.ExecuteNonQueryAsync();

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateBacklogItem",
                Detail = $"Updated backlog item {backlogItemId}",
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
                Description = "UpdateBacklogItem",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            _logger.LogError(ex, "Failed to update backlog item {BacklogItemId}", backlogItemId);
            throw;
        }
    }

    public async Task<bool> DeleteBacklogItemAsync(int backlogItemId, int userId, string username)
    {
        var stopwatch = Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");

        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            var existingData = await GetBacklogItemByIdAsync(backlogItemId);
            if (existingData.Rows.Count == 0)
            {
                return false;
            }

            var row = existingData.Rows[0];
            var history = AppendHistoryEntry(row["history"] == DBNull.Value ? null : row["history"]?.ToString(), username, new[] { "Item archived." });

            const string sql = @"
                UPDATE backlogitem
                SET
                    bi_active = 0,
                    bi_history = @History,
                    bi_u_id_lastupdatedby = @LastUpdatedByUserId,
                    bi_lastupdated = GETUTCDATE()
                WHERE bi_id = @BacklogItemId;";

            using var connection = new SqlConnection(connectionString);
            using var command = new SqlCommand(sql, connection);

            command.Parameters.AddWithValue("@BacklogItemId", backlogItemId);
            command.Parameters.AddWithValue("@History", history);
            command.Parameters.AddWithValue("@LastUpdatedByUserId", userId > 0 ? userId : DBNull.Value);

            await connection.OpenAsync();
            var rowsAffected = await command.ExecuteNonQueryAsync();

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "DeleteBacklogItem",
                Detail = $"Archived backlog item {backlogItemId}",
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
                Description = "DeleteBacklogItem",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            _logger.LogError(ex, "Failed to archive backlog item {BacklogItemId}", backlogItemId);
            throw;
        }
    }

    private static string GetBacklogSelectSql()
    {
        return @"
            SELECT
                bi.bi_id AS backlogItemId,
                bi.bi_code AS code,
                bi.bi_version AS version,
                bi.bi_title AS title,
                bi.bi_source AS source,
                bi.bi_author AS author,
                CASE WHEN bi.bi_date IS NULL THEN NULL ELSE CONVERT(VARCHAR(10), bi.bi_date, 23) END AS [date],
                bi.bi_category AS category,
                bi.bi_type AS [type],
                bi.bi_priority AS priority,
                bi.bi_status AS status,
                bi.bi_effort AS effort,
                bi.bi_desc AS [desc],
                bi.bi_notes AS notes,
                bi.bi_detail_markdown AS detailMarkdown,
                bi.bi_history AS history,
                bi.bi_active AS active,
                CONVERT(VARCHAR(16), bi.bi_insertdatetime, 120) AS insertDateTime,
                CASE WHEN bi.bi_lastupdated IS NULL THEN NULL ELSE CONVERT(VARCHAR(16), bi.bi_lastupdated, 120) END AS lastUpdated,
                bi.bi_u_id_lastupdatedby AS lastUpdatedByUserId
            FROM backlogitem bi";
    }

    private static object ToDbValue(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? DBNull.Value : value.Trim();
    }

    private static object ToDbDateValue(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return DBNull.Value;
        }

        return DateTime.TryParse(value, out var parsedDate)
            ? parsedDate.Date
            : DBNull.Value;
    }

    private static string AppendHistoryEntry(string? existingHistory, string username, IEnumerable<string> lines)
    {
        var historyLines = lines.Where(line => !string.IsNullOrWhiteSpace(line)).ToList();
        if (historyLines.Count == 0)
        {
            return existingHistory ?? string.Empty;
        }

        var entry = $"## {DateTime.UtcNow:yyyy-MM-dd HH:mm} UTC - {username}\n{string.Join("\n", historyLines.Select(line => $"- {line.Trim()}"))}";

        if (string.IsNullOrWhiteSpace(existingHistory))
        {
            return entry;
        }

        return $"{existingHistory.Trim()}\n\n{entry}";
    }

    private static void AddChange(List<string> changes, string fieldName, string? oldValue, string? newValue)
    {
        var existingValue = string.IsNullOrWhiteSpace(oldValue) ? "(empty)" : oldValue.Trim();
        var updatedValue = string.IsNullOrWhiteSpace(newValue) ? "(empty)" : newValue.Trim();

        if (!string.Equals(existingValue, updatedValue, StringComparison.Ordinal))
        {
            changes.Add($"{fieldName} changed: {existingValue} -> {updatedValue}");
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

            -- Residential config
            DECLARE @ResidentialZoneLabel VARCHAR(50) = 'Residential';
            DECLARE @ResidentialAdminUID INT = 45;
            DECLARE @ResidentialCallCenterName VARCHAR(50) = 'Residential';

            -- Create temp tables with proper filtering
            IF OBJECT_ID('tempdb..#BaseData') IS NOT NULL DROP TABLE #BaseData;
            IF OBJECT_ID('tempdb..#WorkOrderNotes') IS NOT NULL DROP TABLE #WorkOrderNotes;
            IF OBJECT_ID('tempdb..#StatusChanges') IS NOT NULL DROP TABLE #StatusChanges;

            -- Base set: ALL qualifying work orders
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
                -- Original zone-based results
                SELECT sr.sr_id, 
                    sr.sr_insertdatetime, 
                    sr.sr_totaldue,
                    sr.sr_requestnumber,
                    sr.sr_datenextstep,
                    sr.sr_actionablenote,
                    sr.sr_escalated,
                    wo.wo_startdatetime,
                    z.z_number + '-' + z.z_acronym AS zone, 
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
                    CASE WHEN sr.sr_escalated IS NOT NULL THEN 1 ELSE 0 END as is_escalated,
                    0 as is_residential
                FROM servicerequest sr WITH (NOLOCK)
                INNER JOIN workorder wo WITH (NOLOCK) ON sr.wo_id_primary = wo.wo_id
                INNER JOIN #BaseData bd ON wo.wo_id = bd.wo_id
                INNER JOIN xrefCompanyCallCenter xccc WITH (NOLOCK) ON sr.xccc_id = xccc.xccc_id
                INNER JOIN Company c WITH (NOLOCK) ON xccc.c_id = c.c_id
                INNER JOIN callcenter cc WITH (NOLOCK) ON xccc.cc_id = cc.cc_id
                CROSS APPLY (
                    SELECT TOP 1 xwou.u_id 
                    FROM xrefWorkOrderUser xwou WITH (NOLOCK)
                    WHERE xwou.wo_id = wo.wo_id
                    ORDER BY xwou.xwou_id ASC
                ) pt
                INNER JOIN [user] u WITH (NOLOCK) ON pt.u_id = u.u_id
                INNER JOIN zone z WITH (NOLOCK) ON u.z_id = z.z_id
                INNER JOIN statussecondary ss WITH (NOLOCK) ON wo.ss_id = ss.ss_id
                INNER JOIN Priority p WITH (NOLOCK) ON sr.p_id = p.p_id
                LEFT JOIN trade t WITH (NOLOCK) ON sr.t_id = t.t_id
                INNER JOIN xrefadminzonestatussecondary xazss WITH (NOLOCK) ON z.z_id = xazss.z_id AND ss.ss_id = xazss.ss_id
                INNER JOIN [user] admin_user WITH (NOLOCK) ON xazss.u_id = admin_user.u_id
                LEFT JOIN #WorkOrderNotes won ON won.wo_id = wo.wo_id
                LEFT JOIN #StatusChanges ssc ON ssc.wo_id = wo.wo_id
                WHERE sr.s_id NOT IN (9, 6)
                AND c.c_name NOT IN ('Metro Pipe Program')

                UNION ALL

                -- Residential results (hardcoded admin & zone label)
                SELECT sr.sr_id, 
                    sr.sr_insertdatetime, 
                    sr.sr_totaldue,
                    sr.sr_requestnumber,
                    sr.sr_datenextstep,
                    sr.sr_actionablenote,
                    sr.sr_escalated,
                    wo.wo_startdatetime,
                    @ResidentialZoneLabel AS zone, 
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
                    res_admin.u_id as admin_u_id,
                    res_admin.u_firstname as admin_firstname,
                    res_admin.u_lastname as admin_lastname,
                    CASE WHEN sr.sr_escalated IS NOT NULL THEN 1 ELSE 0 END as is_escalated,
                    1 as is_residential
                FROM servicerequest sr WITH (NOLOCK)
                INNER JOIN workorder wo WITH (NOLOCK) ON sr.wo_id_primary = wo.wo_id
                INNER JOIN #BaseData bd ON wo.wo_id = bd.wo_id
                INNER JOIN xrefCompanyCallCenter xccc WITH (NOLOCK) ON sr.xccc_id = xccc.xccc_id
                INNER JOIN Company c WITH (NOLOCK) ON xccc.c_id = c.c_id
                INNER JOIN callcenter cc WITH (NOLOCK) ON xccc.cc_id = cc.cc_id
                INNER JOIN statussecondary ss WITH (NOLOCK) ON wo.ss_id = ss.ss_id
                INNER JOIN Priority p WITH (NOLOCK) ON sr.p_id = p.p_id
                LEFT JOIN trade t WITH (NOLOCK) ON sr.t_id = t.t_id
                INNER JOIN [user] res_admin WITH (NOLOCK) ON res_admin.u_id = @ResidentialAdminUID
                LEFT JOIN #WorkOrderNotes won ON won.wo_id = wo.wo_id
                LEFT JOIN #StatusChanges ssc ON ssc.wo_id = wo.wo_id
                WHERE sr.s_id NOT IN (9, 6)
                AND c.c_name NOT IN ('Metro Pipe Program')
                AND cc.cc_name = @ResidentialCallCenterName
                AND ss.ss_id IN (SELECT DISTINCT ss_id FROM xrefadminzonestatussecondary WITH (NOLOCK))
            ),
            final_with_attack_points AS (
                SELECT *,
                    (AttackPriority + AttackStatusSecondary + AttackDaysInStatus + 
                    AttackHoursSinceLastNote + AttackCallCenter + AttackActionableDate) as AttackPoints,
                    CASE 
                        WHEN is_escalated = 1 THEN NULL
                        WHEN cc_name = 'Administrative' THEN NULL
                        ELSE ROW_NUMBER() OVER (
                            PARTITION BY admin_u_id, CASE WHEN is_residential = 1 THEN zone ELSE '' END
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
                is_escalated,
                is_residential
            FROM final_with_attack_points
            WHERE ((rn_non_escalated <= @TopCount) OR (is_escalated = 1))
            AND NOT (is_residential = 0 AND cc_name = @ResidentialCallCenterName)
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

    public async Task<DataTable> GetReceiptsDashboardAsync(int? days = null)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var dateFilter = (days.HasValue && days.Value > 0)
                ? $"AND att.att_insertdatetime >= DATEADD(day, -{days.Value}, GETDATE())"
                : string.Empty;

            var sql = $@"

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
                {dateFilter}
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

    public async Task<DataTable> GetServiceRequestReportAsync(DateTime startDate, DateTime endDate)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            const string sql = @"
select 
       cc.cc_name 'Call Center', cc.cc_portalname, cc.cc_portalurl, cc.cc_portalcredentials
       , c.c_name Company, c.c_portalname, c.c_portalurl, c.c_portalcredentials
       , t_parent.t_trade 'Parent Trade', t.t_trade Trade,
       sr.sr_requestnumber 'Service Request #', sr.sr_insertdatetime Created,

       u.u_firstname + ' ' + u.u_lastname 'Primary Tech',
       ISNULL((
           select STRING_AGG(u2.u_firstname + ' ' + u2.u_lastname, ', ') 
           from workorder wo2 with(nolock)
           join xrefworkorderuser xwou2 with(nolock) on xwou2.wo_id = wo2.wo_id
           join [user] u2 with(nolock) on u2.u_id = xwou2.u_id
           where wo2.sr_id = sr.sr_id
             and u2.u_id <> xwou.u_id
       ), '') 'Additional Techs',

       wo.wo_startdatetime 'Primary WO Start', wo.wo_enddatetime 'Primary WO End',
       sr.sr_quickbooks_docnumber 'Invoice Number', s.s_status 'Status', sr.sr_totaldue 'Total Due',
       REPLACE(REPLACE(sr.sr_summaryworkcompleted, CHAR(13), ''), CHAR(10), '') 'Summary of Work Completed',
       l.l_location, a.a_address1, a.a_city, a.a_state, a.a_zip,
       wonote.won_user 'Note Created By', wonote.won_insertdatetime 'Note Created',
       REPLACE(REPLACE(REPLACE(wonote.won_note, CHAR(13), ''), CHAR(10), ''), char(9), '') 'Most Recent Note',

       ISNULL((
           select COUNT(distinct xwosi.si_id)
           from workorder wo2 with(nolock)
           join xrefworkorderserviceitem xwosi with(nolock) on xwosi.wo_id = wo2.wo_id
           where wo2.sr_id = sr.sr_id
       ), 0) 'Service Item Count',

       ISNULL((
           select STRING_AGG(si.si_name, ' | ')
           from (
               select distinct xwosi.si_id
               from workorder wo2 with(nolock)
               join xrefworkorderserviceitem xwosi with(nolock) on xwosi.wo_id = wo2.wo_id
               where wo2.sr_id = sr.sr_id
           ) si_distinct
           join serviceitem si with(nolock) on si.si_id = si_distinct.si_id
       ), '') 'Service Items'

from status s, trade t, trade t_parent, servicerequest sr with(nolock),
     xrefcompanycallcenter xccc, location l with(nolock), callcenter cc, company c,
     address a with(nolock), xrefworkorderuser xwou with(nolock), [user] u with(nolock), workorder wo with(nolock)

left join (
    select max(won_id) as maxid, wo_id 
    from workordernote 
    where won_user <> 'System Generated' 
    group by wo_id
) as won on wo.wo_id = won.wo_id
left join workordernote wonote with(nolock) on wonote.won_id = won.maxid

where 1=1
and sr.sr_insertdatetime >= @startDate
and sr.sr_insertdatetime < @endDate
and sr.t_id = t.t_id
and sr.s_id = s.s_id
and sr.wo_id_primary = wo.wo_id
and wo.sr_id = sr.sr_id
and sr.xccc_id = xccc.xccc_id
and xccc.cc_id = cc.cc_id
and xccc.c_id = c.c_id
and sr.l_id = l.l_id
and l.a_id = a.a_id
and wo.wo_id = xwou.wo_id
and u.u_id = xwou.u_id
and t.t_id_parent = t_parent.t_id

order by sr.sr_insertdatetime
";

            var parameters = new Dictionary<string, object>
            {
                { "@startDate", startDate },
                { "@endDate", endDate }
            };

            var result = await ExecuteQueryAsync(sql, parameters);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetServiceRequestReport",
                Detail = $"Retrieved {result.Rows.Count} service request records ({startDate:yyyy-MM-dd} to {endDate:yyyy-MM-dd})",
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
                Description = "GetServiceRequestReport",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            _logger.LogError(ex, "Error retrieving service request report data");
            throw;
        }
    }

    public async Task<DataTable> GetTechReceiptsDashboardAsync(int userId)
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
                AND att.u_id_submittedby = @UserId
                ORDER BY att_id desc
";

            var parameters = new Dictionary<string, object> { { "@UserId", userId } };
            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTechReceiptsDashboard",
                Detail = $"Retrieved {result.Rows.Count} receipt records for user {userId}",
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
                Description = "GetTechReceiptsDashboard",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving tech receipts dashboard data");
            throw;
        }
    }

    public async Task<DataTable> GetTechDetailDashboardAsync(int? userId = null)
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
                -- First, determine if user is Regional Facility Manager or Zone Facility Manager
                -- RFM takes priority over ZFM
                DECLARE @ManagedZones TABLE (z_id INT);
                DECLARE @IsRFM BIT = 0;
                
                IF @UserId IS NOT NULL
                BEGIN
                    -- Check if user is Regional Facility Manager
                    IF EXISTS (SELECT 1 FROM region WHERE u_id = @UserId)
                    BEGIN
                        SET @IsRFM = 1;
                        -- Get all zones in the user's managed region(s)
                        INSERT INTO @ManagedZones (z_id)
                        SELECT z.z_id 
                        FROM zone z
                        INNER JOIN region r ON z.reg_id = r.reg_id
                        WHERE r.u_id = @UserId;
                    END
                    ELSE
                    BEGIN
                        -- User is not RFM, check if they're Zone Facility Manager
                        INSERT INTO @ManagedZones (z_id)
                        SELECT z_id FROM zone WHERE u_id = @UserId;
                    END
                END;

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
                    WHERE u.u_active = 1 and u.u_id not in (43)
                    -- Filter by managed zones if user is a Zone Facility Manager
                    AND (
                        NOT EXISTS (SELECT 1 FROM @ManagedZones)  -- No managed zones = show all
                        OR u.z_id IN (SELECT z_id FROM @ManagedZones)  -- Has managed zones = filter by them
                    )
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
                ORDER BY z.z_number, rp.u_lastname, rp.u_firstname;
            ";

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand(sql, connection))
                {
                    command.CommandTimeout = 60;
                    
                    // Add userId parameter
                    if (userId.HasValue)
                    {
                        command.Parameters.AddWithValue("@UserId", userId.Value);
                    }
                    else
                    {
                        command.Parameters.AddWithValue("@UserId", DBNull.Value);
                    }
                    
                    var adapter = new SqlDataAdapter(command);
                    var dataTable = new DataTable();
                    adapter.Fill(dataTable);
                    
                    stopwatch.Stop();
                    await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                    {
                        Name = "DataService",
                        Description = "GetTechDetailDashboard",
                        Detail = $"Retrieved {dataTable.Rows.Count} tech detail records (userId: {(userId.HasValue ? userId.Value.ToString() : "all")})",
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

    public async Task<DataTable> GetTechActivityDashboardAsync(DateTime? startDate = null, DateTime? endDate = null, int? userId = null)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            
            // Default to 90 days if no dates provided
            var effectiveStartDate = startDate ?? DateTime.Now.AddDays(-90);
            var effectiveEndDate = endDate ?? DateTime.Now;
            
            var sql = @"
                -- Determine if user is Regional Facility Manager or Zone Facility Manager
                DECLARE @ManagedZones TABLE (z_id INT);
                DECLARE @IsRFM BIT = 0;
                
                IF @UserId IS NOT NULL
                BEGIN
                    -- Check if user is Regional Facility Manager
                    IF EXISTS (SELECT 1 FROM region WHERE u_id = @UserId)
                    BEGIN
                        SET @IsRFM = 1;
                        -- Get all zones in the user's managed region(s)
                        INSERT INTO @ManagedZones (z_id)
                        SELECT z.z_id 
                        FROM zone z
                        INNER JOIN region r ON z.reg_id = r.reg_id
                        WHERE r.u_id = @UserId;
                    END
                    ELSE
                    BEGIN
                        -- User is not RFM, check if they're Zone Facility Manager
                        INSERT INTO @ManagedZones (z_id)
                        SELECT z_id FROM zone WHERE u_id = @UserId;
                    END
                END;

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
                    -- Filter by managed zones if user is a ZFM or RFM
                    AND (
                        NOT EXISTS (SELECT 1 FROM @ManagedZones)  -- No managed zones = show all
                        OR u.z_id IN (SELECT z_id FROM @ManagedZones)  -- Has managed zones = filter by them
                    )
                ORDER BY tt.tt_id DESC;
            ";

            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new SqlCommand(sql, connection))
                {
                    command.Parameters.AddWithValue("@StartDate", effectiveStartDate);
                    command.Parameters.AddWithValue("@EndDate", effectiveEndDate);
                    
                    // Add userId parameter
                    if (userId.HasValue)
                    {
                        command.Parameters.AddWithValue("@UserId", userId.Value);
                    }
                    else
                    {
                        command.Parameters.AddWithValue("@UserId", DBNull.Value);
                    }
                    
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

    public async Task<DataTable> GetAttachmentsByCallCenterAsync(int ccId)
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
                    att_extension,
                    cc_id
                FROM attachment 
                WHERE cc_id = @cc_id
                ORDER BY att_insertdatetime DESC";

            var parameters = new Dictionary<string, object>
            {
                ["@cc_id"] = ccId
            };

            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAttachmentsByCallCenter",
                Detail = $"Retrieved {result.Rows.Count} attachments for call center {ccId}",
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
                Description = "GetAttachmentsByCallCenter",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving attachments for call center {CcId}", ccId);
            throw;
        }
    }

    public async Task<bool> UpdateAttachmentDescriptionAsync(int attId, string description)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE attachment 
                SET att_description = @description, att_modifieddatetime = GETUTCDATE()
                WHERE att_id = @att_id";

            var parameters = new Dictionary<string, object>
            {
                ["@att_id"] = attId,
                ["@description"] = description ?? string.Empty
            };

            await ExecuteNonQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAttachmentDescription",
                Detail = $"Updated attachment {attId} description",
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
                Description = "UpdateAttachmentDescription",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating attachment {AttId} description", attId);
            throw;
        }
    }

    public async Task<bool> DeleteAttachmentAsync(int attId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                DELETE FROM attachment 
                WHERE att_id = @att_id";

            var parameters = new Dictionary<string, object>
            {
                ["@att_id"] = attId
            };

            await ExecuteNonQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "DeleteAttachment",
                Detail = $"Deleted attachment {attId}",
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
                Description = "DeleteAttachment",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error deleting attachment {AttId}", attId);
            throw;
        }
    }

    public async Task<DataTable?> GetAttachmentByIdAsync(int attId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    a.att_id, a.att_filename, a.att_extension, a.att_description, 
                    a.att_insertdatetime, a.att_modifieddatetime, a.att_active,
                    a.att_receipt, a.att_public, a.att_signoff, a.att_submittedby,
                    a.att_receiptamount, a.sr_id, a.cc_id,
                    cc.cc_name, cc.cc_id
                FROM attachment a
                LEFT JOIN CallCenter cc ON a.cc_id = cc.cc_id
                WHERE a.att_id = @att_id";

            var parameters = new Dictionary<string, object>
            {
                ["@att_id"] = attId
            };

            var result = await ExecuteQueryAsync(sql, parameters);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAttachmentById",
                Detail = $"Retrieved attachment {attId}",
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
                Description = "GetAttachmentById",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving attachment {AttId}", attId);
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

    private static DateTime ConvertToDateTime(object value)
    {
        if (value == null || value == DBNull.Value)
            return DateTime.MinValue;
        
        if (DateTime.TryParse(value.ToString(), out var result))
            return result;
            
        return DateTime.MinValue;
    }

    private static DateTime? ConvertToNullableDateTime(object value)
    {
        if (value == null || value == DBNull.Value)
            return null;
        
        if (DateTime.TryParse(value.ToString(), out var result))
            return result;
            
        return null;
    }

    private static int? ConvertToNullableInt(object value)
    {
        if (value == null || value == DBNull.Value)
            return null;
        
        if (int.TryParse(value.ToString(), out var result))
            return result;
            
        return null;
    }

    private static decimal? ConvertToNullableDecimal(object value)
    {
        if (value == null || value == DBNull.Value)
            return null;
        
        if (decimal.TryParse(value.ToString(), out var result))
            return result;
            
        return null;
    }

    private static bool ConvertToBool(object value)
    {
        if (value == null || value == DBNull.Value)
            return false;
        
        if (bool.TryParse(value.ToString(), out var result))
            return result;
        
        // Handle bit values (0/1)
        if (int.TryParse(value.ToString(), out var intResult))
            return intResult != 0;
            
        return false;
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

            // Check if work order belongs to a high-volume company
            if (actualWoId.HasValue && actualWoId.Value > 0)
            {
                const string checkHighVolumeSql = @"
                    SELECT COUNT(1)
                    FROM workorder wo
                    INNER JOIN servicerequest sr ON wo.sr_id = sr.sr_id
                    INNER JOIN xrefCompanyCallCenter xccc ON sr.xccc_id = xccc.xccc_id
                    INNER JOIN company c ON xccc.c_id = c.c_id
                    WHERE wo.wo_id = @wo_id
                      AND c.c_id IN (SELECT cs_value FROM configsetting WHERE cs_identifier = 'HighVolumeCompanyID')";

                using var checkHighVolumeCommand = new SqlCommand(checkHighVolumeSql, connection);
                checkHighVolumeCommand.Parameters.Add("@wo_id", System.Data.SqlDbType.Int).Value = actualWoId.Value;

                var isHighVolume = (int)(await checkHighVolumeCommand.ExecuteScalarAsync() ?? 0) > 0;

                if (isHighVolume)
                {
                    _logger.LogWarning("Attempted to insert time tracking detail for high-volume company work order {WoId} for user {UserId}", 
                        actualWoId, userId);
                    
                    await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                    {
                        Name = "DataService",
                        Description = "InsertTimeTrackingDetail - High Volume Company Rejected",
                        Detail = $"User {userId} attempted to create time tracking detail for high-volume company work order {actualWoId}",
                        ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                        MachineName = Environment.MachineName
                    });
                    
                    return false;
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

    #region Company Administration

    public async Task<List<CompanyListDto>> GetCallCenterCompaniesAsync(int callCenterId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            var companies = new List<CompanyListDto>();
            const string sql = @"
                SELECT 
                    xccc.xccc_id,
                    xccc.c_id as company_id,
                    xccc.cc_id as callcenter_id,
                    c.c_name as company_name,
                    xccc.xccc_active,
                    xccc.xccc_note
                FROM xrefCompanyCallCenter xccc
                INNER JOIN Company c ON xccc.c_id = c.c_id
                WHERE xccc.cc_id = @callCenterId
                ORDER BY c.c_name";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.AddWithValue("@callCenterId", callCenterId);
                await connection.OpenAsync();
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        companies.Add(new CompanyListDto
                        {
                            XcccId = reader.GetInt32(0),
                            CompanyId = reader.GetInt32(1),
                            CallCenterId = reader.GetInt32(2),
                            CompanyName = reader.GetString(3),
                            Active = reader.GetBoolean(4),
                            Note = reader.IsDBNull(5) ? null : reader.GetString(5)
                        });
                    }
                }
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCallCenterCompanies",
                Detail = $"Retrieved {companies.Count} companies for call center {callCenterId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return companies;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error getting companies for call center {CallCenterId}", callCenterId);
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCallCenterCompanies",
                Detail = $"Error retrieving companies for call center {callCenterId}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<CompanyDetailDto?> GetCompanyDetailAsync(int xcccId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            CompanyDetailDto? companyDetail = null;

            const string companySql = @"
                SELECT 
                    xccc.xccc_id,
                    xccc.c_id,
                    xccc.cc_id,
                    c.c_name,
                    cc.cc_name,
                    xccc.xccc_tripcharge,
                    xccc.br_id,
                    xccc.terms_id,
                    xccc.xccc_taxexempt,
                    xccc.xccc_minimumlaborchargeinminutes,
                    xccc.xccc_markuppercentage,
                    xccc.xccc_markuppercentagesupplier,
                    xccc.xccc_active,
                    xccc.xccc_firmquote,
                    xccc.xccc_invoicedateshow,
                    xccc.xccc_ivrrequestnumber,
                    xccc.xccc_clientrep,
                    xccc.xccc_licenserep,
                    xccc.xccc_invoiceextratext,
                    xccc.xccc_note,
                    c.c_portalurl,
                    c.c_portalname,
                    c.c_portalcredentials,
                    xccc.xccc_insertdatetime,
                    xccc.xccc_modifieddatetime,
                    br.br_description,
                    br.br_roundtominute,
                    t.terms_description,
                    t.terms_numberofdays
                FROM xrefCompanyCallCenter xccc
                INNER JOIN Company c ON xccc.c_id = c.c_id
                INNER JOIN CallCenter cc ON xccc.cc_id = cc.cc_id
                LEFT JOIN BillableRule br ON xccc.br_id = br.br_id
                LEFT JOIN Terms t ON xccc.terms_id = t.terms_id
                WHERE xccc.xccc_id = @xcccId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(companySql, connection))
            {
                command.Parameters.AddWithValue("@xcccId", xcccId);
                await connection.OpenAsync();
                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        companyDetail = new CompanyDetailDto
                        {
                            XcccId = reader.GetInt32(0),
                            CompanyId = reader.GetInt32(1),
                            CallCenterId = reader.GetInt32(2),
                            CompanyName = reader.GetString(3),
                            CallCenterName = reader.GetString(4),
                            TripCharge = reader.IsDBNull(5) ? null : reader.GetDecimal(5),
                            BillableRuleId = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                            TermsId = reader.IsDBNull(7) ? null : reader.GetInt32(7),
                            TaxExempt = reader.GetBoolean(8),
                            MinimumLaborChargeMinutes = reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
                            MarkupPercentage = reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                            MarkupPercentageSupplier = reader.IsDBNull(11) ? 0 : reader.GetInt32(11),
                            Active = reader.GetBoolean(12),
                            FirmQuote = reader.GetBoolean(13),
                            InvoiceDateShow = reader.GetBoolean(14),
                            IvrRequestNumber = reader.GetBoolean(15),
                            ClientRepresentative = reader.IsDBNull(16) ? null : reader.GetString(16),
                            LicenseRepresentative = reader.IsDBNull(17) ? null : reader.GetString(17),
                            InvoiceExtraText = reader.IsDBNull(18) ? null : reader.GetString(18),
                            Note = reader.IsDBNull(19) ? null : reader.GetString(19),
                            PortalUrl = reader.IsDBNull(20) ? null : reader.GetString(20),
                            PortalName = reader.IsDBNull(21) ? null : reader.GetString(21),
                            PortalCredentials = reader.IsDBNull(22) ? null : reader.GetString(22),
                            InsertDateTime = reader.GetDateTime(23),
                            ModifiedDateTime = reader.IsDBNull(24) ? null : reader.GetDateTime(24),
                            BillableRuleDescription = reader.IsDBNull(25) ? null : reader.GetString(25),
                            BillableRuleRoundToMinute = reader.IsDBNull(26) ? null : reader.GetInt32(26),
                            TermsDescription = reader.IsDBNull(27) ? null : reader.GetString(27),
                            TermsNumberOfDays = reader.IsDBNull(28) ? 0 : reader.GetInt32(28)
                        };
                    }
                }
            }

            if (companyDetail != null)
            {
                // Get Materials Markup
                const string markupSql = @"
                    SELECT mm_id, mm_from, mm_to, mm_markup, mm_markuphighquantity, mm_insertdatetime, mm_modifieddatetime
                    FROM MaterialsMarkup
                    WHERE xccc_id = @xcccId
                    ORDER BY mm_from";

                using (var connection = new SqlConnection(connectionString))
                using (var command = new SqlCommand(markupSql, connection))
                {
                    command.Parameters.AddWithValue("@xcccId", xcccId);
                    await connection.OpenAsync();
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            companyDetail.MaterialsMarkup.Add(new MaterialsMarkupDto
                            {
                                MmId = reader.GetInt32(0),
                                XcccId = xcccId,
                                FromPrice = reader.GetInt32(1),
                                ToPrice = reader.GetInt32(2),
                                MarkupPercentage = reader.GetInt32(3),
                                MarkupHighQuantity = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                                InsertDateTime = reader.GetDateTime(5),
                                ModifiedDateTime = reader.IsDBNull(6) ? null : reader.GetDateTime(6)
                            });
                        }
                    }
                }

                // Get Billable Rules
                const string billableRuleSql = @"
                    SELECT br_id, br_description, br_roundtominute, br_order
                    FROM BillableRule
                    ORDER BY br_order";

                using (var connection = new SqlConnection(connectionString))
                using (var command = new SqlCommand(billableRuleSql, connection))
                {
                    await connection.OpenAsync();
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            companyDetail.BillableRules.Add(new BillableRuleDto
                            {
                                BrId = reader.GetInt32(0),
                                Description = reader.GetString(1),
                                RoundToMinute = reader.GetInt32(2),
                                Order = reader.GetInt32(3)
                            });
                        }
                    }
                }

                // Get Terms
                const string termsSql = @"
                    SELECT terms_id, terms_description, terms_numberofdays, terms_order
                    FROM Terms
                    ORDER BY terms_order";

                using (var connection = new SqlConnection(connectionString))
                using (var command = new SqlCommand(termsSql, connection))
                {
                    await connection.OpenAsync();
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            companyDetail.Terms.Add(new TermsDto
                            {
                                TermsId = reader.GetInt32(0),
                                Description = reader.GetString(1),
                                NumberOfDays = reader.GetInt32(2),
                                Order = reader.GetInt32(3)
                            });
                        }
                    }
                }
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCompanyDetail",
                Detail = $"Retrieved company detail for xccc_id {xcccId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return companyDetail;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error getting company detail for xccc_id {XcccId}", xcccId);
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCompanyDetail",
                Detail = $"Error retrieving company detail for xccc_id {xcccId}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<bool> UpdateCompanyGeneralInfoAsync(UpdateCompanyGeneralInfoRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string sql = @"
                UPDATE xrefCompanyCallCenter
                SET 
                    xccc_tripcharge = @tripcharge,
                    br_id = @billableRuleId,
                    terms_id = @termsId,
                    xccc_taxexempt = @taxexempt,
                    xccc_minimumlaborchargeinminutes = @minimumlaborcharge,
                    xccc_markuppercentage = @markuppercentage,
                    xccc_markuppercentagesupplier = @markuppercentagesupplier,
                    xccc_active = @active,
                    xccc_firmquote = @firmquote,
                    xccc_invoicedateshow = @invoicedateshow,
                    xccc_ivrrequestnumber = @ivrrequestnumber,
                    xccc_clientrep = @clientrep,
                    xccc_licenserep = @licenserep,
                    xccc_invoiceextratext = @invoiceextratext,
                    xccc_note = @note,
                    xccc_modifieddatetime = GETDATE()
                WHERE xccc_id = @xcccId;
                
                UPDATE Company
                SET
                    c_portalurl = @portalurl,
                    c_portalname = @portalname,
                    c_portalcredentials = @portalcredentials
                WHERE c_id = (SELECT c_id FROM xrefCompanyCallCenter WHERE xccc_id = @xcccId)";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@xcccId", SqlDbType.Int).Value = request.XcccId;
                command.Parameters.Add("@tripcharge", SqlDbType.Decimal).Value = request.TripCharge.HasValue ? (object)request.TripCharge.Value : DBNull.Value;
                command.Parameters.Add("@billableRuleId", SqlDbType.Int).Value = request.BillableRuleId.HasValue ? (object)request.BillableRuleId.Value : DBNull.Value;
                command.Parameters.Add("@termsId", SqlDbType.Int).Value = request.TermsId.HasValue ? (object)request.TermsId.Value : DBNull.Value;
                command.Parameters.Add("@taxexempt", SqlDbType.Bit).Value = request.TaxExempt;
                command.Parameters.Add("@minimumlaborcharge", SqlDbType.Int).Value = request.MinimumLaborChargeMinutes;
                command.Parameters.Add("@markuppercentage", SqlDbType.Int).Value = request.MarkupPercentage;
                command.Parameters.Add("@markuppercentagesupplier", SqlDbType.Int).Value = request.MarkupPercentageSupplier;
                command.Parameters.Add("@active", SqlDbType.Bit).Value = request.Active;
                command.Parameters.Add("@firmquote", SqlDbType.Bit).Value = request.FirmQuote;
                command.Parameters.Add("@invoicedateshow", SqlDbType.Bit).Value = request.InvoiceDateShow;
                command.Parameters.Add("@ivrrequestnumber", SqlDbType.Bit).Value = request.IvrRequestNumber;
                command.Parameters.Add("@clientrep", SqlDbType.VarChar, 200).Value = !string.IsNullOrEmpty(request.ClientRepresentative) ? (object)request.ClientRepresentative : DBNull.Value;
                command.Parameters.Add("@licenserep", SqlDbType.VarChar, 200).Value = !string.IsNullOrEmpty(request.LicenseRepresentative) ? (object)request.LicenseRepresentative : DBNull.Value;
                command.Parameters.Add("@invoiceextratext", SqlDbType.VarChar, 4000).Value = !string.IsNullOrEmpty(request.InvoiceExtraText) ? (object)request.InvoiceExtraText : DBNull.Value;
                command.Parameters.Add("@note", SqlDbType.VarChar, 8000).Value = !string.IsNullOrEmpty(request.Note) ? (object)request.Note : DBNull.Value;
                command.Parameters.Add("@portalurl", SqlDbType.VarChar, 100).Value = !string.IsNullOrEmpty(request.PortalUrl) ? (object)request.PortalUrl : DBNull.Value;
                command.Parameters.Add("@portalname", SqlDbType.VarChar, 50).Value = !string.IsNullOrEmpty(request.PortalName) ? (object)request.PortalName : DBNull.Value;
                command.Parameters.Add("@portalcredentials", SqlDbType.VarChar, 200).Value = !string.IsNullOrEmpty(request.PortalCredentials) ? (object)request.PortalCredentials : DBNull.Value;

                await connection.OpenAsync();
                var rowsAffected = await command.ExecuteNonQueryAsync();

                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "UpdateCompanyGeneralInfo",
                    Detail = $"Updated company general info for xccc_id {request.XcccId}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return rowsAffected > 0;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error updating company general info for xccc_id {XcccId}", request.XcccId);
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCompanyGeneralInfo",
                Detail = $"Error updating company general info for xccc_id {request.XcccId}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<int?> CreateMaterialsMarkupAsync(CreateMaterialsMarkupRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string sql = @"
                INSERT INTO MaterialsMarkup (xccc_id, mm_from, mm_to, mm_markup, mm_markuphighquantity, mm_insertdatetime)
                VALUES (@xcccId, @fromPrice, @toPrice, @markupPercentage, @markupHighQuantity, GETDATE());
                SELECT CAST(SCOPE_IDENTITY() as int)";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@xcccId", SqlDbType.Int).Value = request.XcccId;
                command.Parameters.Add("@fromPrice", SqlDbType.Int).Value = request.FromPrice;
                command.Parameters.Add("@toPrice", SqlDbType.Int).Value = request.ToPrice;
                command.Parameters.Add("@markupPercentage", SqlDbType.Int).Value = request.MarkupPercentage;
                command.Parameters.Add("@markupHighQuantity", SqlDbType.Int).Value = request.MarkupHighQuantity;

                await connection.OpenAsync();
                var newId = (int?)await command.ExecuteScalarAsync();

                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "CreateMaterialsMarkup",
                    Detail = $"Created materials markup for xccc_id {request.XcccId}, range {request.FromPrice}-{request.ToPrice}%, markup {request.MarkupPercentage}%, high quantity {request.MarkupHighQuantity}%",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return newId;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error creating materials markup for xccc_id {XcccId}", request.XcccId);
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateMaterialsMarkup",
                Detail = $"Error creating materials markup for xccc_id {request.XcccId}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<UpdateMaterialsMarkupRequest> GetMaterialsMarkupByIdAsync(int mmId)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string sql = @"
                SELECT 
                    mm_from,
                    mm_to,
                    mm_markup,
                    mm_markuphighquantity
                FROM MaterialsMarkup
                WHERE mm_id = @mmId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@mmId", SqlDbType.Int).Value = mmId;
                await connection.OpenAsync();

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        return new UpdateMaterialsMarkupRequest
                        {
                            MmId = mmId,
                            FromPrice = reader.GetInt32(0),
                            ToPrice = reader.GetInt32(1),
                            MarkupPercentage = reader.GetInt32(2),
                            MarkupHighQuantity = reader.GetInt32(3)
                        };
                    }
                }
            }

            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving materials markup mm_id {MmId}", mmId);
            throw;
        }
    }

    public async Task<(UpdateMaterialsMarkupRequest? MarkupData, string? CompanyName)> GetMaterialsMarkupWithCompanyByIdAsync(int mmId)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string sql = @"
                SELECT 
                    mm.mm_from,
                    mm.mm_to,
                    mm.mm_markup,
                    mm.mm_markuphighquantity,
                    c.c_name
                FROM MaterialsMarkup mm
                INNER JOIN xrefCompanyCallCenter xccc ON mm.xccc_id = xccc.xccc_id
                INNER JOIN company c ON xccc.c_id = c.c_id
                WHERE mm.mm_id = @mmId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@mmId", SqlDbType.Int).Value = mmId;
                await connection.OpenAsync();

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        var markupData = new UpdateMaterialsMarkupRequest
                        {
                            MmId = mmId,
                            FromPrice = reader.GetInt32(0),
                            ToPrice = reader.GetInt32(1),
                            MarkupPercentage = reader.GetInt32(2),
                            MarkupHighQuantity = reader.GetInt32(3)
                        };
                        var companyName = reader.IsDBNull(4) ? null : reader.GetString(4);
                        return (markupData, companyName);
                    }
                }
            }

            return (null, null);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving materials markup with company mm_id {MmId}", mmId);
            throw;
        }
    }

    public async Task<bool> UpdateMaterialsMarkupAsync(UpdateMaterialsMarkupRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            // Get the xccc_id for this markup record
            const string getXcccSql = "SELECT xccc_id FROM MaterialsMarkup WHERE mm_id = @mmId";
            int xcccId;

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(getXcccSql, connection))
            {
                command.Parameters.Add("@mmId", SqlDbType.Int).Value = request.MmId;
                await connection.OpenAsync();
                var result = await command.ExecuteScalarAsync();
                if (result == null)
                {
                    throw new InvalidOperationException($"Materials markup record {request.MmId} not found");
                }
                xcccId = (int)result;
            }

            const string sql = @"
                UPDATE MaterialsMarkup
                SET 
                    mm_from = @fromPrice,
                    mm_to = @toPrice,
                    mm_markup = @markupPercentage,
                    mm_markuphighquantity = @markupHighQuantity,
                    mm_modifieddatetime = GETDATE()
                WHERE mm_id = @mmId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@mmId", SqlDbType.Int).Value = request.MmId;
                command.Parameters.Add("@fromPrice", SqlDbType.Int).Value = request.FromPrice;
                command.Parameters.Add("@toPrice", SqlDbType.Int).Value = request.ToPrice;
                command.Parameters.Add("@markupPercentage", SqlDbType.Int).Value = request.MarkupPercentage;
                command.Parameters.Add("@markupHighQuantity", SqlDbType.Int).Value = request.MarkupHighQuantity;

                await connection.OpenAsync();
                var rowsAffected = await command.ExecuteNonQueryAsync();

                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "UpdateMaterialsMarkup",
                    Detail = $"Updated materials markup mm_id {request.MmId}, range {request.FromPrice}-{request.ToPrice}%, markup {request.MarkupPercentage}%, high quantity {request.MarkupHighQuantity}%",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return rowsAffected > 0;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error updating materials markup mm_id {MmId}", request.MmId);
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateMaterialsMarkup",
                Detail = $"Error updating materials markup mm_id {request.MmId}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<bool> DeleteMaterialsMarkupAsync(int mmId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string sql = "DELETE FROM MaterialsMarkup WHERE mm_id = @mmId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@mmId", SqlDbType.Int).Value = mmId;

                await connection.OpenAsync();
                var rowsAffected = await command.ExecuteNonQueryAsync();

                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "DeleteMaterialsMarkup",
                    Detail = $"Deleted materials markup mm_id {mmId}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return rowsAffected > 0;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error deleting materials markup mm_id {MmId}", mmId);
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "DeleteMaterialsMarkup",
                Detail = $"Error deleting materials markup mm_id {mmId}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<bool> ResetMaterialsMarkupToDefaultAsync(int xcccId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            using (var connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync();

                // Step 1: Delete existing materials markup for this company/call center
                const string deleteSql = "DELETE FROM MaterialsMarkup WHERE xccc_id = @xcccId";
                using (var deleteCommand = new SqlCommand(deleteSql, connection))
                {
                    deleteCommand.Parameters.Add("@xcccId", SqlDbType.Int).Value = xcccId;
                    await deleteCommand.ExecuteNonQueryAsync();
                }

                // Step 2: Get template ID from config settings
                const string configSql = "SELECT cs_value FROM ConfigSetting WHERE cs_type = 'Config' AND cs_identifier = 'MaterialsMarkupTemplateID'";
                string templateIdStr = null;
                using (var configCommand = new SqlCommand(configSql, connection))
                {
                    var result = await configCommand.ExecuteScalarAsync();
                    if (result == null || result == DBNull.Value)
                    {
                        throw new InvalidOperationException("MaterialsMarkupTemplateID config setting not found");
                    }
                    templateIdStr = result.ToString();
                }

                if (!int.TryParse(templateIdStr, out var templateId))
                {
                    throw new InvalidOperationException($"Invalid MaterialsMarkupTemplateID value: {templateIdStr}");
                }

                // Step 3: Copy materials markup from template
                const string insertSql = @"
                    INSERT INTO MaterialsMarkup (xccc_id, mm_from, mm_to, mm_markup, mm_markuphighquantity)
                    SELECT @xcccId, mm_from, mm_to, mm_markup, mm_markuphighquantity
                    FROM MaterialsMarkup
                    WHERE xccc_id = @templateId";

                using (var insertCommand = new SqlCommand(insertSql, connection))
                {
                    insertCommand.Parameters.Add("@xcccId", SqlDbType.Int).Value = xcccId;
                    insertCommand.Parameters.Add("@templateId", SqlDbType.Int).Value = templateId;
                    await insertCommand.ExecuteNonQueryAsync();
                }

                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "ResetMaterialsMarkupToDefault",
                    Detail = $"Reset materials markup to default template (xccc_id: {xcccId}, template_id: {templateId})",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return true;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error resetting materials markup to default for xccc_id {XcccId}", xcccId);
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "ResetMaterialsMarkupToDefault",
                Detail = $"Error resetting materials markup to default for xccc_id {xcccId}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    // Company Priority methods
    public async Task<List<CompanyPriorityDto>> GetCompanyPrioritiesAsync(int companyId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            // Match old interface behavior: Load ALL priorities, then merge with company-specific data
            // This ensures all 9 priorities appear even if not in xrefCompanyPriority yet
            const string sql = @"
                SELECT 
                    ISNULL(xcp.xcp_id, 0) AS xcp_id,
                    @companyId AS c_id,
                    p.p_id,
                    p.p_priority,
                    ISNULL(xcp.xcp_priority, '') AS xcp_priority,
                    ISNULL(xcp.xcp_arrivaltimeinhours, p.p_arrivaltimeinhours) AS xcp_arrivaltimeinhours,
                    ISNULL(p.p_order, 999) AS p_order
                FROM Priority p
                LEFT JOIN xrefCompanyPriority xcp ON p.p_id = xcp.p_id AND xcp.c_id = @companyId
                ORDER BY ISNULL(p.p_order, 999)";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@companyId", SqlDbType.Int).Value = companyId;
                await connection.OpenAsync();

                var priorities = new List<CompanyPriorityDto>();
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        priorities.Add(new CompanyPriorityDto
                        {
                            XcpId = reader.GetInt32(0), // Will be 0 if not in xrefCompanyPriority
                            CompanyId = reader.GetInt32(1),
                            PriorityId = reader.GetInt32(2),
                            PriorityName = reader.IsDBNull(3) ? string.Empty : reader.GetString(3),
                            CompanySpecificName = reader.IsDBNull(4) ? string.Empty : reader.GetString(4),
                            ArrivalTimeInHours = reader.IsDBNull(5) ? 0 : reader.GetDecimal(5),
                            PriorityOrder = reader.IsDBNull(6) ? 0 : reader.GetInt32(6)
                        });
                    }
                }

                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "GetCompanyPriorities",
                    Detail = $"Retrieved {priorities.Count} priorities (all system priorities merged with company-specific data) for company c_id {companyId}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return priorities;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error retrieving priorities for company c_id {CompanyId}", companyId);
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCompanyPriorities",
                Detail = $"Error retrieving priorities for company c_id {companyId}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<bool> UpdateCompanyPriorityAsync(UpdateCompanyPriorityRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            string sql;
            if (request.XcpId == 0)
            {
                // INSERT new record - priority doesn't exist in xrefCompanyPriority yet
                sql = @"
                    INSERT INTO xrefCompanyPriority (c_id, p_id, xcp_priority, xcp_arrivaltimeinhours, xcp_insertdatetime)
                    VALUES (@companyId, @priorityId, @companySpecificName, @arrivalTimeInHours, GETDATE())";
            }
            else
            {
                // UPDATE existing record
                sql = @"
                    UPDATE xrefCompanyPriority
                    SET 
                        xcp_priority = @companySpecificName,
                        xcp_arrivaltimeinhours = @arrivalTimeInHours,
                        xcp_modifieddatetime = GETDATE()
                    WHERE xcp_id = @xcpId";
            }

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                if (request.XcpId == 0)
                {
                    command.Parameters.Add("@companyId", SqlDbType.Int).Value = request.CompanyId;
                    command.Parameters.Add("@priorityId", SqlDbType.Int).Value = request.PriorityId;
                }
                else
                {
                    command.Parameters.Add("@xcpId", SqlDbType.Int).Value = request.XcpId;
                }
                
                command.Parameters.Add("@companySpecificName", SqlDbType.VarChar, 50).Value = request.CompanySpecificName;
                
                var arrivalTimeParam = command.Parameters.Add("@arrivalTimeInHours", SqlDbType.Decimal);
                arrivalTimeParam.Precision = 18;
                arrivalTimeParam.Scale = 2;
                arrivalTimeParam.Value = request.ArrivalTimeInHours;

                await connection.OpenAsync();
                var rowsAffected = await command.ExecuteNonQueryAsync();

                stopwatch.Stop();
                var action = request.XcpId == 0 ? "Inserted" : "Updated";
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "UpdateCompanyPriority",
                    Detail = $"{action} company priority for c_id {request.CompanyId}, p_id {request.PriorityId}, name '{request.CompanySpecificName}', arrival time {request.ArrivalTimeInHours} hours",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return rowsAffected > 0;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error updating company priority xcp_id {XcpId}", request.XcpId);
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCompanyPriority",
                Detail = $"Error updating company priority xcp_id {request.XcpId}: {ex}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    // User Attachment Type methods
    public async Task<DataTable> GetAllUserAttachmentTypesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    uat_id,
                    uat_insertdatetime,
                    uat_modifieddatetime,
                    uat_type
                FROM dbo.userattachmenttype
                ORDER BY uat_type";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllUserAttachmentTypes",
                Detail = $"Retrieved {result.Rows.Count} user attachment types",
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
                Description = "GetAllUserAttachmentTypes",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving user attachment types");
            throw;
        }
    }

    public async Task<int?> CreateUserAttachmentTypeAsync(EvoAPI.Shared.DTOs.CreateUserAttachmentTypeRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.userattachmenttype 
                (uat_type, uat_insertdatetime, uat_modifieddatetime)
                VALUES 
                (@UatType, GETDATE(), GETDATE());
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@UatType", request.uat_type }
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
                    Description = "CreateUserAttachmentType",
                    Detail = $"Created new user attachment type '{request.uat_type}' with ID {id}",
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
                Description = "CreateUserAttachmentType",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating user attachment type {UatType}", request.uat_type);
            throw;
        }
    }

    public async Task<bool> UpdateUserAttachmentTypeAsync(EvoAPI.Shared.DTOs.UpdateUserAttachmentTypeRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.userattachmenttype 
                SET 
                    uat_type = @UatType,
                    uat_modifieddatetime = GETDATE()
                WHERE uat_id = @UatId";

            var parameters = new Dictionary<string, object>
            {
                { "@UatId", request.uat_id },
                { "@UatType", request.uat_type }
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
                Description = "UpdateUserAttachmentType",
                Detail = $"Updated user attachment type {request.uat_id} - {request.uat_type}. Rows affected: {rowsAffected}",
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
                Description = "UpdateUserAttachmentType",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating user attachment type {UatId}", request.uat_id);
            throw;
        }
    }

    // User Clothing Size methods
    public async Task<DataTable> GetAllUserClothingSizesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    uc_id,
                    uc_insertdatetime,
                    uc_modifieddatetime,
                    uc_clothingsize
                FROM dbo.UserClothing
                ORDER BY uc_clothingsize";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllUserClothingSizes",
                Detail = $"Retrieved {result.Rows.Count} user clothing sizes",
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
                Description = "GetAllUserClothingSizes",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving user clothing sizes");
            throw;
        }
    }

    public async Task<int?> CreateUserClothingSizeAsync(CreateUserClothingSizeRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.UserClothing 
                (uc_clothingsize, uc_insertdatetime, uc_modifieddatetime)
                VALUES 
                (@ClothingSize, GETDATE(), GETDATE());
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@ClothingSize", request.ClothingSize }
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
                    Description = "CreateUserClothingSize",
                    Detail = $"Created new user clothing size '{request.ClothingSize}' with ID {id}",
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
                Description = "CreateUserClothingSize",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating user clothing size {ClothingSize}", request.ClothingSize);
            throw;
        }
    }

    public async Task<bool> UpdateUserClothingSizeAsync(UpdateUserClothingSizeRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.UserClothing
                SET 
                    uc_clothingsize = @ClothingSize,
                    uc_modifieddatetime = GETDATE()
                WHERE uc_id = @Id";

            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@ClothingSize", request.ClothingSize }
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
                Description = "UpdateUserClothingSize",
                Detail = $"Updated user clothing size {request.Id} - {request.ClothingSize}. Rows affected: {rowsAffected}",
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
                Description = "UpdateUserClothingSize",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating user clothing size {Id}", request.Id);
            throw;
        }
    }

    // User Pants Waist methods
    public async Task<DataTable> GetAllUserPantsWaistAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    upw_id,
                    upw_insertdatetime,
                    upw_modifieddatetime,
                    upw_size,
                    upw_sex
                FROM dbo.UserPantsWaist
                ORDER BY upw_size, upw_sex";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllUserPantsWaist",
                Detail = $"Retrieved {result.Rows.Count} user pants waist sizes",
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
                Description = "GetAllUserPantsWaist",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving user pants waist sizes");
            throw;
        }
    }

    public async Task<int?> CreateUserPantsWaistAsync(CreateUserPantsWaistRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.UserPantsWaist 
                (upw_size, upw_sex, upw_insertdatetime, upw_modifieddatetime)
                VALUES 
                (@Size, @Sex, GETDATE(), GETDATE());
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@Size", request.Size },
                { "@Sex", request.Sex }
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
                    Description = "CreateUserPantsWaist",
                    Detail = $"Created new user pants waist size '{request.Size}' ({request.Sex}) with ID {id}",
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
                Description = "CreateUserPantsWaist",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating user pants waist size {Size}", request.Size);
            throw;
        }
    }

    public async Task<bool> UpdateUserPantsWaistAsync(UpdateUserPantsWaistRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.UserPantsWaist
                SET 
                    upw_size = @Size,
                    upw_sex = @Sex,
                    upw_modifieddatetime = GETDATE()
                WHERE upw_id = @Id";

            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@Size", request.Size },
                { "@Sex", request.Sex }
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
                Description = "UpdateUserPantsWaist",
                Detail = $"Updated user pants waist size {request.Id} - Size: {request.Size}, Sex: {request.Sex}. Rows affected: {rowsAffected}",
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
                Description = "UpdateUserPantsWaist",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating user pants waist size {Id}", request.Id);
            throw;
        }
    }

    // User Pants Length methods
    public async Task<DataTable> GetAllUserPantsLengthAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    upl_id,
                    upl_insertdatetime,
                    upl_modifieddatetime,
                    upl_size,
                    upl_sex
                FROM dbo.UserPantsLength
                ORDER BY upl_size, upl_sex";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllUserPantsLength",
                Detail = $"Retrieved {result.Rows.Count} user pants length sizes",
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
                Description = "GetAllUserPantsLength",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving user pants length sizes");
            throw;
        }
    }

    public async Task<int?> CreateUserPantsLengthAsync(CreateUserPantsLengthRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.UserPantsLength 
                (upl_size, upl_sex, upl_insertdatetime, upl_modifieddatetime)
                VALUES 
                (@Size, @Sex, GETDATE(), GETDATE());
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@Size", request.Size },
                { "@Sex", request.Sex }
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
                    Description = "CreateUserPantsLength",
                    Detail = $"Created new user pants length size '{request.Size}' ({request.Sex}) with ID {id}",
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
                Description = "CreateUserPantsLength",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating user pants length size {Size}", request.Size);
            throw;
        }
    }

    public async Task<bool> UpdateUserPantsLengthAsync(UpdateUserPantsLengthRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.UserPantsLength
                SET 
                    upl_size = @Size,
                    upl_sex = @Sex,
                    upl_modifieddatetime = GETDATE()
                WHERE upl_id = @Id";

            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@Size", request.Size },
                { "@Sex", request.Sex }
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
                Description = "UpdateUserPantsLength",
                Detail = $"Updated user pants length size {request.Id} - Size: {request.Size}, Sex: {request.Sex}. Rows affected: {rowsAffected}",
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
                Description = "UpdateUserPantsLength",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating user pants length size {Id}", request.Id);
            throw;
        }
    }
    public async Task<DataTable> GetAllUserRelationshipsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    ur_id,
                    ur_insertdatetime,
                    ur_modifieddatetime,
                    ur_relationship
                FROM dbo.UserRelationship
                ORDER BY ur_relationship";

            var result = await ExecuteQueryAsync(sql);
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllUserRelationships",
                Detail = $"Retrieved {result.Rows.Count} user relationships",
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
                Description = "GetAllUserRelationships",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving user relationships");
            throw;
        }
    }

    public async Task<int?> CreateUserRelationshipAsync(CreateUserRelationshipRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.UserRelationship 
                (ur_relationship, ur_insertdatetime, ur_modifieddatetime)
                VALUES 
                (@Relationship, GETDATE(), GETDATE());
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@Relationship", request.Relationship }
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
                    Description = "CreateUserRelationship",
                    Detail = $"Created new user relationship '{request.Relationship}' with ID {id}",
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
                Description = "CreateUserRelationship",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating user relationship {Relationship}", request.Relationship);
            throw;
        }
    }

    public async Task<bool> UpdateUserRelationshipAsync(UpdateUserRelationshipRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.UserRelationship
                SET 
                    ur_relationship = @Relationship,
                    ur_modifieddatetime = GETDATE()
                WHERE ur_id = @Id";

            var parameters = new Dictionary<string, object>
            {
                { "@Id", request.Id },
                { "@Relationship", request.Relationship }
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
                Description = "UpdateUserRelationship",
                Detail = $"Updated user relationship {request.Id} - {request.Relationship}. Rows affected: {rowsAffected}",
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
                Description = "UpdateUserRelationship",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating user relationship {Id}", request.Id);
            throw;
        }
    }

    // User Emergency Contact methods
    public async Task<Dictionary<int, List<UserEmergencyContactDto>>> GetAllEmergencyContactsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    xuec.xuec_id,
                    xuec.xuec_insertdatetime,
                    xuec.xuec_modifieddatetime,
                    xuec.u_id,
                    xuec.ur_id,
                    xuec.xuec_name,
                    xuec.xuec_phone,
                    ur.ur_relationship
                FROM dbo.xrefUserEmergencyContact xuec
                LEFT JOIN dbo.UserRelationship ur ON xuec.ur_id = ur.ur_id
                ORDER BY xuec.u_id, xuec.xuec_id";

            var dt = await ExecuteQueryAsync(sql);
            
            var result = new Dictionary<int, List<UserEmergencyContactDto>>();
            foreach (DataRow row in dt.Rows)
            {
                var userId = Convert.ToInt32(row["u_id"]);
                
                if (!result.ContainsKey(userId))
                {
                    result[userId] = new List<UserEmergencyContactDto>();
                }
                
                result[userId].Add(new UserEmergencyContactDto
                {
                    XuecId = Convert.ToInt32(row["xuec_id"]),
                    UserId = userId,
                    RelationshipId = row["ur_id"] != DBNull.Value ? Convert.ToInt32(row["ur_id"]) : null,
                    RelationshipName = row["ur_relationship"] != DBNull.Value ? row["ur_relationship"].ToString() : null,
                    Name = row["xuec_name"] != DBNull.Value ? row["xuec_name"].ToString() : null,
                    Phone = row["xuec_phone"] != DBNull.Value ? row["xuec_phone"].ToString() : null,
                    InsertDateTime = Convert.ToDateTime(row["xuec_insertdatetime"]),
                    ModifiedDateTime = row["xuec_modifieddatetime"] != DBNull.Value ? Convert.ToDateTime(row["xuec_modifieddatetime"]) : null
                });
            }
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllEmergencyContacts",
                Detail = $"Retrieved emergency contacts for {result.Count} users",
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
                Description = "GetAllEmergencyContacts",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error getting all emergency contacts");
            throw;
        }
    }

    public async Task<List<UserEmergencyContactDto>> GetUserEmergencyContactsAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    xuec.xuec_id,
                    xuec.xuec_insertdatetime,
                    xuec.xuec_modifieddatetime,
                    xuec.u_id,
                    xuec.ur_id,
                    xuec.xuec_name,
                    xuec.xuec_phone,
                    ur.ur_relationship
                FROM dbo.xrefUserEmergencyContact xuec
                LEFT JOIN dbo.UserRelationship ur ON xuec.ur_id = ur.ur_id
                WHERE xuec.u_id = @UserId
                ORDER BY xuec.xuec_id";

            var parameters = new Dictionary<string, object> { { "@UserId", userId } };
            var dt = await ExecuteQueryAsync(sql, parameters);
            
            var result = new List<UserEmergencyContactDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new UserEmergencyContactDto
                {
                    XuecId = Convert.ToInt32(row["xuec_id"]),
                    UserId = Convert.ToInt32(row["u_id"]),
                    RelationshipId = row["ur_id"] != DBNull.Value ? Convert.ToInt32(row["ur_id"]) : null,
                    RelationshipName = row["ur_relationship"] != DBNull.Value ? row["ur_relationship"].ToString() : null,
                    Name = row["xuec_name"] != DBNull.Value ? row["xuec_name"].ToString() : null,
                    Phone = row["xuec_phone"] != DBNull.Value ? row["xuec_phone"].ToString() : null,
                    InsertDateTime = Convert.ToDateTime(row["xuec_insertdatetime"]),
                    ModifiedDateTime = row["xuec_modifieddatetime"] != DBNull.Value ? Convert.ToDateTime(row["xuec_modifieddatetime"]) : null
                });
            }
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetUserEmergencyContacts",
                Detail = $"Retrieved {result.Count} emergency contacts for user {userId}",
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
                Description = "GetUserEmergencyContacts",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error getting emergency contacts for user {UserId}", userId);
            throw;
        }
    }

    public async Task<int?> CreateUserEmergencyContactAsync(int userId, CreateUserEmergencyContactRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.xrefUserEmergencyContact (xuec_insertdatetime, u_id, ur_id, xuec_name, xuec_phone)
                VALUES (GETDATE(), @UserId, @RelationshipId, @Name, @Phone);
                SELECT SCOPE_IDENTITY();";

            var parameters = new Dictionary<string, object>
            {
                { "@UserId", userId },
                { "@RelationshipId", request.RelationshipId.HasValue ? (object)request.RelationshipId.Value : DBNull.Value },
                { "@Name", string.IsNullOrWhiteSpace(request.Name) ? DBNull.Value : (object)request.Name.Trim() },
                { "@Phone", string.IsNullOrWhiteSpace(request.Phone) ? DBNull.Value : (object)request.Phone.Trim() }
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
            var result = await command.ExecuteScalarAsync();
            var newId = result != null && result != DBNull.Value ? Convert.ToInt32(result) : (int?)null;
            
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateUserEmergencyContact",
                Detail = $"Created emergency contact for user {userId} with ID {newId}",
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
                Description = "CreateUserEmergencyContact",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating emergency contact for user {UserId}", userId);
            throw;
        }
    }

    public async Task<bool> UpdateUserEmergencyContactAsync(int userId, UpdateUserEmergencyContactRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.xrefUserEmergencyContact
                SET 
                    ur_id = @RelationshipId,
                    xuec_name = @Name,
                    xuec_phone = @Phone,
                    xuec_modifieddatetime = GETDATE()
                WHERE xuec_id = @XuecId AND u_id = @UserId";

            var parameters = new Dictionary<string, object>
            {
                { "@XuecId", request.XuecId },
                { "@UserId", userId },
                { "@RelationshipId", request.RelationshipId.HasValue ? (object)request.RelationshipId.Value : DBNull.Value },
                { "@Name", string.IsNullOrWhiteSpace(request.Name) ? DBNull.Value : (object)request.Name.Trim() },
                { "@Phone", string.IsNullOrWhiteSpace(request.Phone) ? DBNull.Value : (object)request.Phone.Trim() }
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
                Description = "UpdateUserEmergencyContact",
                Detail = $"Updated emergency contact {request.XuecId} for user {userId}. Rows affected: {rowsAffected}",
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
                Description = "UpdateUserEmergencyContact",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating emergency contact {XuecId} for user {UserId}", request.XuecId, userId);
            throw;
        }
    }

    public async Task<bool> DeleteUserEmergencyContactAsync(int userId, int xuecId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                DELETE FROM dbo.xrefUserEmergencyContact
                WHERE xuec_id = @XuecId AND u_id = @UserId";

            var parameters = new Dictionary<string, object>
            {
                { "@XuecId", xuecId },
                { "@UserId", userId }
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
                Description = "DeleteUserEmergencyContact",
                Detail = $"Deleted emergency contact {xuecId} for user {userId}. Rows affected: {rowsAffected}",
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
                Description = "DeleteUserEmergencyContact",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error deleting emergency contact {XuecId} for user {UserId}", xuecId, userId);
            throw;
        }
    }

    // Employee Attachments methods
    public async Task<List<EmployeeAttachmentDto>> GetEmployeeAttachmentsAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    xua.xua_id,
                    xua.xua_insertdatetime,
                    xua.xua_modifieddatetime,
                    xua.u_id,
                    xua.att_id,
                    xua.uat_id,
                    xua.xua_description,
                    xua.xua_issuingauthority,
                    xua.xua_dateexpires,
                    a.att_filename,
                    a.att_insertdatetime,
                    uat.uat_type
                FROM dbo.xrefuserattachment xua
                INNER JOIN dbo.attachment a ON xua.att_id = a.att_id
                INNER JOIN dbo.userattachmenttype uat ON xua.uat_id = uat.uat_id
                WHERE xua.u_id = @UserId
                ORDER BY xua.xua_insertdatetime DESC";

            var parameters = new Dictionary<string, object> { { "@UserId", userId } };
            var dt = await ExecuteQueryAsync(sql, parameters);
            
            var result = new List<EmployeeAttachmentDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new EmployeeAttachmentDto
                {
                    xua_id = ConvertToInt(row["xua_id"]),
                    xua_insertdatetime = ConvertToDateTime(row["xua_insertdatetime"]),
                    xua_modifieddatetime = ConvertToNullableDateTime(row["xua_modifieddatetime"]),
                    u_id = ConvertToInt(row["u_id"]),
                    att_id = ConvertToInt(row["att_id"]),
                    uat_id = ConvertToInt(row["uat_id"]),
                    xua_description = row["xua_description"]?.ToString() ?? string.Empty,
                    xua_issuingauthority = row["xua_issuingauthority"]?.ToString() ?? string.Empty,
                    xua_dateexpires = row["xua_dateexpires"]?.ToString() ?? string.Empty,
                    att_filename = row["att_filename"]?.ToString() ?? string.Empty,
                    att_insertdatetime = ConvertToDateTime(row["att_insertdatetime"]),
                    uat_type = row["uat_type"]?.ToString() ?? string.Empty
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetEmployeeAttachments",
                Detail = $"Retrieved {result.Count} attachments for user {userId}",
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
                Description = "GetEmployeeAttachments",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving employee attachments for user {UserId}", userId);
            throw;
        }
    }

    public async Task<int?> CreateEmployeeAttachmentAsync(int userId, CreateEmployeeAttachmentRequest request, int attachmentId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.xrefuserattachment 
                (u_id, att_id, uat_id, xua_description, xua_issuingauthority, xua_dateexpires, xua_insertdatetime, xua_modifieddatetime)
                VALUES 
                (@UserId, @AttId, @UatId, @Description, @IssuingAuthority, @DateExpires, GETDATE(), GETDATE());
                
                SELECT SCOPE_IDENTITY() as NewId;";

            var parameters = new Dictionary<string, object>
            {
                { "@UserId", userId },
                { "@AttId", attachmentId },
                { "@UatId", request.uat_id },
                { "@Description", request.description ?? string.Empty },
                { "@IssuingAuthority", request.xua_issuingauthority ?? string.Empty },
                { "@DateExpires", request.xua_dateexpires ?? "Not Applicable" }
            };

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            using var command = new SqlCommand(sql, connection);
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value ?? DBNull.Value);
            }

            await connection.OpenAsync();
            var result = await command.ExecuteScalarAsync();
            int? xuaId = null;
            if (result != null && int.TryParse(result.ToString(), out var id))
            {
                xuaId = id;
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateEmployeeAttachment",
                Detail = $"Created employee attachment {xuaId} for user {userId} with attachment {attachmentId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return xuaId;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateEmployeeAttachment",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating employee attachment for user {UserId}", userId);
            throw;
        }
    }

    public async Task<bool> UpdateEmployeeAttachmentAsync(int userId, int xuaId, UpdateEmployeeAttachmentRequest request, int? newAttachmentId = null)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var sql = @"
                UPDATE dbo.xrefuserattachment 
                SET 
                    xua_description = @Description,
                    uat_id = @UatId,
                    xua_issuingauthority = @IssuingAuthority,
                    xua_dateexpires = @DateExpires,
                    xua_modifieddatetime = GETDATE()";

            // If new attachment provided, update att_id (orphan the old one)
            if (newAttachmentId.HasValue)
            {
                sql += ", att_id = @NewAttId";
            }

            sql += @" 
                WHERE xua_id = @XuaId AND u_id = @UserId;";

            var parameters = new Dictionary<string, object>
            {
                { "@XuaId", xuaId },
                { "@UserId", userId },
                { "@Description", request.description ?? string.Empty },
                { "@UatId", request.uat_id },
                { "@IssuingAuthority", request.xua_issuingauthority ?? string.Empty },
                { "@DateExpires", request.xua_dateexpires ?? "Not Applicable" }
            };

            if (newAttachmentId.HasValue)
            {
                parameters.Add("@NewAttId", newAttachmentId.Value);
            }

            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new InvalidOperationException("No connection string found");
            }

            using var connection = new SqlConnection(connectionString);
            connection.ConnectionString += ";Connection Timeout=30;";
            using var command = new SqlCommand(sql, connection);
            foreach (var param in parameters)
            {
                command.Parameters.AddWithValue(param.Key, param.Value ?? DBNull.Value);
            }

            await connection.OpenAsync();
            var rowsAffected = await command.ExecuteNonQueryAsync();

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateEmployeeAttachment",
                Detail = $"Updated employee attachment {xuaId} for user {userId}. Rows affected: {rowsAffected}. File replacement: {(newAttachmentId.HasValue ? "Yes" : "No")}",
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
                Description = "UpdateEmployeeAttachment",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating employee attachment {XuaId} for user {UserId}", xuaId, userId);
            throw;
        }
    }

    public async Task<List<CertificationsLicensingReportDto>> GetCertificationsLicensingReportAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    xua.xua_id,
                    xua.u_id,
                    CONCAT(u.u_firstname, ' ', u.u_lastname) AS EmployeeName,
                    u.u_employeenumber AS EmployeeNumber,
                    uat.uat_type AS AttachmentType,
                    xua.xua_description AS Description,
                    xua.xua_issuingauthority AS IssuingAuthority,
                    xua.xua_dateexpires AS DateExpires,
                    xua.xua_insertdatetime AS DateLoaded,
                    a.att_filename AS AttachmentFilename,
                    a.att_id AS AttachmentId
                FROM dbo.xrefuserattachment xua
                INNER JOIN dbo.[user] u ON xua.u_id = u.u_id
                INNER JOIN dbo.attachment a ON xua.att_id = a.att_id
                INNER JOIN dbo.userattachmenttype uat ON xua.uat_id = uat.uat_id
                ORDER BY u.u_firstname, u.u_lastname, xua.xua_insertdatetime DESC";

            var dt = await ExecuteQueryAsync(sql, new Dictionary<string, object>());
            
            var result = new List<CertificationsLicensingReportDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new CertificationsLicensingReportDto
                {
                    xua_id = ConvertToInt(row["xua_id"]),
                    u_id = ConvertToInt(row["u_id"]),
                    EmployeeName = row["EmployeeName"]?.ToString() ?? string.Empty,
                    EmployeeNumber = row["EmployeeNumber"]?.ToString() ?? string.Empty,
                    AttachmentType = row["AttachmentType"]?.ToString() ?? string.Empty,
                    Description = row["Description"]?.ToString() ?? string.Empty,
                    IssuingAuthority = row["IssuingAuthority"]?.ToString() ?? string.Empty,
                    DateExpires = row["DateExpires"]?.ToString() ?? string.Empty,
                    DateLoaded = ConvertToDateTime(row["DateLoaded"]),
                    AttachmentFilename = row["AttachmentFilename"]?.ToString() ?? string.Empty,
                    AttachmentId = ConvertToInt(row["AttachmentId"])
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCertificationsLicensingReport",
                Detail = $"Retrieved {result.Count} certification and licensing records",
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
                Description = "GetCertificationsLicensingReport",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving certifications and licensing report");
            throw;
        }
    }

    public async Task<List<CertificationsLicensingReportDto>> GetTechCertificationsLicensingReportAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    xua.xua_id,
                    xua.u_id,
                    CONCAT(u.u_firstname, ' ', u.u_lastname) AS EmployeeName,
                    u.u_employeenumber AS EmployeeNumber,
                    uat.uat_type AS AttachmentType,
                    xua.xua_description AS Description,
                    xua.xua_issuingauthority AS IssuingAuthority,
                    xua.xua_dateexpires AS DateExpires,
                    xua.xua_insertdatetime AS DateLoaded,
                    a.att_filename AS AttachmentFilename,
                    a.att_id AS AttachmentId
                FROM dbo.xrefuserattachment xua
                INNER JOIN dbo.[user] u ON xua.u_id = u.u_id
                INNER JOIN dbo.attachment a ON xua.att_id = a.att_id
                INNER JOIN dbo.userattachmenttype uat ON xua.uat_id = uat.uat_id
                WHERE xua.u_id = @UserId
                ORDER BY u.u_firstname, u.u_lastname, xua.xua_insertdatetime DESC";

            var parameters = new Dictionary<string, object> { { "@UserId", userId } };
            var dt = await ExecuteQueryAsync(sql, parameters);
            
            var result = new List<CertificationsLicensingReportDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new CertificationsLicensingReportDto
                {
                    xua_id = ConvertToInt(row["xua_id"]),
                    u_id = ConvertToInt(row["u_id"]),
                    EmployeeName = row["EmployeeName"]?.ToString() ?? string.Empty,
                    EmployeeNumber = row["EmployeeNumber"]?.ToString() ?? string.Empty,
                    AttachmentType = row["AttachmentType"]?.ToString() ?? string.Empty,
                    Description = row["Description"]?.ToString() ?? string.Empty,
                    IssuingAuthority = row["IssuingAuthority"]?.ToString() ?? string.Empty,
                    DateExpires = row["DateExpires"]?.ToString() ?? string.Empty,
                    DateLoaded = ConvertToDateTime(row["DateLoaded"]),
                    AttachmentFilename = row["AttachmentFilename"]?.ToString() ?? string.Empty,
                    AttachmentId = ConvertToInt(row["AttachmentId"])
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTechCertificationsLicensingReport",
                Detail = $"Retrieved {result.Count} certification and licensing records for user {userId}",
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
                Description = "GetTechCertificationsLicensingReport",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving tech certifications and licensing report");
            throw;
        }
    }

    #endregion

    #region Company Trades Management

    public async Task<List<LaborRateDto>> GetCompanyTradesAsync(int xcccId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    lr.lr_id,
                    lr.xccc_id,
                    lr.t_id,
                    t.t_trade AS TradeName,
                    t.t_description AS TradeDescription,
                    tp.t_trade AS ParentTradeName,
                    t.t_nte AS TNte,
                    lr.lr_descriptionoverride,
                    lr.lr_nte,
                    lr.lr_rateregular,
                    lr.lr_rateovertime,
                    lr.lr_rateholiday,
                    lr.lr_ratespecial,
                    lr.lr_ratescheduledafterhours,
                    lr.lr_rateregulardiscount,
                    lr.lr_rateregulardiscounthourslimit,
                    lr.lr_ratehelper,
                    lr.lr_ratehelperovertime,
                    lr.lr_rateflat,
                    lr.lr_flatorhourly,
                    lr.lr_tripcharge,
                    lr.lr_markup,
                    lr.lr_note,
                    lr.lr_insertdatetime,
                    lr.lr_modifieddatetime,
                    t.t_active
                FROM dbo.LaborRate lr
                INNER JOIN dbo.Trade t ON lr.t_id = t.t_id
                LEFT JOIN dbo.Trade tp ON t.t_id_parent = tp.t_id
                WHERE lr.xccc_id = @xcccId
                ORDER BY ISNULL(tp.t_trade, ''), t.t_trade";

            var parameters = new Dictionary<string, object>
            {
                ["@xcccId"] = xcccId
            };

            var dt = await ExecuteQueryAsync(sql, parameters);
            
            var result = new List<LaborRateDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new LaborRateDto
                {
                    LrId = ConvertToInt(row["lr_id"]),
                    XcccId = ConvertToInt(row["xccc_id"]),
                    TId = ConvertToInt(row["t_id"]),
                    TradeName = row["TradeName"]?.ToString() ?? string.Empty,
                    TradeDescription = row["TradeDescription"]?.ToString(),
                    ParentTradeName = row["ParentTradeName"]?.ToString(),
                    TNte = ConvertToNullableInt(row["TNte"]),
                    LrDescriptionOverride = row["lr_descriptionoverride"]?.ToString(),
                    LrNte = ConvertToNullableInt(row["lr_nte"]),
                    LrRateRegular = ConvertToNullableDecimal(row["lr_rateregular"]),
                    LrRateOvertime = ConvertToNullableDecimal(row["lr_rateovertime"]),
                    LrRateHoliday = ConvertToNullableDecimal(row["lr_rateholiday"]),
                    LrRateSpecial = ConvertToNullableDecimal(row["lr_ratespecial"]),
                    LrRateScheduledAfterHours = ConvertToNullableDecimal(row["lr_ratescheduledafterhours"]),
                    LrRateRegularDiscount = ConvertToNullableDecimal(row["lr_rateregulardiscount"]),
                    LrRateRegularDiscountHoursLimit = ConvertToNullableDecimal(row["lr_rateregulardiscounthourslimit"]),
                    LrRateHelper = ConvertToNullableDecimal(row["lr_ratehelper"]),
                    LrRateHelperOvertime = ConvertToNullableDecimal(row["lr_ratehelperovertime"]),
                    LrRateFlat = ConvertToNullableDecimal(row["lr_rateflat"]),
                    LrFlatOrHourly = row["lr_flatorhourly"]?.ToString(),
                    LrTripCharge = ConvertToNullableDecimal(row["lr_tripcharge"]),
                    LrMarkup = ConvertToNullableInt(row["lr_markup"]),
                    LrNote = row["lr_note"]?.ToString(),
                    TActive = ConvertToBool(row["t_active"]),
                    LrInsertDateTime = ConvertToDateTime(row["lr_insertdatetime"]),
                    LrModifiedDateTime = ConvertToNullableDateTime(row["lr_modifieddatetime"])
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCompanyTrades",
                Detail = $"Retrieved {result.Count} labor rates for company xcccId {xcccId}",
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
                Description = "GetCompanyTrades",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving company trades for xcccId {XcccId}", xcccId);
            throw;
        }
    }

    public async Task<List<CompanyTradeDto>> GetAvailableTradesForCompanyAsync(int xcccId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    t.t_id,
                    t.t_id_parent,
                    t.t_trade AS TradeName,
                    t.t_description AS TradeDescription,
                    tp.t_trade AS ParentTradeName,
                    t.t_nte,
                    t.t_parentonly,
                    t.t_highvolume
                FROM dbo.Trade t
                LEFT JOIN dbo.Trade tp ON t.t_id_parent = tp.t_id
                WHERE t.t_id_parent IS NOT NULL
                  AND t.t_id NOT IN (
                      SELECT t_id FROM dbo.LaborRate WHERE xccc_id = @xcccId
                  )
                ORDER BY ISNULL(tp.t_trade, ''), t.t_trade";

            var parameters = new Dictionary<string, object>
            {
                ["@xcccId"] = xcccId
            };

            var dt = await ExecuteQueryAsync(sql, parameters);
            
            var result = new List<CompanyTradeDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new CompanyTradeDto
                {
                    TId = ConvertToInt(row["t_id"]),
                    TIdParent = ConvertToNullableInt(row["t_id_parent"]),
                    TradeName = row["TradeName"]?.ToString() ?? string.Empty,
                    TradeDescription = row["TradeDescription"]?.ToString(),
                    ParentTradeName = row["ParentTradeName"]?.ToString(),
                    TNte = ConvertToNullableInt(row["t_nte"]),
                    TParentOnly = ConvertToBool(row["t_parentonly"]),
                    THighVolume = ConvertToBool(row["t_highvolume"])
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAvailableTradesForCompany",
                Detail = $"Retrieved {result.Count} available trades for company xcccId {xcccId}",
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
                Description = "GetAvailableTradesForCompany",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving available trades for xcccId {XcccId}", xcccId);
            throw;
        }
    }

    public async Task<List<CheckListDto>> GetCompanyChecklistsAsync(int xcccId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    cl.cl_id,
                    cl.xccc_id,
                    cl.clt_id,
                    cl.cl_name,
                    cl.cl_publicforquote,
                    cl.cl_publicforinvoice,
                    cl.cl_active,
                    clt.clt_type
                FROM dbo.CheckList cl
                LEFT JOIN dbo.CheckListType clt ON cl.clt_id = clt.clt_id
                WHERE cl.xccc_id = @xcccId
                ORDER BY cl.cl_name";

            var parameters = new Dictionary<string, object>
            {
                ["@xcccId"] = xcccId
            };

            var dt = await ExecuteQueryAsync(sql, parameters);
            
            var result = new List<CheckListDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new CheckListDto
                {
                    ClId = ConvertToInt(row["cl_id"]),
                    XcccId = ConvertToInt(row["xccc_id"]),
                    CltId = ConvertToInt(row["clt_id"]),
                    ClName = row["cl_name"]?.ToString() ?? string.Empty,
                    ClPublicForQuote = ConvertToBool(row["cl_publicforquote"]),
                    ClPublicForInvoice = ConvertToBool(row["cl_publicforinvoice"]),
                    ClActive = ConvertToBool(row["cl_active"]),
                    CltType = row["clt_type"]?.ToString()
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCompanyChecklists",
                Detail = $"Retrieved {result.Count} checklists for company xcccId {xcccId}",
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
                Description = "GetCompanyChecklists",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving checklists for xcccId {XcccId}", xcccId);
            throw;
        }
    }

    public async Task<List<CheckListDto>> GetCompanyChecklistsWithQuestionsAsync(int xcccId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // First get all checklists
            var checklists = await GetCompanyChecklistsAsync(xcccId);
            
            // Then get all questions for all checklists in one query
            const string questionsSql = @"
                SELECT 
                    clq.clq_id,
                    clq.cl_id,
                    clq.clat_id,
                    clq.clq_question,
                    clq.clq_order,
                    clq.clq_required,
                    clq.clq_answervalues,
                    clat.clat_type,
                    clq.clq_skip_answer,
                    clq.clq_skip_to_order
                FROM dbo.CheckListQuestion clq
                INNER JOIN dbo.CheckListAnswerType clat ON clq.clat_id = clat.clat_id
                INNER JOIN dbo.CheckList cl ON clq.cl_id = cl.cl_id
                WHERE cl.xccc_id = @xcccId
                ORDER BY clq.cl_id, clq.clq_order";

            var parameters = new Dictionary<string, object>
            {
                ["@xcccId"] = xcccId
            };

            var dt = await ExecuteQueryAsync(questionsSql, parameters);
            
            var questionsMap = new Dictionary<int, List<CheckListQuestionDto>>();
            foreach (DataRow row in dt.Rows)
            {
                var clId = ConvertToInt(row["cl_id"]);
                if (!questionsMap.ContainsKey(clId))
                {
                    questionsMap[clId] = new List<CheckListQuestionDto>();
                }
                questionsMap[clId].Add(new CheckListQuestionDto
                {
                    ClqId = ConvertToInt(row["clq_id"]),
                    ClId = clId,
                    ClatId = ConvertToInt(row["clat_id"]),
                    ClqQuestion = row["clq_question"]?.ToString() ?? string.Empty,
                    ClqOrder = ConvertToNullableInt(row["clq_order"]),
                    ClqRequired = ConvertToBool(row["clq_required"]),
                    ClqAnswerValues = row["clq_answervalues"]?.ToString(),
                    ClatType = row["clat_type"]?.ToString(),
                    ClqSkipAnswer = row["clq_skip_answer"]?.ToString(),
                    ClqSkipToOrder = ConvertToNullableInt(row["clq_skip_to_order"])
                });
            }

            // Attach questions to checklists
            foreach (var cl in checklists)
            {
                cl.Questions = questionsMap.ContainsKey(cl.ClId) ? questionsMap[cl.ClId] : new List<CheckListQuestionDto>();
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCompanyChecklistsWithQuestions",
                Detail = $"Retrieved {checklists.Count} checklists with questions for xcccId {xcccId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return checklists;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCompanyChecklistsWithQuestions",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving checklists with questions for xcccId {XcccId}", xcccId);
            throw;
        }
    }

    public async Task<List<CheckListTypeDto>> GetCheckListTypesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"SELECT clt_id, clt_type FROM dbo.CheckListType ORDER BY clt_type";

            var dt = await ExecuteQueryAsync(sql, new Dictionary<string, object>());
            
            var result = new List<CheckListTypeDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new CheckListTypeDto
                {
                    CltId = ConvertToInt(row["clt_id"]),
                    CltType = row["clt_type"]?.ToString() ?? string.Empty
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCheckListTypes",
                Detail = $"Retrieved {result.Count} checklist types",
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
                Description = "GetCheckListTypes",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving checklist types");
            throw;
        }
    }

    public async Task<List<CheckListAnswerTypeDto>> GetCheckListAnswerTypesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"SELECT clat_id, clat_type FROM dbo.CheckListAnswerType ORDER BY clat_type";

            var dt = await ExecuteQueryAsync(sql, new Dictionary<string, object>());
            
            var result = new List<CheckListAnswerTypeDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new CheckListAnswerTypeDto
                {
                    ClatId = ConvertToInt(row["clat_id"]),
                    ClatType = row["clat_type"]?.ToString() ?? string.Empty
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCheckListAnswerTypes",
                Detail = $"Retrieved {result.Count} checklist answer types",
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
                Description = "GetCheckListAnswerTypes",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving checklist answer types");
            throw;
        }
    }

    public async Task<CheckListDto> CreateCheckListAsync(int xcccId, CreateCheckListRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.CheckList (xccc_id, clt_id, cl_name, cl_publicforquote, cl_publicforinvoice, cl_active, cl_insertdatetime)
                VALUES (@xcccId, @cltId, @clName, @clPublicForQuote, @clPublicForInvoice, @clActive, GETDATE());
                SELECT CAST(SCOPE_IDENTITY() AS INT);";

            int clId;
            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@xcccId", SqlDbType.Int).Value = xcccId;
                command.Parameters.Add("@cltId", SqlDbType.Int).Value = request.CltId;
                command.Parameters.Add("@clName", SqlDbType.VarChar, 50).Value = request.ClName;
                command.Parameters.Add("@clPublicForQuote", SqlDbType.Bit).Value = request.ClPublicForQuote;
                command.Parameters.Add("@clPublicForInvoice", SqlDbType.Bit).Value = request.ClPublicForInvoice;
                command.Parameters.Add("@clActive", SqlDbType.Bit).Value = request.ClActive;

                await connection.OpenAsync();
                clId = (int)await command.ExecuteScalarAsync();
            }

            // Retrieve the created checklist
            var checklists = await GetCompanyChecklistsAsync(xcccId);
            var newChecklist = checklists.FirstOrDefault(c => c.ClId == clId);

            if (newChecklist == null)
            {
                throw new Exception("Failed to retrieve newly created checklist");
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateCheckList",
                Detail = $"Created checklist '{request.ClName}' with ID {clId} for xcccId {xcccId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return newChecklist;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateCheckList",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating checklist for xcccId {XcccId}", xcccId);
            throw;
        }
    }

    public async Task<CheckListDto?> UpdateCheckListAsync(int clId, UpdateCheckListRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.CheckList
                SET cl_name = @clName,
                    clt_id = @cltId,
                    cl_publicforquote = @clPublicForQuote,
                    cl_publicforinvoice = @clPublicForInvoice,
                    cl_active = @clActive,
                    cl_modifieddatetime = GETDATE()
                WHERE cl_id = @clId";

            var parameters = new Dictionary<string, object>
            {
                ["@clId"] = clId,
                ["@clName"] = request.ClName,
                ["@cltId"] = request.CltId,
                ["@clPublicForQuote"] = request.ClPublicForQuote,
                ["@clPublicForInvoice"] = request.ClPublicForInvoice,
                ["@clActive"] = request.ClActive
            };

            await ExecuteQueryAsync(sql, parameters);

            // Get xccc_id to reload
            const string getXcccSql = "SELECT xccc_id FROM dbo.CheckList WHERE cl_id = @clId";
            var xcccParams = new Dictionary<string, object> { ["@clId"] = clId };
            var xcccDt = await ExecuteQueryAsync(getXcccSql, xcccParams);
            
            if (xcccDt.Rows.Count == 0) return null;
            
            var xcccId = ConvertToInt(xcccDt.Rows[0]["xccc_id"]);
            var checklists = await GetCompanyChecklistsAsync(xcccId);
            var updatedChecklist = checklists.FirstOrDefault(c => c.ClId == clId);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCheckList",
                Detail = $"Updated checklist {clId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return updatedChecklist;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCheckList",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating checklist {ClId}", clId);
            throw;
        }
    }

    public async Task<CheckListQuestionDto> CreateCheckListQuestionAsync(int clId, CreateCheckListQuestionRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }
        
        try
        {
            const string sql = @"
                INSERT INTO dbo.CheckListQuestion (cl_id, clat_id, clq_question, clq_order, clq_required, clq_answervalues, clq_skip_answer, clq_skip_to_order, clq_insertdatetime)
                VALUES (@clId, @clatId, @clqQuestion, @clqOrder, @clqRequired, @clqAnswerValues, @clqSkipAnswer, @clqSkipToOrder, GETDATE());
                SELECT CAST(SCOPE_IDENTITY() AS INT);";

            int clqId;
            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@clId", SqlDbType.Int).Value = clId;
                command.Parameters.Add("@clatId", SqlDbType.Int).Value = request.ClatId;
                command.Parameters.Add("@clqQuestion", SqlDbType.VarChar, 800).Value = request.ClqQuestion;
                command.Parameters.Add("@clqOrder", SqlDbType.Int).Value = (object?)request.ClqOrder ?? DBNull.Value;
                command.Parameters.Add("@clqRequired", SqlDbType.Bit).Value = request.ClqRequired;
                command.Parameters.Add("@clqAnswerValues", SqlDbType.VarChar, 8000).Value = (object?)request.ClqAnswerValues ?? DBNull.Value;
                command.Parameters.Add("@clqSkipAnswer", SqlDbType.NVarChar, 500).Value = (object?)request.ClqSkipAnswer ?? DBNull.Value;
                command.Parameters.Add("@clqSkipToOrder", SqlDbType.Int).Value = (object?)request.ClqSkipToOrder ?? DBNull.Value;

                await connection.OpenAsync();
                clqId = (int)await command.ExecuteScalarAsync();
            }

            // Retrieve the created question
            const string getQuestionSql = @"
                SELECT
                    clq.clq_id, clq.cl_id, clq.clat_id, clq.clq_question,
                    clq.clq_order, clq.clq_required, clq.clq_answervalues,
                    clat.clat_type, clq.clq_skip_answer, clq.clq_skip_to_order
                FROM dbo.CheckListQuestion clq
                INNER JOIN dbo.CheckListAnswerType clat ON clq.clat_id = clat.clat_id
                WHERE clq.clq_id = @clqId";

            var qParams = new Dictionary<string, object> { ["@clqId"] = clqId };
            var dt = await ExecuteQueryAsync(getQuestionSql, qParams);
            
            if (dt.Rows.Count == 0)
            {
                throw new Exception("Failed to retrieve newly created question");
            }

            var row = dt.Rows[0];
            var question = new CheckListQuestionDto
            {
                ClqId = ConvertToInt(row["clq_id"]),
                ClId = ConvertToInt(row["cl_id"]),
                ClatId = ConvertToInt(row["clat_id"]),
                ClqQuestion = row["clq_question"]?.ToString() ?? string.Empty,
                ClqOrder = ConvertToNullableInt(row["clq_order"]),
                ClqRequired = ConvertToBool(row["clq_required"]),
                ClqAnswerValues = row["clq_answervalues"]?.ToString(),
                ClatType = row["clat_type"]?.ToString(),
                ClqSkipAnswer = row["clq_skip_answer"]?.ToString(),
                ClqSkipToOrder = ConvertToNullableInt(row["clq_skip_to_order"])
            };

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateCheckListQuestion",
                Detail = $"Created question {clqId} for checklist {clId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return question;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateCheckListQuestion",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating question for checklist {ClId}", clId);
            throw;
        }
    }

    public async Task<CheckListQuestionDto?> UpdateCheckListQuestionAsync(int clqId, UpdateCheckListQuestionRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.CheckListQuestion
                SET clat_id = @clatId,
                    clq_question = @clqQuestion,
                    clq_order = @clqOrder,
                    clq_required = @clqRequired,
                    clq_answervalues = @clqAnswerValues,
                    clq_skip_answer = @clqSkipAnswer,
                    clq_skip_to_order = @clqSkipToOrder,
                    clq_modifieddatetime = GETDATE()
                WHERE clq_id = @clqId";

            var parameters = new Dictionary<string, object>
            {
                ["@clqId"] = clqId,
                ["@clatId"] = request.ClatId,
                ["@clqQuestion"] = request.ClqQuestion,
                ["@clqOrder"] = (object?)request.ClqOrder ?? DBNull.Value,
                ["@clqRequired"] = request.ClqRequired,
                ["@clqAnswerValues"] = (object?)request.ClqAnswerValues ?? DBNull.Value,
                ["@clqSkipAnswer"] = (object?)request.ClqSkipAnswer ?? DBNull.Value,
                ["@clqSkipToOrder"] = (object?)request.ClqSkipToOrder ?? DBNull.Value
            };

            await ExecuteQueryAsync(sql, parameters);

            // Retrieve the updated question
            const string getQuestionSql = @"
                SELECT
                    clq.clq_id, clq.cl_id, clq.clat_id, clq.clq_question,
                    clq.clq_order, clq.clq_required, clq.clq_answervalues,
                    clat.clat_type, clq.clq_skip_answer, clq.clq_skip_to_order
                FROM dbo.CheckListQuestion clq
                INNER JOIN dbo.CheckListAnswerType clat ON clq.clat_id = clat.clat_id
                WHERE clq.clq_id = @clqId";

            var qParams = new Dictionary<string, object> { ["@clqId"] = clqId };
            var dt = await ExecuteQueryAsync(getQuestionSql, qParams);

            if (dt.Rows.Count == 0) return null;

            var row = dt.Rows[0];
            var question = new CheckListQuestionDto
            {
                ClqId = ConvertToInt(row["clq_id"]),
                ClId = ConvertToInt(row["cl_id"]),
                ClatId = ConvertToInt(row["clat_id"]),
                ClqQuestion = row["clq_question"]?.ToString() ?? string.Empty,
                ClqOrder = ConvertToNullableInt(row["clq_order"]),
                ClqRequired = ConvertToBool(row["clq_required"]),
                ClqAnswerValues = row["clq_answervalues"]?.ToString(),
                ClatType = row["clat_type"]?.ToString(),
                ClqSkipAnswer = row["clq_skip_answer"]?.ToString(),
                ClqSkipToOrder = ConvertToNullableInt(row["clq_skip_to_order"])
            };

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCheckListQuestion",
                Detail = $"Updated question {clqId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return question;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCheckListQuestion",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating question {ClqId}", clqId);
            throw;
        }
    }

    public async Task CloneCheckListsAsync(int sourceXcccId, int targetXcccId, List<int>? checklistIds = null)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            // Get source checklists with questions
            var sourceChecklists = await GetCompanyChecklistsWithQuestionsAsync(sourceXcccId);

            if (checklistIds != null && checklistIds.Count > 0)
            {
                var idSet = new HashSet<int>(checklistIds);
                sourceChecklists = sourceChecklists.Where(cl => idSet.Contains(cl.ClId)).ToList();
            }
            
            foreach (var checklist in sourceChecklists)
            {
                // Create the checklist in the target company
                var createRequest = new CreateCheckListRequest
                {
                    CltId = checklist.CltId,
                    ClName = checklist.ClName,
                    ClPublicForQuote = checklist.ClPublicForQuote,
                    ClPublicForInvoice = checklist.ClPublicForInvoice,
                    ClActive = checklist.ClActive
                };

                var newChecklist = await CreateCheckListAsync(targetXcccId, createRequest);

                // Clone all questions
                if (checklist.Questions != null)
                {
                    foreach (var question in checklist.Questions)
                    {
                        var questionRequest = new CreateCheckListQuestionRequest
                        {
                            ClatId = question.ClatId,
                            ClqQuestion = question.ClqQuestion,
                            ClqOrder = question.ClqOrder,
                            ClqRequired = question.ClqRequired,
                            ClqAnswerValues = question.ClqAnswerValues,
                            ClqSkipAnswer = question.ClqSkipAnswer,
                            ClqSkipToOrder = question.ClqSkipToOrder
                        };

                        await CreateCheckListQuestionAsync(newChecklist.ClId, questionRequest);
                    }
                }
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CloneCheckLists",
                Detail = $"Cloned {sourceChecklists.Count} checklists from xcccId {sourceXcccId} to {targetXcccId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CloneCheckLists",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error cloning checklists from xcccId {Source} to {Target}", sourceXcccId, targetXcccId);
            throw;
        }
    }

    public async Task<LaborRateDto> CreateCompanyTradeAsync(int xcccId, CreateLaborRateRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // Validate required fields
            if (string.IsNullOrWhiteSpace(request.LrFlatOrHourly))
            {
                throw new ArgumentException("Flat/Hourly selection is required");
            }

            const string sql = @"
                INSERT INTO dbo.LaborRate (
                    xccc_id, t_id, lr_descriptionoverride, lr_nte,
                    lr_rateregular, lr_rateovertime, lr_rateholiday, lr_ratespecial,
                    lr_ratescheduledafterhours, lr_rateregulardiscount, lr_rateregulardiscounthourslimit,
                    lr_ratehelper, lr_ratehelperovertime, lr_rateflat, lr_flatorhourly,
                    lr_tripcharge, lr_markup, lr_note, lr_insertdatetime
                ) VALUES (
                    @xcccId, @tId, @lrDescriptionOverride, @lrNte,
                    @lrRateRegular, @lrRateOvertime, @lrRateHoliday, @lrRateSpecial,
                    @lrRateScheduledAfterHours, @lrRateRegularDiscount, @lrRateRegularDiscountHoursLimit,
                    @lrRateHelper, @lrRateHelperOvertime, @lrRateFlat, @lrFlatOrHourly,
                    @lrTripCharge, @lrMarkup, @lrNote, GETDATE()
                );
                SELECT CAST(SCOPE_IDENTITY() AS INT);";

            var parameters = new Dictionary<string, object>
            {
                ["@xcccId"] = xcccId,
                ["@tId"] = request.TId,
                ["@lrDescriptionOverride"] = (object?)request.LrDescriptionOverride ?? DBNull.Value,
                ["@lrNte"] = (object?)request.LrNte ?? DBNull.Value,
                ["@lrRateRegular"] = (object?)request.LrRateRegular ?? DBNull.Value,
                ["@lrRateOvertime"] = (object?)request.LrRateOvertime ?? DBNull.Value,
                ["@lrRateHoliday"] = (object?)request.LrRateHoliday ?? DBNull.Value,
                ["@lrRateSpecial"] = (object?)request.LrRateSpecial ?? DBNull.Value,
                ["@lrRateScheduledAfterHours"] = (object?)request.LrRateScheduledAfterHours ?? DBNull.Value,
                ["@lrRateRegularDiscount"] = (object?)request.LrRateRegularDiscount ?? DBNull.Value,
                ["@lrRateRegularDiscountHoursLimit"] = (object?)request.LrRateRegularDiscountHoursLimit ?? DBNull.Value,
                ["@lrRateHelper"] = (object?)request.LrRateHelper ?? DBNull.Value,
                ["@lrRateHelperOvertime"] = (object?)request.LrRateHelperOvertime ?? DBNull.Value,
                ["@lrRateFlat"] = (object?)request.LrRateFlat ?? DBNull.Value,
                ["@lrFlatOrHourly"] = request.LrFlatOrHourly,
                ["@lrTripCharge"] = (object?)request.LrTripCharge ?? DBNull.Value,
                ["@lrMarkup"] = (object?)request.LrMarkup ?? DBNull.Value,
                ["@lrNote"] = (object?)request.LrNote ?? DBNull.Value
            };

            var dt = await ExecuteQueryAsync(sql, parameters);
            var lrId = dt.Rows.Count > 0 ? ConvertToInt(dt.Rows[0][0]) : 0;

            if (lrId == 0)
            {
                throw new Exception("Failed to create labor rate");
            }

            // Retrieve the newly created labor rate
            var laborRates = await GetCompanyTradesAsync(xcccId);
            var newLaborRate = laborRates.FirstOrDefault(lr => lr.LrId == lrId);

            if (newLaborRate == null)
            {
                throw new Exception("Failed to retrieve newly created labor rate");
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateCompanyTrade",
                Detail = $"Created labor rate {lrId} for company xcccId {xcccId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return newLaborRate;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateCompanyTrade",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating labor rate for xcccId {XcccId}", xcccId);
            throw;
        }
    }

    public async Task<(LaborRateDto? LaborRate, string? CompanyName, string? TradeName)> GetLaborRateWithCompanyByIdAsync(int lrId)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string sql = @"
                SELECT 
                    lr.lr_id, lr.xccc_id, lr.t_id,
                    t.t_trade, t.t_description,
                    pt.t_trade AS parent_trade_name,
                    t.t_nte,
                    lr.lr_descriptionoverride, lr.lr_nte,
                    lr.lr_rateregular, lr.lr_rateovertime, lr.lr_rateholiday, lr.lr_ratespecial,
                    lr.lr_ratescheduledafterhours, lr.lr_rateregulardiscount, lr.lr_rateregulardiscounthourslimit,
                    lr.lr_ratehelper, lr.lr_ratehelperovertime, lr.lr_rateflat, lr.lr_flatorhourly,
                    lr.lr_tripcharge, lr.lr_markup, lr.lr_note,
                    lr.lr_insertdatetime, lr.lr_modifieddatetime,
                    c.c_name
                FROM dbo.LaborRate lr
                INNER JOIN dbo.Trade t ON lr.t_id = t.t_id
                LEFT JOIN dbo.Trade pt ON t.t_id_parent = pt.t_id
                INNER JOIN xrefCompanyCallCenter xccc ON lr.xccc_id = xccc.xccc_id
                INNER JOIN company c ON xccc.c_id = c.c_id
                WHERE lr.lr_id = @lrId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@lrId", SqlDbType.Int).Value = lrId;
                await connection.OpenAsync();

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        var laborRate = new LaborRateDto
                        {
                            LrId = reader.GetInt32(0),
                            XcccId = reader.GetInt32(1),
                            TId = reader.GetInt32(2),
                            TradeName = reader.IsDBNull(3) ? null : reader.GetString(3),
                            TradeDescription = reader.IsDBNull(4) ? null : reader.GetString(4),
                            ParentTradeName = reader.IsDBNull(5) ? null : reader.GetString(5),
                            TNte = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                            LrDescriptionOverride = reader.IsDBNull(7) ? null : reader.GetString(7),
                            LrNte = reader.IsDBNull(8) ? null : reader.GetInt32(8),
                            LrRateRegular = reader.IsDBNull(9) ? null : reader.GetDecimal(9),
                            LrRateOvertime = reader.IsDBNull(10) ? null : reader.GetDecimal(10),
                            LrRateHoliday = reader.IsDBNull(11) ? null : reader.GetDecimal(11),
                            LrRateSpecial = reader.IsDBNull(12) ? null : reader.GetDecimal(12),
                            LrRateScheduledAfterHours = reader.IsDBNull(13) ? null : reader.GetDecimal(13),
                            LrRateRegularDiscount = reader.IsDBNull(14) ? null : reader.GetDecimal(14),
                            LrRateRegularDiscountHoursLimit = reader.IsDBNull(15) ? null : reader.GetDecimal(15),
                            LrRateHelper = reader.IsDBNull(16) ? null : reader.GetDecimal(16),
                            LrRateHelperOvertime = reader.IsDBNull(17) ? null : reader.GetDecimal(17),
                            LrRateFlat = reader.IsDBNull(18) ? null : reader.GetDecimal(18),
                            LrFlatOrHourly = reader.IsDBNull(19) ? null : reader.GetString(19),
                            LrTripCharge = reader.IsDBNull(20) ? null : reader.GetDecimal(20),
                            LrMarkup = reader.IsDBNull(21) ? null : reader.GetInt32(21),
                            LrNote = reader.IsDBNull(22) ? null : reader.GetString(22),
                            LrInsertDateTime = reader.GetDateTime(23),
                            LrModifiedDateTime = reader.IsDBNull(24) ? null : reader.GetDateTime(24)
                        };
                        var companyName = reader.IsDBNull(25) ? null : reader.GetString(25);
                        var tradeName = reader.IsDBNull(3) ? null : reader.GetString(3);
                        return (laborRate, companyName, tradeName);
                    }
                }
            }

            return (null, null, null);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving labor rate with company lr_id {LrId}", lrId);
            throw;
        }
    }

    public async Task<List<string>> GetChecklistNamesByIdsAsync(int xcccId, List<int> checklistIds)
    {
        if (checklistIds == null || !checklistIds.Any())
        {
            return new List<string>();
        }

        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            var idList = string.Join(",", checklistIds);
            var sql = $@"
                SELECT cl_name
                FROM CheckList
                WHERE xccc_id = @xcccId AND cl_id IN ({idList})
                ORDER BY cl_name";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@xcccId", SqlDbType.Int).Value = xcccId;
                await connection.OpenAsync();

                var names = new List<string>();
                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        names.Add(reader.GetString(0));
                    }
                }
                return names;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving checklist names for xcccId {XcccId}", xcccId);
            throw;
        }
    }

    public async Task<LaborRateDto?> UpdateCompanyTradeAsync(int xcccId, int lrId, UpdateLaborRateRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                UPDATE dbo.LaborRate
                SET lr_descriptionoverride = @lrDescriptionOverride,
                    lr_nte = @lrNte,
                    lr_rateregular = @lrRateRegular,
                    lr_rateovertime = @lrRateOvertime,
                    lr_rateholiday = @lrRateHoliday,
                    lr_ratespecial = @lrRateSpecial,
                    lr_ratescheduledafterhours = @lrRateScheduledAfterHours,
                    lr_rateregulardiscount = @lrRateRegularDiscount,
                    lr_rateregulardiscounthourslimit = @lrRateRegularDiscountHoursLimit,
                    lr_ratehelper = @lrRateHelper,
                    lr_ratehelperovertime = @lrRateHelperOvertime,
                    lr_rateflat = @lrRateFlat,
                    lr_flatorhourly = @lrFlatOrHourly,
                    lr_tripcharge = @lrTripCharge,
                    lr_markup = @lrMarkup,
                    lr_note = @lrNote,
                    lr_modifieddatetime = GETDATE()
                WHERE lr_id = @lrId AND xccc_id = @xcccId";

            var parameters = new Dictionary<string, object>
            {
                ["@lrId"] = lrId,
                ["@xcccId"] = xcccId,
                ["@lrDescriptionOverride"] = (object?)request.LrDescriptionOverride ?? DBNull.Value,
                ["@lrNte"] = (object?)request.LrNte ?? DBNull.Value,
                ["@lrRateRegular"] = (object?)request.LrRateRegular ?? DBNull.Value,
                ["@lrRateOvertime"] = (object?)request.LrRateOvertime ?? DBNull.Value,
                ["@lrRateHoliday"] = (object?)request.LrRateHoliday ?? DBNull.Value,
                ["@lrRateSpecial"] = (object?)request.LrRateSpecial ?? DBNull.Value,
                ["@lrRateScheduledAfterHours"] = (object?)request.LrRateScheduledAfterHours ?? DBNull.Value,
                ["@lrRateRegularDiscount"] = (object?)request.LrRateRegularDiscount ?? DBNull.Value,
                ["@lrRateRegularDiscountHoursLimit"] = (object?)request.LrRateRegularDiscountHoursLimit ?? DBNull.Value,
                ["@lrRateHelper"] = (object?)request.LrRateHelper ?? DBNull.Value,
                ["@lrRateHelperOvertime"] = (object?)request.LrRateHelperOvertime ?? DBNull.Value,
                ["@lrRateFlat"] = (object?)request.LrRateFlat ?? DBNull.Value,
                ["@lrFlatOrHourly"] = (object?)request.LrFlatOrHourly ?? DBNull.Value,
                ["@lrTripCharge"] = (object?)request.LrTripCharge ?? DBNull.Value,
                ["@lrMarkup"] = (object?)request.LrMarkup ?? DBNull.Value,
                ["@lrNote"] = (object?)request.LrNote ?? DBNull.Value
            };

            await ExecuteQueryAsync(sql, parameters);

            // Retrieve the updated labor rate
            var laborRates = await GetCompanyTradesAsync(xcccId);
            var updatedLaborRate = laborRates.FirstOrDefault(lr => lr.LrId == lrId);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCompanyTrade",
                Detail = $"Updated labor rate {lrId} for company xcccId {xcccId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return updatedLaborRate;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCompanyTrade",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating labor rate {LrId} for xcccId {XcccId}", lrId, xcccId);
            throw;
        }
    }

    public async Task<List<int>> GetTradeChecklistsAsync(int lrId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT cl_id
                FROM dbo.xrefLaborRateCheckList
                WHERE lr_id = @lrId";

            var parameters = new Dictionary<string, object>
            {
                ["@lrId"] = lrId
            };

            var dt = await ExecuteQueryAsync(sql, parameters);
            
            var result = new List<int>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(ConvertToInt(row["cl_id"]));
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTradeChecklists",
                Detail = $"Retrieved {result.Count} checklists for labor rate {lrId}",
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
                Description = "GetTradeChecklists",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving checklists for labor rate {LrId}", lrId);
            throw;
        }
    }

    public async Task UpdateTradeChecklistsAsync(int lrId, List<int> checklistIds)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // Delete existing checklist associations
            const string deleteSql = @"
                DELETE FROM dbo.xrefLaborRateCheckList
                WHERE lr_id = @lrId";

            var deleteParams = new Dictionary<string, object>
            {
                ["@lrId"] = lrId
            };

            await ExecuteQueryAsync(deleteSql, deleteParams);

            // Insert new checklist associations
            if (checklistIds != null && checklistIds.Any())
            {
                foreach (var clId in checklistIds)
                {
                    const string insertSql = @"
                        INSERT INTO dbo.xrefLaborRateCheckList (lr_id, cl_id, xlrcl_insertdatetime)
                        VALUES (@lrId, @clId, GETDATE())";

                    var insertParams = new Dictionary<string, object>
                    {
                        ["@lrId"] = lrId,
                        ["@clId"] = clId
                    };

                    await ExecuteQueryAsync(insertSql, insertParams);
                }
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateTradeChecklists",
                Detail = $"Updated checklists for labor rate {lrId} with {checklistIds?.Count ?? 0} checklists",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateTradeChecklists",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating checklists for labor rate {LrId}", lrId);
            throw;
        }
    }

    #endregion

    #region Contact Management

    public async Task<List<ContactDto>> GetCompanyContactsAsync(int cId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    c.con_id,
                    c.o_id,
                    c.ct_id,
                    ct.ct_title,
                    c.con_firstname,
                    c.con_lastname,
                    c.con_email,
                    c.con_phone,
                    c.con_mobile,
                    c.con_fax,
                    c.con_insertdatetime,
                    c.con_modifieddatetime
                FROM contact c
                INNER JOIN xrefcompanycontact xcc ON c.con_id = xcc.con_id
                INNER JOIN contacttitle ct ON c.ct_id = ct.ct_id
                WHERE xcc.c_id = @cId
                ORDER BY ct.ct_title, c.con_lastname, c.con_firstname";

            var parameters = new Dictionary<string, object>
            {
                ["@cId"] = cId
            };

            var dt = await ExecuteQueryAsync(sql, parameters);
            
            var result = new List<ContactDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new ContactDto
                {
                    ConId = ConvertToInt(row["con_id"]),
                    OId = ConvertToNullableInt(row["o_id"]),
                    CtId = ConvertToInt(row["ct_id"]),
                    CtTitle = row["ct_title"]?.ToString(),
                    ConFirstname = row["con_firstname"]?.ToString(),
                    ConLastname = row["con_lastname"]?.ToString(),
                    ConEmail = row["con_email"]?.ToString(),
                    ConPhone = row["con_phone"]?.ToString(),
                    ConMobile = row["con_mobile"]?.ToString(),
                    ConFax = row["con_fax"]?.ToString(),
                    ConInsertDateTime = ConvertToDateTime(row["con_insertdatetime"]),
                    ConModifiedDateTime = ConvertToNullableDateTime(row["con_modifieddatetime"])
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCompanyContacts",
                Detail = $"Retrieved {result.Count} contacts for company c_id {cId}",
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
                Description = "GetCompanyContacts",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving contacts for company c_id {CId}", cId);
            throw;
        }
    }

    public async Task<List<ContactTitleDto>> GetContactTitlesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    ct_id,
                    o_id,
                    ct_title,
                    ct_insertdatetime,
                    ct_modifieddatetime,
                    ct_active
                FROM contacttitle
                WHERE ct_active = 1
                ORDER BY ct_title";

            var dt = await ExecuteQueryAsync(sql, new Dictionary<string, object>());
            
            var result = new List<ContactTitleDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new ContactTitleDto
                {
                    CtId = ConvertToInt(row["ct_id"]),
                    OId = ConvertToNullableInt(row["o_id"]),
                    CtTitle = row["ct_title"]?.ToString() ?? string.Empty,
                    CtInsertDateTime = ConvertToDateTime(row["ct_insertdatetime"]),
                    CtModifiedDateTime = ConvertToNullableDateTime(row["ct_modifieddatetime"]),
                    CtActive = ConvertToBool(row["ct_active"])
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetContactTitles",
                Detail = $"Retrieved {result.Count} active contact titles",
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
                Description = "GetContactTitles",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving contact titles");
            throw;
        }
    }

    public async Task<(ContactDto? Contact, string? CompanyName)> GetContactWithCompanyByIdAsync(int conId)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string sql = @"
                SELECT 
                    c.con_id, c.o_id, c.ct_id,
                    ct.ct_title,
                    c.con_firstname, c.con_lastname, c.con_email,
                    c.con_phone, c.con_mobile, c.con_fax,
                    c.con_insertdatetime, c.con_modifieddatetime,
                    co.c_name
                FROM contact c
                INNER JOIN contacttitle ct ON c.ct_id = ct.ct_id
                INNER JOIN xrefcompanycontact xcc ON c.con_id = xcc.con_id
                INNER JOIN company co ON xcc.c_id = co.c_id
                WHERE c.con_id = @conId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@conId", SqlDbType.Int).Value = conId;
                await connection.OpenAsync();

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        var contact = new ContactDto
                        {
                            ConId = reader.GetInt32(0),
                            OId = reader.IsDBNull(1) ? null : reader.GetInt32(1),
                            CtId = reader.GetInt32(2),
                            CtTitle = reader.IsDBNull(3) ? null : reader.GetString(3),
                            ConFirstname = reader.IsDBNull(4) ? null : reader.GetString(4),
                            ConLastname = reader.IsDBNull(5) ? null : reader.GetString(5),
                            ConEmail = reader.IsDBNull(6) ? null : reader.GetString(6),
                            ConPhone = reader.IsDBNull(7) ? null : reader.GetString(7),
                            ConMobile = reader.IsDBNull(8) ? null : reader.GetString(8),
                            ConFax = reader.IsDBNull(9) ? null : reader.GetString(9),
                            ConInsertDateTime = reader.GetDateTime(10),
                            ConModifiedDateTime = reader.IsDBNull(11) ? null : reader.GetDateTime(11)
                        };
                        var companyName = reader.IsDBNull(12) ? null : reader.GetString(12);
                        return (contact, companyName);
                    }
                }
            }

            return (null, null);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving contact with company con_id {ConId}", conId);
            throw;
        }
    }

    public async Task<ContactDto> CreateContactAsync(int cId, CreateContactRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }
        
        try
        {
            int conId;
            
            // Insert contact
            const string insertContactSql = @"
                INSERT INTO contact (
                    o_id, ct_id, con_firstname, con_lastname, con_email,
                    con_phone, con_mobile, con_fax, con_insertdatetime
                ) VALUES (
                    @oId, @ctId, @conFirstname, @conLastname, @conEmail,
                    @conPhone, @conMobile, @conFax, GETDATE()
                );
                SELECT CAST(SCOPE_IDENTITY() AS INT);";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(insertContactSql, connection))
            {
                command.Parameters.Add("@oId", SqlDbType.Int).Value = 1;
                command.Parameters.Add("@ctId", SqlDbType.Int).Value = request.CtId;
                command.Parameters.Add("@conFirstname", SqlDbType.VarChar, 50).Value = (object?)request.ConFirstname ?? DBNull.Value;
                command.Parameters.Add("@conLastname", SqlDbType.VarChar, 50).Value = (object?)request.ConLastname ?? DBNull.Value;
                command.Parameters.Add("@conEmail", SqlDbType.VarChar, 100).Value = (object?)request.ConEmail ?? DBNull.Value;
                command.Parameters.Add("@conPhone", SqlDbType.VarChar, 15).Value = (object?)request.ConPhone ?? DBNull.Value;
                command.Parameters.Add("@conMobile", SqlDbType.VarChar, 15).Value = (object?)request.ConMobile ?? DBNull.Value;
                command.Parameters.Add("@conFax", SqlDbType.VarChar, 15).Value = (object?)request.ConFax ?? DBNull.Value;

                await connection.OpenAsync();
                conId = (int)await command.ExecuteScalarAsync();
            }

            if (conId == 0)
            {
                throw new Exception("Failed to create contact");
            }

            // Create xref entry
            const string insertXrefSql = @"
                INSERT INTO xrefcompanycontact (c_id, con_id, xccon_insertdatetime)
                VALUES (@cId, @conId, GETDATE())";

            var xrefParams = new Dictionary<string, object>
            {
                ["@cId"] = cId,
                ["@conId"] = conId
            };

            await ExecuteQueryAsync(insertXrefSql, xrefParams);

            // Retrieve the newly created contact
            var contacts = await GetCompanyContactsAsync(cId);
            var newContact = contacts.FirstOrDefault(c => c.ConId == conId);

            if (newContact == null)
            {
                throw new Exception("Failed to retrieve newly created contact");
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateContact",
                Detail = $"Created contact {conId} for company c_id {cId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return newContact;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateContact",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating contact for company c_id {CId}", cId);
            throw;
        }
    }

    public async Task<ContactDto?> UpdateContactAsync(int conId, UpdateContactRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }
        
        try
        {
            // Get c_id from xref table
            const string getCIdSql = "SELECT c_id FROM xrefcompanycontact WHERE con_id = @conId";
            int cId;

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(getCIdSql, connection))
            {
                command.Parameters.Add("@conId", SqlDbType.Int).Value = conId;
                await connection.OpenAsync();
                var result = await command.ExecuteScalarAsync();
                if (result == null)
                {
                    throw new InvalidOperationException($"Contact {conId} not found in xref table");
                }
                cId = (int)result;
            }

            const string sql = @"
                UPDATE contact
                SET ct_id = @ctId,
                    con_firstname = @conFirstname,
                    con_lastname = @conLastname,
                    con_email = @conEmail,
                    con_phone = @conPhone,
                    con_mobile = @conMobile,
                    con_fax = @conFax,
                    con_modifieddatetime = GETDATE()
                WHERE con_id = @conId";

            var parameters = new Dictionary<string, object>
            {
                ["@conId"] = conId,
                ["@ctId"] = request.CtId,
                ["@conFirstname"] = (object?)request.ConFirstname ?? DBNull.Value,
                ["@conLastname"] = (object?)request.ConLastname ?? DBNull.Value,
                ["@conEmail"] = (object?)request.ConEmail ?? DBNull.Value,
                ["@conPhone"] = (object?)request.ConPhone ?? DBNull.Value,
                ["@conMobile"] = (object?)request.ConMobile ?? DBNull.Value,
                ["@conFax"] = (object?)request.ConFax ?? DBNull.Value
            };

            await ExecuteQueryAsync(sql, parameters);

            // Retrieve the updated contact
            var contacts = await GetCompanyContactsAsync(cId);
            var updatedContact = contacts.FirstOrDefault(c => c.ConId == conId);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateContact",
                Detail = $"Updated contact {conId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return updatedContact;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateContact",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating contact {ConId}", conId);
            throw;
        }
    }

    #endregion

    #region Address Management

    public async Task<List<AddressDto>> GetCompanyAddressesAsync(int cId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    a.a_id,
                    a.o_id,
                    a.at_id,
                    at.at_title,
                    a.a_insertdatetime,
                    a.a_modifieddatetime,
                    a.a_description,
                    a.a_address1,
                    a.a_address2,
                    a.a_city,
                    a.a_state,
                    a.a_zip,
                    a.a_latitude,
                    a.a_longitude,
                    a.a_picture,
                    a.a_active,
                    a.a_tempid
                FROM address a
                INNER JOIN xrefcompanyaddress xca ON a.a_id = xca.a_id
                INNER JOIN addresstitle at ON a.at_id = at.at_id
                WHERE xca.c_id = @cId
                ORDER BY at.at_title, a.a_address1";

            var parameters = new Dictionary<string, object>
            {
                ["@cId"] = cId
            };

            var dt = await ExecuteQueryAsync(sql, parameters);
            
            var result = new List<AddressDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new AddressDto
                {
                    AId = ConvertToInt(row["a_id"]),
                    OId = ConvertToInt(row["o_id"]),
                    AtId = ConvertToInt(row["at_id"]),
                    AtTitle = row["at_title"]?.ToString(),
                    AInsertDateTime = ConvertToDateTime(row["a_insertdatetime"]),
                    AModifiedDateTime = ConvertToNullableDateTime(row["a_modifieddatetime"]),
                    ADescription = row["a_description"]?.ToString(),
                    AAddress1 = row["a_address1"]?.ToString(),
                    AAddress2 = row["a_address2"]?.ToString(),
                    ACity = row["a_city"]?.ToString(),
                    AState = row["a_state"]?.ToString(),
                    AZip = row["a_zip"]?.ToString(),
                    ALatitude = row["a_latitude"]?.ToString(),
                    ALongitude = row["a_longitude"]?.ToString(),
                    APicture = row["a_picture"]?.ToString(),
                    AActive = ConvertToBool(row["a_active"]),
                    ATempId = row["a_tempid"]?.ToString()
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCompanyAddresses",
                Detail = $"Retrieved {result.Count} addresses for company c_id {cId}",
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
                Description = "GetCompanyAddresses",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving addresses for company c_id {CId}", cId);
            throw;
        }
    }

    public async Task<List<AddressTitleDto>> GetAddressTitlesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    at_id,
                    o_id,
                    at_insertdatetime,
                    at_modifieddatetime,
                    at_title,
                    at_active
                FROM addresstitle
                WHERE at_active = 1
                ORDER BY at_title";

            var dt = await ExecuteQueryAsync(sql, new Dictionary<string, object>());
            
            var result = new List<AddressTitleDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new AddressTitleDto
                {
                    AtId = ConvertToInt(row["at_id"]),
                    OId = ConvertToInt(row["o_id"]),
                    AtInsertDateTime = ConvertToDateTime(row["at_insertdatetime"]),
                    AtModifiedDateTime = ConvertToNullableDateTime(row["at_modifieddatetime"]),
                    AtTitle = row["at_title"]?.ToString() ?? string.Empty,
                    AtActive = ConvertToBool(row["at_active"])
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAddressTitles",
                Detail = $"Retrieved {result.Count} active address titles",
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
                Description = "GetAddressTitles",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving address titles");
            throw;
        }
    }

    public async Task<(AddressDto? Address, string? CompanyName)> GetAddressWithCompanyByIdAsync(int aId)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string sql = @"
                SELECT 
                    a.a_id, a.o_id, a.at_id,
                    at.at_title,
                    a.a_insertdatetime, a.a_modifieddatetime, a.a_description,
                    a.a_address1, a.a_address2, a.a_city, a.a_state, a.a_zip,
                    a.a_latitude, a.a_longitude, a.a_picture, a.a_active, a.a_tempid,
                    co.c_name
                FROM address a
                INNER JOIN addresstitle at ON a.at_id = at.at_id
                INNER JOIN xrefcompanyaddress xca ON a.a_id = xca.a_id
                INNER JOIN company co ON xca.c_id = co.c_id
                WHERE a.a_id = @aId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@aId", SqlDbType.Int).Value = aId;
                await connection.OpenAsync();

                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        var address = new AddressDto
                        {
                            AId = reader.GetInt32(0),
                            OId = reader.GetInt32(1),
                            AtId = reader.GetInt32(2),
                            AtTitle = reader.IsDBNull(3) ? null : reader.GetString(3),
                            AInsertDateTime = reader.GetDateTime(4),
                            AModifiedDateTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                            ADescription = reader.IsDBNull(6) ? null : reader.GetString(6),
                            AAddress1 = reader.IsDBNull(7) ? null : reader.GetString(7),
                            AAddress2 = reader.IsDBNull(8) ? null : reader.GetString(8),
                            ACity = reader.IsDBNull(9) ? null : reader.GetString(9),
                            AState = reader.IsDBNull(10) ? null : reader.GetString(10),
                            AZip = reader.IsDBNull(11) ? null : reader.GetString(11),
                            ALatitude = reader.IsDBNull(12) ? null : reader.GetString(12),
                            ALongitude = reader.IsDBNull(13) ? null : reader.GetString(13),
                            APicture = reader.IsDBNull(14) ? null : reader.GetString(14),
                            AActive = reader.GetBoolean(15),
                            ATempId = reader.IsDBNull(16) ? null : reader.GetString(16)
                        };
                        var companyName = reader.IsDBNull(17) ? null : reader.GetString(17);
                        return (address, companyName);
                    }
                }
            }

            return (null, null);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving address with company a_id {AId}", aId);
            throw;
        }
    }

    public async Task<AddressDto> CreateAddressAsync(int cId, CreateAddressRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }
        
        try
        {
            int aId;
            
            // Insert address
            const string insertAddressSql = @"
                INSERT INTO address (
                    o_id, at_id, a_description, a_address1, a_address2,
                    a_city, a_state, a_zip, a_latitude, a_longitude,
                    a_insertdatetime, a_active
                ) VALUES (
                    @oId, @atId, @aDescription, @aAddress1, @aAddress2,
                    @aCity, @aState, @aZip, @aLatitude, @aLongitude,
                    GETDATE(), 1
                );
                SELECT CAST(SCOPE_IDENTITY() AS INT);";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(insertAddressSql, connection))
            {
                command.Parameters.Add("@oId", SqlDbType.Int).Value = 1;
                command.Parameters.Add("@atId", SqlDbType.Int).Value = request.AtId;
                command.Parameters.Add("@aDescription", SqlDbType.VarChar, 150).Value = (object?)request.ADescription ?? DBNull.Value;
                command.Parameters.Add("@aAddress1", SqlDbType.VarChar, 150).Value = (object?)request.AAddress1 ?? DBNull.Value;
                command.Parameters.Add("@aAddress2", SqlDbType.VarChar, 150).Value = (object?)request.AAddress2 ?? DBNull.Value;
                command.Parameters.Add("@aCity", SqlDbType.VarChar, 100).Value = (object?)request.ACity ?? DBNull.Value;
                command.Parameters.Add("@aState", SqlDbType.VarChar, 50).Value = (object?)request.AState ?? DBNull.Value;
                command.Parameters.Add("@aZip", SqlDbType.VarChar, 15).Value = (object?)request.AZip ?? DBNull.Value;
                command.Parameters.Add("@aLatitude", SqlDbType.VarChar, 25).Value = (object?)request.ALatitude ?? DBNull.Value;
                command.Parameters.Add("@aLongitude", SqlDbType.VarChar, 25).Value = (object?)request.ALongitude ?? DBNull.Value;

                await connection.OpenAsync();
                aId = (int)await command.ExecuteScalarAsync();
            }

            if (aId == 0)
            {
                throw new Exception("Failed to create address");
            }

            // Create xref entry
            const string insertXrefSql = @"
                INSERT INTO xrefcompanyaddress (c_id, a_id, xca_insertdatetime)
                VALUES (@cId, @aId, GETDATE())";

            var xrefParams = new Dictionary<string, object>
            {
                ["@cId"] = cId,
                ["@aId"] = aId
            };

            await ExecuteQueryAsync(insertXrefSql, xrefParams);

            // Retrieve the newly created address
            var addresses = await GetCompanyAddressesAsync(cId);
            var newAddress = addresses.FirstOrDefault(a => a.AId == aId);

            if (newAddress == null)
            {
                throw new Exception("Failed to retrieve newly created address");
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateAddress",
                Detail = $"Created address {aId} for company c_id {cId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return newAddress;
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
            
            _logger.LogError(ex, "Error creating address for company c_id {CId}", cId);
            throw;
        }
    }

    public async Task<AddressDto?> UpdateAddressAsync(int aId, UpdateAddressRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }
        
        try
        {
            // Get c_id from xref table
            const string getCIdSql = "SELECT c_id FROM xrefcompanyaddress WHERE a_id = @aId";
            int cId;

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(getCIdSql, connection))
            {
                command.Parameters.Add("@aId", SqlDbType.Int).Value = aId;
                await connection.OpenAsync();
                var result = await command.ExecuteScalarAsync();
                if (result == null)
                {
                    throw new InvalidOperationException($"Address {aId} not found in xref table");
                }
                cId = (int)result;
            }

            const string sql = @"
                UPDATE address
                SET at_id = @atId,
                    a_description = @aDescription,
                    a_address1 = @aAddress1,
                    a_address2 = @aAddress2,
                    a_city = @aCity,
                    a_state = @aState,
                    a_zip = @aZip,
                    a_latitude = @aLatitude,
                    a_longitude = @aLongitude,
                    a_modifieddatetime = GETDATE()
                WHERE a_id = @aId";

            var parameters = new Dictionary<string, object>
            {
                ["@aId"] = aId,
                ["@atId"] = request.AtId,
                ["@aDescription"] = (object?)request.ADescription ?? DBNull.Value,
                ["@aAddress1"] = (object?)request.AAddress1 ?? DBNull.Value,
                ["@aAddress2"] = (object?)request.AAddress2 ?? DBNull.Value,
                ["@aCity"] = (object?)request.ACity ?? DBNull.Value,
                ["@aState"] = (object?)request.AState ?? DBNull.Value,
                ["@aZip"] = (object?)request.AZip ?? DBNull.Value,
                ["@aLatitude"] = (object?)request.ALatitude ?? DBNull.Value,
                ["@aLongitude"] = (object?)request.ALongitude ?? DBNull.Value
            };

            await ExecuteQueryAsync(sql, parameters);

            // Retrieve the updated address
            var addresses = await GetCompanyAddressesAsync(cId);
            var updatedAddress = addresses.FirstOrDefault(a => a.AId == aId);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateAddress",
                Detail = $"Updated address {aId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return updatedAddress;
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
            
            _logger.LogError(ex, "Error updating address {AId}", aId);
            throw;
        }
    }

    #endregion

    #region Location Management

    public async Task<List<LocationDto>> GetCompanyLocationsAsync(int cId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string sql = @"
                SELECT 
                    l.l_id,
                    l.c_id,
                    l.a_id,
                    l.l_location,
                    l.l_phone,
                    l.l_hours,
                    l.l_note,
                    l.l_email,
                    l.l_active,
                    l.l_insertdatetime,
                    l.l_modifieddatetime,
                    a.a_address1,
                    a.a_address2,
                    a.a_city,
                    a.a_state,
                    a.a_zip,
                    a.a_latitude,
                    a.a_longitude
                FROM location l
                LEFT JOIN address a ON l.a_id = a.a_id
                WHERE l.c_id = @cId
                ORDER BY l.l_location";

            var locations = new List<LocationDto>();

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.Parameters.Add("@cId", SqlDbType.Int).Value = cId;
                await connection.OpenAsync();

                using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        locations.Add(new LocationDto
                        {
                            LId = (int)reader["l_id"],
                            CId = (int)reader["c_id"],
                            AId = reader["a_id"] != DBNull.Value ? (int)reader["a_id"] : 0,
                            LLocation = reader["l_location"]?.ToString() ?? string.Empty,
                            LPhone = reader["l_phone"]?.ToString(),
                            LHours = reader["l_hours"]?.ToString(),
                            LNote = reader["l_note"]?.ToString(),
                            LEmail = reader["l_email"]?.ToString(),
                            LActive = reader["l_active"] != DBNull.Value && (bool)reader["l_active"],
                            InsertDateTime = reader["l_insertdatetime"] != DBNull.Value ? (DateTime)reader["l_insertdatetime"] : DateTime.Now,
                            ModifiedDateTime = reader["l_modifieddatetime"] != DBNull.Value ? (DateTime)reader["l_modifieddatetime"] : null,
                            AAddress1 = reader["a_address1"]?.ToString(),
                            AAddress2 = reader["a_address2"]?.ToString(),
                            ACity = reader["a_city"]?.ToString(),
                            AState = reader["a_state"]?.ToString(),
                            AZip = reader["a_zip"]?.ToString(),
                            ALatitude = reader["a_latitude"]?.ToString(),
                            ALongitude = reader["a_longitude"]?.ToString()
                        });
                    }
                }
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCompanyLocations",
                Detail = $"Retrieved {locations.Count} locations for company {cId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return locations;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCompanyLocations",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving locations for company {CId}", cId);
            throw;
        }
    }

    public async Task<LocationDto> CreateLocationAsync(int cId, CreateLocationRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            // First create/get the address
            int aId;
            const string createAddressSql = @"
                INSERT INTO address (a_address1, a_address2, a_city, a_state, a_zip, a_latitude, a_longitude, a_insertdatetime)
                VALUES (@address1, @address2, @city, @state, @zip, @latitude, @longitude, GETDATE());
                SELECT CAST(SCOPE_IDENTITY() as int);";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(createAddressSql, connection))
            {
                command.Parameters.Add("@address1", SqlDbType.VarChar).Value = request.AAddress1;
                command.Parameters.Add("@address2", SqlDbType.VarChar).Value = (object?)request.AAddress2 ?? DBNull.Value;
                command.Parameters.Add("@city", SqlDbType.VarChar).Value = request.ACity;
                command.Parameters.Add("@state", SqlDbType.VarChar).Value = request.AState;
                command.Parameters.Add("@zip", SqlDbType.VarChar).Value = request.AZip;
                command.Parameters.Add("@latitude", SqlDbType.VarChar).Value = (object?)request.ALatitude ?? DBNull.Value;
                command.Parameters.Add("@longitude", SqlDbType.VarChar).Value = (object?)request.ALongitude ?? DBNull.Value;

                await connection.OpenAsync();
                var result = await command.ExecuteScalarAsync();
                aId = result != null ? (int)result : 0;
            }

            // Now create the location
            const string createLocationSql = @"
                INSERT INTO location (c_id, a_id, l_location, l_phone, l_hours, l_note, l_email, l_active, l_insertdatetime)
                VALUES (@cId, @aId, @lLocation, @lPhone, @lHours, @lNote, @lEmail, 1, GETDATE());
                SELECT CAST(SCOPE_IDENTITY() as int);";

            int lId;
            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(createLocationSql, connection))
            {
                command.Parameters.Add("@cId", SqlDbType.Int).Value = cId;
                command.Parameters.Add("@aId", SqlDbType.Int).Value = aId;
                command.Parameters.Add("@lLocation", SqlDbType.VarChar).Value = request.LLocation;
                command.Parameters.Add("@lPhone", SqlDbType.VarChar).Value = (object?)request.LPhone ?? DBNull.Value;
                command.Parameters.Add("@lHours", SqlDbType.VarChar).Value = (object?)request.LHours ?? DBNull.Value;
                command.Parameters.Add("@lNote", SqlDbType.VarChar).Value = (object?)request.LNote ?? DBNull.Value;
                command.Parameters.Add("@lEmail", SqlDbType.VarChar).Value = (object?)request.LEmail ?? DBNull.Value;

                await connection.OpenAsync();
                var result = await command.ExecuteScalarAsync();
                lId = result != null ? (int)result : 0;
            }

            // Retrieve the created location
            var locations = await GetCompanyLocationsAsync(cId);
            var createdLocation = locations.FirstOrDefault(l => l.LId == lId);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateLocation",
                Detail = $"Created location {lId} for company {cId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return createdLocation ?? new LocationDto();
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateLocation",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating location for company {CId}", cId);
            throw;
        }
    }

    public async Task<LocationDto?> UpdateLocationAsync(int lId, UpdateLocationRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            // Get location details
            const string getLocationSql = "SELECT c_id, a_id FROM location WHERE l_id = @lId";
            int cId, aId;

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(getLocationSql, connection))
            {
                command.Parameters.Add("@lId", SqlDbType.Int).Value = lId;
                await connection.OpenAsync();
                using (var reader = await command.ExecuteReaderAsync())
                {
                    if (!await reader.ReadAsync())
                    {
                        return null;
                    }
                    cId = (int)reader["c_id"];
                    aId = (int)reader["a_id"];
                }
            }

            // Update address
            const string updateAddressSql = @"
                UPDATE address
                SET a_address1 = @address1,
                    a_address2 = @address2,
                    a_city = @city,
                    a_state = @state,
                    a_zip = @zip,
                    a_latitude = @latitude,
                    a_longitude = @longitude,
                    a_modifieddatetime = GETDATE()
                WHERE a_id = @aId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(updateAddressSql, connection))
            {
                command.Parameters.Add("@aId", SqlDbType.Int).Value = aId;
                command.Parameters.Add("@address1", SqlDbType.VarChar).Value = request.AAddress1;
                command.Parameters.Add("@address2", SqlDbType.VarChar).Value = (object?)request.AAddress2 ?? DBNull.Value;
                command.Parameters.Add("@city", SqlDbType.VarChar).Value = request.ACity;
                command.Parameters.Add("@state", SqlDbType.VarChar).Value = request.AState;
                command.Parameters.Add("@zip", SqlDbType.VarChar).Value = request.AZip;
                command.Parameters.Add("@latitude", SqlDbType.VarChar).Value = (object?)request.ALatitude ?? DBNull.Value;
                command.Parameters.Add("@longitude", SqlDbType.VarChar).Value = (object?)request.ALongitude ?? DBNull.Value;

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }

            // Update location
            const string updateLocationSql = @"
                UPDATE location
                SET l_location = @lLocation,
                    l_phone = @lPhone,
                    l_hours = @lHours,
                    l_note = @lNote,
                    l_email = @lEmail,
                    l_modifieddatetime = GETDATE()
                WHERE l_id = @lId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(updateLocationSql, connection))
            {
                command.Parameters.Add("@lId", SqlDbType.Int).Value = lId;
                command.Parameters.Add("@lLocation", SqlDbType.VarChar).Value = request.LLocation;
                command.Parameters.Add("@lPhone", SqlDbType.VarChar).Value = (object?)request.LPhone ?? DBNull.Value;
                command.Parameters.Add("@lHours", SqlDbType.VarChar).Value = (object?)request.LHours ?? DBNull.Value;
                command.Parameters.Add("@lNote", SqlDbType.VarChar).Value = (object?)request.LNote ?? DBNull.Value;
                command.Parameters.Add("@lEmail", SqlDbType.VarChar).Value = (object?)request.LEmail ?? DBNull.Value;

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }

            // Retrieve the updated location
            var locations = await GetCompanyLocationsAsync(cId);
            var updatedLocation = locations.FirstOrDefault(l => l.LId == lId);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateLocation",
                Detail = $"Updated location {lId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return updatedLocation;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateLocation",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating location {LId}", lId);
            throw;
        }
    }

    #endregion

    #region Call Center Contacts

    public async Task<List<ContactDto>> GetCallCenterContactsAsync(int ccId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    c.con_id,
                    c.o_id,
                    c.ct_id,
                    ct.ct_title,
                    c.con_firstname,
                    c.con_lastname,
                    c.con_email,
                    c.con_phone,
                    c.con_mobile,
                    c.con_fax
                FROM contact c
                INNER JOIN xrefcallcentercontact xccc ON c.con_id = xccc.con_id
                INNER JOIN contacttitle ct ON c.ct_id = ct.ct_id
                WHERE xccc.cc_id = @ccId
                ORDER BY ct.ct_title, c.con_lastname, c.con_firstname";

            var parameters = new Dictionary<string, object>
            {
                ["@ccId"] = ccId
            };

            var dt = await ExecuteQueryAsync(sql, parameters);
            
            var result = new List<ContactDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new ContactDto
                {
                    ConId = ConvertToInt(row["con_id"]),
                    OId = ConvertToInt(row["o_id"]),
                    CtId = ConvertToInt(row["ct_id"]),
                    CtTitle = row["ct_title"]?.ToString() ?? string.Empty,
                    ConFirstname = row["con_firstname"]?.ToString() ?? string.Empty,
                    ConLastname = row["con_lastname"]?.ToString() ?? string.Empty,
                    ConEmail = row["con_email"]?.ToString() ?? string.Empty,
                    ConPhone = row["con_phone"]?.ToString() ?? string.Empty,
                    ConMobile = row["con_mobile"]?.ToString() ?? string.Empty,
                    ConFax = row["con_fax"]?.ToString() ?? string.Empty
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCallCenterContacts",
                Detail = $"Retrieved {result.Count} contacts for call center cc_id {ccId}",
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
                Description = "GetCallCenterContacts",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving contacts for call center cc_id {CcId}", ccId);
            throw;
        }
    }

    public async Task<ContactDto> CreateCallCenterContactAsync(int ccId, CreateContactRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            int newConId;

            // Insert contact
            const string insertContactSql = @"
                INSERT INTO contact (o_id, ct_id, con_firstname, con_lastname, con_email, con_phone, con_mobile, con_fax, con_insertdatetime)
                VALUES (1, @ctId, @firstname, @lastname, @email, @phone, @mobile, @fax, GETDATE());
                SELECT CAST(SCOPE_IDENTITY() AS INT);";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(insertContactSql, connection))
            {
                command.Parameters.Add("@ctId", SqlDbType.Int).Value = request.CtId;
                command.Parameters.Add("@firstname", SqlDbType.VarChar).Value = request.ConFirstname;
                command.Parameters.Add("@lastname", SqlDbType.VarChar).Value = request.ConLastname;
                command.Parameters.Add("@email", SqlDbType.VarChar).Value = (object?)request.ConEmail ?? DBNull.Value;
                command.Parameters.Add("@phone", SqlDbType.VarChar).Value = (object?)request.ConPhone ?? DBNull.Value;
                command.Parameters.Add("@mobile", SqlDbType.VarChar).Value = (object?)request.ConMobile ?? DBNull.Value;
                command.Parameters.Add("@fax", SqlDbType.VarChar).Value = (object?)request.ConFax ?? DBNull.Value;

                await connection.OpenAsync();
                newConId = (int)await command.ExecuteScalarAsync();
            }

            // Create cross-reference
            const string insertXrefSql = @"
                INSERT INTO xrefcallcentercontact (cc_id, con_id)
                VALUES (@ccId, @conId)";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(insertXrefSql, connection))
            {
                command.Parameters.Add("@ccId", SqlDbType.Int).Value = ccId;
                command.Parameters.Add("@conId", SqlDbType.Int).Value = newConId;

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }

            // Retrieve the created contact
            var contacts = await GetCallCenterContactsAsync(ccId);
            var createdContact = contacts.FirstOrDefault(c => c.ConId == newConId);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateCallCenterContact",
                Detail = $"Created contact {newConId} for call center {ccId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return createdContact ?? new ContactDto();
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateCallCenterContact",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating contact for call center {CcId}", ccId);
            throw;
        }
    }

    public async Task<ContactDto?> UpdateCallCenterContactAsync(int conId, UpdateContactRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string updateContactSql = @"
                UPDATE contact
                SET ct_id = @ctId,
                    con_firstname = @firstname,
                    con_lastname = @lastname,
                    con_email = @email,
                    con_phone = @phone,
                    con_mobile = @mobile,
                    con_fax = @fax,
                    con_modifieddatetime = GETDATE()
                WHERE con_id = @conId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(updateContactSql, connection))
            {
                command.Parameters.Add("@conId", SqlDbType.Int).Value = conId;
                command.Parameters.Add("@ctId", SqlDbType.Int).Value = request.CtId;
                command.Parameters.Add("@firstname", SqlDbType.VarChar).Value = request.ConFirstname;
                command.Parameters.Add("@lastname", SqlDbType.VarChar).Value = request.ConLastname;
                command.Parameters.Add("@email", SqlDbType.VarChar).Value = (object?)request.ConEmail ?? DBNull.Value;
                command.Parameters.Add("@phone", SqlDbType.VarChar).Value = (object?)request.ConPhone ?? DBNull.Value;
                command.Parameters.Add("@mobile", SqlDbType.VarChar).Value = (object?)request.ConMobile ?? DBNull.Value;
                command.Parameters.Add("@fax", SqlDbType.VarChar).Value = (object?)request.ConFax ?? DBNull.Value;

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }

            // Get the cc_id to retrieve the updated contact
            const string getCcIdSql = "SELECT cc_id FROM xrefcallcentercontact WHERE con_id = @conId";
            int ccId;
            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(getCcIdSql, connection))
            {
                command.Parameters.Add("@conId", SqlDbType.Int).Value = conId;
                await connection.OpenAsync();
                ccId = (int)await command.ExecuteScalarAsync();
            }

            // Retrieve the updated contact
            var contacts = await GetCallCenterContactsAsync(ccId);
            var updatedContact = contacts.FirstOrDefault(c => c.ConId == conId);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCallCenterContact",
                Detail = $"Updated contact {conId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return updatedContact;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCallCenterContact",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating contact {ConId}", conId);
            throw;
        }
    }

    public async Task<bool> DeleteCallCenterContactXrefAsync(int ccId, int conId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string deleteSql = @"
                DELETE FROM xrefcallcentercontact
                WHERE cc_id = @ccId AND con_id = @conId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(deleteSql, connection))
            {
                command.Parameters.Add("@ccId", SqlDbType.Int).Value = ccId;
                command.Parameters.Add("@conId", SqlDbType.Int).Value = conId;

                await connection.OpenAsync();
                var rowsAffected = await command.ExecuteNonQueryAsync();

                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "DeleteCallCenterContactXref",
                    Detail = $"Deleted xref for call center {ccId} and contact {conId}. Rows affected: {rowsAffected}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return rowsAffected > 0;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "DeleteCallCenterContactXref",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error deleting contact xref for call center {CcId} and contact {ConId}", ccId, conId);
            throw;
        }
    }

    public async Task<ContactDto?> GetContactByIdAsync(int conId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    c.con_id,
                    c.o_id,
                    c.ct_id,
                    ct.ct_title,
                    c.con_firstname,
                    c.con_lastname,
                    c.con_email,
                    c.con_phone,
                    c.con_mobile,
                    c.con_fax
                FROM contact c
                INNER JOIN contacttitle ct ON c.ct_id = ct.ct_id
                WHERE c.con_id = @conId";

            var parameters = new Dictionary<string, object>
            {
                ["@conId"] = conId
            };

            var dt = await ExecuteQueryAsync(sql, parameters);
            
            if (dt.Rows.Count == 0)
            {
                return null;
            }

            var row = dt.Rows[0];
            var contact = new ContactDto
            {
                ConId = ConvertToInt(row["con_id"]),
                OId = ConvertToInt(row["o_id"]),
                CtId = ConvertToInt(row["ct_id"]),
                CtTitle = row["ct_title"]?.ToString() ?? string.Empty,
                ConFirstname = row["con_firstname"]?.ToString() ?? string.Empty,
                ConLastname = row["con_lastname"]?.ToString() ?? string.Empty,
                ConEmail = row["con_email"]?.ToString() ?? string.Empty,
                ConPhone = row["con_phone"]?.ToString() ?? string.Empty,
                ConMobile = row["con_mobile"]?.ToString() ?? string.Empty,
                ConFax = row["con_fax"]?.ToString() ?? string.Empty
            };

            stopwatch.Stop();
            return contact;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error retrieving contact {ConId}", conId);
            throw;
        }
    }

    #endregion

    #region Call Center Addresses

    public async Task<List<AddressDto>> GetCallCenterAddressesAsync(int ccId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            const string sql = @"
                SELECT 
                    a.a_id,
                    a.o_id,
                    a.at_id,
                    at.at_title,
                    a.a_description,
                    a.a_address1,
                    a.a_address2,
                    a.a_city,
                    a.a_state,
                    a.a_zip,
                    a.a_latitude,
                    a.a_longitude,
                    a.a_active
                FROM address a
                INNER JOIN xrefcallcenteraddress xcca ON a.a_id = xcca.a_id
                INNER JOIN addresstitle at ON a.at_id = at.at_id
                WHERE xcca.cc_id = @ccId
                ORDER BY at.at_title, a.a_city";

            var parameters = new Dictionary<string, object>
            {
                ["@ccId"] = ccId
            };

            var dt = await ExecuteQueryAsync(sql, parameters);
            
            var result = new List<AddressDto>();
            foreach (DataRow row in dt.Rows)
            {
                result.Add(new AddressDto
                {
                    AId = ConvertToInt(row["a_id"]),
                    OId = ConvertToInt(row["o_id"]),
                    AtId = ConvertToInt(row["at_id"]),
                    AtTitle = row["at_title"]?.ToString() ?? string.Empty,
                    ADescription = row["a_description"]?.ToString() ?? string.Empty,
                    AAddress1 = row["a_address1"]?.ToString() ?? string.Empty,
                    AAddress2 = row["a_address2"]?.ToString() ?? string.Empty,
                    ACity = row["a_city"]?.ToString() ?? string.Empty,
                    AState = row["a_state"]?.ToString() ?? string.Empty,
                    AZip = row["a_zip"]?.ToString() ?? string.Empty,
                    ALatitude = row["a_latitude"]?.ToString() ?? string.Empty,
                    ALongitude = row["a_longitude"]?.ToString() ?? string.Empty,
                    AActive = ConvertToBool(row["a_active"])
                });
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCallCenterAddresses",
                Detail = $"Retrieved {result.Count} addresses for call center cc_id {ccId}",
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
                Description = "GetCallCenterAddresses",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error retrieving addresses for call center cc_id {CcId}", ccId);
            throw;
        }
    }

    public async Task<AddressDto> CreateCallCenterAddressAsync(int ccId, CreateAddressRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            int newAId;

            // Insert address
            const string insertAddressSql = @"
                INSERT INTO address (o_id, at_id, a_description, a_address1, a_address2, a_city, a_state, a_zip, a_latitude, a_longitude, a_active, a_insertdatetime)
                VALUES (1, @atId, @description, @address1, @address2, @city, @state, @zip, @latitude, @longitude, 1, GETDATE());
                SELECT CAST(SCOPE_IDENTITY() AS INT);";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(insertAddressSql, connection))
            {
                command.Parameters.Add("@atId", SqlDbType.Int).Value = request.AtId;
                command.Parameters.Add("@description", SqlDbType.VarChar).Value = (object?)request.ADescription ?? DBNull.Value;
                command.Parameters.Add("@address1", SqlDbType.VarChar).Value = request.AAddress1;
                command.Parameters.Add("@address2", SqlDbType.VarChar).Value = (object?)request.AAddress2 ?? DBNull.Value;
                command.Parameters.Add("@city", SqlDbType.VarChar).Value = request.ACity;
                command.Parameters.Add("@state", SqlDbType.VarChar).Value = request.AState;
                command.Parameters.Add("@zip", SqlDbType.VarChar).Value = request.AZip;
                command.Parameters.Add("@latitude", SqlDbType.VarChar).Value = (object?)request.ALatitude ?? DBNull.Value;
                command.Parameters.Add("@longitude", SqlDbType.VarChar).Value = (object?)request.ALongitude ?? DBNull.Value;

                await connection.OpenAsync();
                newAId = (int)await command.ExecuteScalarAsync();
            }

            // Create cross-reference
            const string insertXrefSql = @"
                INSERT INTO xrefcallcenteraddress (cc_id, a_id)
                VALUES (@ccId, @aId)";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(insertXrefSql, connection))
            {
                command.Parameters.Add("@ccId", SqlDbType.Int).Value = ccId;
                command.Parameters.Add("@aId", SqlDbType.Int).Value = newAId;

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }

            // Retrieve the created address
            var addresses = await GetCallCenterAddressesAsync(ccId);
            var createdAddress = addresses.FirstOrDefault(a => a.AId == newAId);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateCallCenterAddress",
                Detail = $"Created address {newAId} for call center {ccId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return createdAddress ?? new AddressDto();
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CreateCallCenterAddress",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error creating address for call center {CcId}", ccId);
            throw;
        }
    }

    public async Task<AddressDto?> UpdateCallCenterAddressAsync(int aId, UpdateAddressRequest request)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string updateAddressSql = @"
                UPDATE address
                SET at_id = @atId,
                    a_description = @description,
                    a_address1 = @address1,
                    a_address2 = @address2,
                    a_city = @city,
                    a_state = @state,
                    a_zip = @zip,
                    a_latitude = @latitude,
                    a_longitude = @longitude,
                    a_active = @active,
                    a_modifieddatetime = GETDATE()
                WHERE a_id = @aId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(updateAddressSql, connection))
            {
                command.Parameters.Add("@aId", SqlDbType.Int).Value = aId;
                command.Parameters.Add("@atId", SqlDbType.Int).Value = request.AtId;
                command.Parameters.Add("@description", SqlDbType.VarChar).Value = (object?)request.ADescription ?? DBNull.Value;
                command.Parameters.Add("@address1", SqlDbType.VarChar).Value = request.AAddress1;
                command.Parameters.Add("@address2", SqlDbType.VarChar).Value = (object?)request.AAddress2 ?? DBNull.Value;
                command.Parameters.Add("@city", SqlDbType.VarChar).Value = request.ACity;
                command.Parameters.Add("@state", SqlDbType.VarChar).Value = request.AState;
                command.Parameters.Add("@zip", SqlDbType.VarChar).Value = request.AZip;
                command.Parameters.Add("@latitude", SqlDbType.VarChar).Value = (object?)request.ALatitude ?? DBNull.Value;
                command.Parameters.Add("@longitude", SqlDbType.VarChar).Value = (object?)request.ALongitude ?? DBNull.Value;

                await connection.OpenAsync();
                await command.ExecuteNonQueryAsync();
            }

            // Get the cc_id to retrieve the updated address
            const string getCcIdSql = "SELECT cc_id FROM xrefcallcenteraddress WHERE a_id = @aId";
            int ccId;
            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(getCcIdSql, connection))
            {
                command.Parameters.Add("@aId", SqlDbType.Int).Value = aId;
                await connection.OpenAsync();
                ccId = (int)await command.ExecuteScalarAsync();
            }

            // Retrieve the updated address
            var addresses = await GetCallCenterAddressesAsync(ccId);
            var updatedAddress = addresses.FirstOrDefault(a => a.AId == aId);

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCallCenterAddress",
                Detail = $"Updated address {aId}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return updatedAddress;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateCallCenterAddress",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error updating address {AId}", aId);
            throw;
        }
    }

    public async Task<bool> DeleteCallCenterAddressXrefAsync(int ccId, int aId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
        {
            throw new InvalidOperationException("No connection string found");
        }

        try
        {
            const string deleteSql = @"
                DELETE FROM xrefcallcenteraddress
                WHERE cc_id = @ccId AND a_id = @aId";

            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(deleteSql, connection))
            {
                command.Parameters.Add("@ccId", SqlDbType.Int).Value = ccId;
                command.Parameters.Add("@aId", SqlDbType.Int).Value = aId;

                await connection.OpenAsync();
                var rowsAffected = await command.ExecuteNonQueryAsync();

                stopwatch.Stop();
                await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
                {
                    Name = "DataService",
                    Description = "DeleteCallCenterAddressXref",
                    Detail = $"Deleted xref for call center {ccId} and address {aId}. Rows affected: {rowsAffected}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                    MachineName = Environment.MachineName
                });

                return rowsAffected > 0;
            }
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogErrorAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "DeleteCallCenterAddressXref",
                Detail = ex.ToString(),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            
            _logger.LogError(ex, "Error deleting address xref for call center {CcId} and address {AId}", ccId, aId);
            throw;
        }
    }

    /// <summary>
    /// Gets all call center attachments (one per call center, most recent)
    /// Returns a DataTable with cc_id and attachment details for bulk loading
    /// </summary>
    public async Task<DataTable> GetAllCallCenterAttachmentsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
                throw new InvalidOperationException("Connection string not configured");

            const string sql = @"
                SELECT 
                    a.att_id,
                    a.cc_id,
                    a.att_filename,
                    a.att_extension,
                    a.att_description,
                    a.att_insertdatetime,
                    a.att_modifieddatetime,
                    a.att_active,
                    a.att_receipt,
                    a.att_public,
                    a.att_signoff,
                    a.att_submittedby,
                    a.att_receiptamount,
                    a.sr_id
                FROM (
                    SELECT 
                        att_id,
                        cc_id,
                        att_filename,
                        att_extension,
                        att_description,
                        att_insertdatetime,
                        att_modifieddatetime,
                        att_active,
                        att_receipt,
                        att_public,
                        att_signoff,
                        att_submittedby,
                        att_receiptamount,
                        sr_id,
                        ROW_NUMBER() OVER (PARTITION BY cc_id ORDER BY att_insertdatetime DESC) as rn
                    FROM attachment
                    WHERE cc_id IS NOT NULL AND cc_id > 0
                ) a
                WHERE a.rn = 1
                ORDER BY a.cc_id";

            var dt = new DataTable();
            using (var connection = new SqlConnection(connectionString))
            using (var command = new SqlCommand(sql, connection))
            {
                command.CommandTimeout = 30;
                var adapter = new SqlDataAdapter(command);
                adapter.Fill(dt);
            }

            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllCallCenterAttachments",
                Detail = $"Retrieved {dt.Rows.Count} call center attachments",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });

            return dt;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllCallCenterAttachments",
                Detail = $"Error retrieving call center attachments: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    #endregion

    #region Time Off Requests

    public async Task<DataTable> GetTimeOffRequestTypesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"SELECT tort_id, tort_type FROM TimeOffRequestType";
            var result = await ExecuteQueryAsync(sql);
            stopwatch.Stop();
            _logger.LogInformation("GetTimeOffRequestTypesAsync completed in {ElapsedMs}ms, {Count} rows",
                stopwatch.ElapsedMilliseconds, result.Rows.Count);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetTimeOffRequestTypesAsync");
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTimeOffRequestTypes",
                Detail = $"Error: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<DataTable> GetTimeOffRequestTypeDetailsAsync(int tortId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                SELECT tortd_id, tort_id, tortd_typedetail, tortd_workflowrequired, 
                       tortd_admincancreate, 
                       ISNULL(tortd_techcancreate, 1) as tortd_techcancreate,
                       ISNULL(tortd_maxdaysoff, 30) as tortd_maxdaysoff
                FROM TimeOffRequestTypeDetail
                WHERE (@tort_id = 0 OR tort_id = @tort_id)
                AND tortd_active = 1";

            var parameters = new Dictionary<string, object>
            {
                { "@tort_id", tortId }
            };
            var result = await ExecuteQueryAsync(sql, parameters);
            stopwatch.Stop();
            _logger.LogInformation("GetTimeOffRequestTypeDetailsAsync completed in {ElapsedMs}ms for tortId={TortId}, {Count} rows",
                stopwatch.ElapsedMilliseconds, tortId, result.Rows.Count);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetTimeOffRequestTypeDetailsAsync for tortId={TortId}", tortId);
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTimeOffRequestTypeDetails",
                Detail = $"Error for tortId={tortId}: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<DataTable> GetTimeOffBalanceAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                SELECT u_id, u_username, u_firstname, u_lastname, u_email, 
                       ISNULL(u_daysavailablepto, 0) as u_daysavailablepto, 
                       ISNULL(u_daysavailablevacation, 0) as u_daysavailablevacation 
                FROM [user] u 
                WHERE u_id = @u_id";

            var parameters = new Dictionary<string, object>
            {
                { "@u_id", userId }
            };
            var result = await ExecuteQueryAsync(sql, parameters);
            stopwatch.Stop();
            _logger.LogInformation("GetTimeOffBalanceAsync completed in {ElapsedMs}ms for userId={UserId}",
                stopwatch.ElapsedMilliseconds, userId);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetTimeOffBalanceAsync for userId={UserId}", userId);
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTimeOffBalance",
                Detail = $"Error for userId={userId}: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<DataTable> GetTimeOffRequestsAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                SELECT tor.tor_id, u.u_firstname, u.u_lastname, 
                       ISNULL(u.u_daysavailablevacation, 0) as u_daysavailablevacation, 
                       ISNULL(u.u_daysavailablepto, 0) as u_daysavailablepto,
                       tor.tor_startdate, tor.tor_enddate, tortd.tortd_typedetail, tortd.tortd_id,
                       tors.tors_status, tor.tor_note, tor.tor_notereason, tors.tors_id, 
                       tor.u_id, tor.tor_totalhours, ISNULL(u.z_id, 0) as z_id
                FROM timeoffrequest tor
                INNER JOIN timeoffrequesttypedetail tortd ON tor.tortd_id = tortd.tortd_id
                INNER JOIN timeoffrequeststatus tors ON tor.tors_id = tors.tors_id
                INNER JOIN [user] u ON tor.u_id = u.u_id
                LEFT JOIN zone z ON u.z_id = z.z_id
                WHERE u.u_id = @u_id
                ORDER BY tors.tors_orderby, tor.tor_startdate DESC";

            var parameters = new Dictionary<string, object>
            {
                { "@u_id", userId }
            };
            var result = await ExecuteQueryAsync(sql, parameters);
            stopwatch.Stop();
            _logger.LogInformation("GetTimeOffRequestsAsync completed in {ElapsedMs}ms for userId={UserId}, {Count} rows",
                stopwatch.ElapsedMilliseconds, userId, result.Rows.Count);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetTimeOffRequestsAsync for userId={UserId}", userId);
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetTimeOffRequests",
                Detail = $"Error for userId={userId}: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<int?> InsertTimeOffRequestAsync(EvoAPI.Shared.DTOs.CreateTimeOffRequestDto request, int statusId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var note = request.Note ?? string.Empty;

            const string sql = @"
                INSERT INTO TimeOffRequest (tortd_id, u_id, u_id_admincreated, tors_id, tor_startdate, tor_enddate, tor_note) 
                VALUES (@tortd_id, @u_id, @u_id_admincreated, @tors_id, @tor_startdate, @tor_enddate, @tor_note);
                SELECT CAST(SCOPE_IDENTITY() AS INT);";

            var parameters = new Dictionary<string, object>
            {
                { "@tortd_id", request.TortdId },
                { "@u_id", request.UserId },
                { "@u_id_admincreated", request.UserIdAdmincreated },
                { "@tors_id", statusId },
                { "@tor_startdate", request.StartDate },
                { "@tor_enddate", request.EndDate },
                { "@tor_note", note }
            };

            var result = await ExecuteQueryAsync(sql, parameters);
            stopwatch.Stop();

            if (result.Rows.Count > 0 && result.Rows[0][0] != DBNull.Value)
            {
                var newId = Convert.ToInt32(result.Rows[0][0]);
                _logger.LogInformation("InsertTimeOffRequestAsync completed in {ElapsedMs}ms, new tor_id={TorId}",
                    stopwatch.ElapsedMilliseconds, newId);
                return newId;
            }

            _logger.LogWarning("InsertTimeOffRequestAsync completed but no ID returned");
            return null;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in InsertTimeOffRequestAsync");
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "InsertTimeOffRequest",
                Detail = $"Error: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<bool> InsertTimeOffRequestDetailsAsync(int torId, List<EvoAPI.Shared.DTOs.CreateTimeOffRequestDetailDto> details)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            int totalHours = 0;

            foreach (var detail in details)
            {
                const string sqlDetail = @"
                    INSERT INTO TimeOffRequestDetail (tor_id, tord_date, tord_starthour, tord_endhour) 
                    VALUES (@tor_id, @tord_date, @tord_starthour, @tord_endhour)";

                var detailParams = new Dictionary<string, object>
                {
                    { "@tor_id", torId },
                    { "@tord_date", detail.Date },
                    { "@tord_starthour", detail.StartHour },
                    { "@tord_endhour", detail.EndHour }
                };

                await ExecuteNonQueryAsync(sqlDetail, detailParams);
                totalHours += detail.EndHour - detail.StartHour;
            }

            // Update total hours on the parent request
            if (totalHours > 0)
            {
                const string sqlUpdate = @"UPDATE TimeOffRequest SET tor_totalhours = @totalHours WHERE tor_id = @tor_id";
                var updateParams = new Dictionary<string, object>
                {
                    { "@totalHours", totalHours },
                    { "@tor_id", torId }
                };
                await ExecuteNonQueryAsync(sqlUpdate, updateParams);
            }

            stopwatch.Stop();
            _logger.LogInformation("InsertTimeOffRequestDetailsAsync completed in {ElapsedMs}ms for torId={TorId}, {Count} details, {TotalHours} total hours",
                stopwatch.ElapsedMilliseconds, torId, details.Count, totalHours);
            return true;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in InsertTimeOffRequestDetailsAsync for torId={TorId}", torId);
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "InsertTimeOffRequestDetails",
                Detail = $"Error for torId={torId}: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<bool> CancelTimeOffRequestAsync(int torId, int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            // Verify the request belongs to this user and is cancellable (not already cancelled/rejected)
            const string verifySql = @"
                SELECT tor_id, u_id, tors_id 
                FROM TimeOffRequest 
                WHERE tor_id = @tor_id";

            var verifyParams = new Dictionary<string, object> { { "@tor_id", torId } };
            var verifyResult = await ExecuteQueryAsync(verifySql, verifyParams);

            if (verifyResult.Rows.Count == 0)
                throw new InvalidOperationException("Time off request not found");

            var row = verifyResult.Rows[0];
            var requestUserId = Convert.ToInt32(row["u_id"]);
            var currentStatus = Convert.ToInt32(row["tors_id"]);

            if (requestUserId != userId)
                throw new UnauthorizedAccessException("You can only cancel your own requests");

            if (currentStatus == 5) // Already cancelled
                throw new InvalidOperationException("Request is already cancelled");

            if (currentStatus == 4) // Rejected
                throw new InvalidOperationException("Cannot cancel a rejected request");

            // Update status to 5 (Cancelled)
            const string sql = @"
                UPDATE TimeOffRequest 
                SET tors_id = 5 
                WHERE tor_id = @tor_id";

            var parameters = new Dictionary<string, object> { { "@tor_id", torId } };
            await ExecuteNonQueryAsync(sql, parameters);

            stopwatch.Stop();
            _logger.LogInformation("CancelTimeOffRequestAsync completed in {ElapsedMs}ms for torId={TorId}, userId={UserId}",
                stopwatch.ElapsedMilliseconds, torId, userId);
            return true;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in CancelTimeOffRequestAsync for torId={TorId}", torId);
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "CancelTimeOffRequest",
                Detail = $"Error for torId={torId}: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<DataTable> GetActiveEmployeesForTimeOffAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                SELECT u.u_id, u.u_firstname, u.u_lastname, u.u_username
                FROM [user] u
                WHERE u.u_active = 1
                ORDER BY u.u_lastname, u.u_firstname";

            var result = await ExecuteQueryAsync(sql);
            stopwatch.Stop();
            _logger.LogInformation("GetActiveEmployeesForTimeOffAsync completed in {ElapsedMs}ms, {Count} rows",
                stopwatch.ElapsedMilliseconds, result.Rows.Count);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetActiveEmployeesForTimeOffAsync");
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetActiveEmployeesForTimeOff",
                Detail = $"Error: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<DataTable> GetAllTimeOffRequestsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                SELECT tor.tor_id, u.u_firstname, u.u_lastname, u.u_daysavailablevacation, 
                       u.u_daysavailablepto, tor.tor_startdate, tor.tor_enddate, tortd.tortd_typedetail, 
                       tortd.tortd_id, tors.tors_status, tor.tor_note, tor.tor_notereason, tors.tors_id, 
                       tor.u_id, tor.tor_totalhours, ISNULL(u.z_id, 0) AS z_id, tor.tor_insertdatetime
                FROM timeoffrequest tor
                INNER JOIN timeoffrequesttypedetail tortd ON tor.tortd_id = tortd.tortd_id
                INNER JOIN timeoffrequeststatus tors ON tor.tors_id = tors.tors_id
                INNER JOIN [user] u ON tor.u_id = u.u_id
                LEFT JOIN zone z ON u.z_id = z.z_id
                WHERE tor.tor_startdate >= '2022-01-01'
                AND tor.tor_enddate <= '2045-12-31'
                ORDER BY tors.tors_orderby, tor.tor_startdate DESC";

            var result = await ExecuteQueryAsync(sql);
            stopwatch.Stop();
            _logger.LogInformation("GetAllTimeOffRequestsAsync completed in {ElapsedMs}ms, {Count} rows",
                stopwatch.ElapsedMilliseconds, result.Rows.Count);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetAllTimeOffRequestsAsync");
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetAllTimeOffRequests",
                Detail = $"Error: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<DataTable> GetTimeOffRequestDetailAsync(int torId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                SELECT tord_id, tor_id, tord_date, tord_starthour, tord_endhour
                FROM timeoffrequestdetail
                WHERE tor_id = @tor_id";

            var parameters = new Dictionary<string, object> { { "@tor_id", torId } };
            var result = await ExecuteQueryAsync(sql, parameters);
            stopwatch.Stop();
            _logger.LogInformation("GetTimeOffRequestDetailAsync completed in {ElapsedMs}ms for torId={TorId}, {Count} rows",
                stopwatch.ElapsedMilliseconds, torId, result.Rows.Count);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetTimeOffRequestDetailAsync for torId={TorId}", torId);
            throw;
        }
    }

    public async Task<bool> UpdateTimeOffRequestStatusAsync(int torId, int torsId, string noteReason)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            // Step 1: Update the status and reviewer note
            const string sqlUpdate = @"
                UPDATE TimeOffRequest 
                SET tors_id = @tors_id, tor_notereason = @tor_notereason
                WHERE tor_id = @tor_id";

            var updateParams = new Dictionary<string, object>
            {
                { "@tors_id", torsId },
                { "@tor_notereason", noteReason ?? "" },
                { "@tor_id", torId }
            };
            await ExecuteNonQueryAsync(sqlUpdate, updateParams);

            // Step 2: If cancelling (status 5), update linked service request and remove WO assignments
            if (torsId == 5)
            {
                const string sqlCancelSR = @"
                    UPDATE servicerequest SET s_id = 6, ss_id = 25
                    WHERE sr_id IN (SELECT sr_id FROM timeoffrequest WHERE tor_id = @tor_id)";
                var cancelParams = new Dictionary<string, object> { { "@tor_id", torId } };
                await ExecuteNonQueryAsync(sqlCancelSR, cancelParams);

                const string sqlDeleteWOUsers = @"
                    DELETE FROM xrefworkorderuser 
                    WHERE wo_id IN (
                        SELECT wo.wo_id FROM workorder wo
                        INNER JOIN timeoffrequest tor ON tor.sr_id = wo.sr_id
                        WHERE tor.tor_id = @tor_id
                    )";
                await ExecuteNonQueryAsync(sqlDeleteWOUsers, cancelParams);
            }

            // Step 3: If approving (status 1), update linked SR to invoiced
            if (torsId == 1)
            {
                const string sqlApproveSR = @"
                    UPDATE ServiceRequest SET s_id = 5 
                    WHERE sr_id IN (SELECT sr_id FROM timeoffrequest WHERE tor_id = @tor_id)";
                var approveParams = new Dictionary<string, object> { { "@tor_id", torId } };
                await ExecuteNonQueryAsync(sqlApproveSR, approveParams);
            }

            stopwatch.Stop();
            _logger.LogInformation("UpdateTimeOffRequestStatusAsync completed in {ElapsedMs}ms for torId={TorId}, newStatus={TorsId}",
                stopwatch.ElapsedMilliseconds, torId, torsId);
            return true;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in UpdateTimeOffRequestStatusAsync for torId={TorId}", torId);
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "UpdateTimeOffRequestStatus",
                Detail = $"Error for torId={torId}, torsId={torsId}: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<bool> DeleteTimeOffRequestAsync(int torId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                DELETE FROM TimeOffRequestDetail WHERE tor_id = @tor_id;
                DELETE FROM TimeOffRequest WHERE tor_id = @tor_id;";

            var parameters = new Dictionary<string, object> { { "@tor_id", torId } };
            await ExecuteNonQueryAsync(sql, parameters);

            stopwatch.Stop();
            _logger.LogInformation("DeleteTimeOffRequestAsync completed in {ElapsedMs}ms for torId={TorId}",
                stopwatch.ElapsedMilliseconds, torId);
            return true;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in DeleteTimeOffRequestAsync for torId={TorId}", torId);
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "DeleteTimeOffRequest",
                Detail = $"Error for torId={torId}: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<bool> IsTimeOffWorkflowCurrentlyZFMReviewAsync(int torId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                SELECT COUNT(*) AS cnt
                FROM timeoffrequest
                WHERE tors_id = 2 AND tor_id = @tor_id";

            var parameters = new Dictionary<string, object> { { "@tor_id", torId } };
            var result = await ExecuteQueryAsync(sql, parameters);
            stopwatch.Stop();
            return result.Rows.Count > 0 && Convert.ToInt32(result.Rows[0]["cnt"]) > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in IsTimeOffWorkflowCurrentlyZFMReviewAsync for torId={TorId}", torId);
            throw;
        }
    }

    public async Task<bool> IsTimeOffWorkflowAdminRequiredAsync(int torId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                SELECT COUNT(*) AS cnt
                FROM timeoffrequest tor
                INNER JOIN timeoffrequesttypedetail tortd ON tor.tortd_id = tortd.tortd_id
                WHERE DATEDIFF(d, tor.tor_insertdatetime, tor.tor_startdate) < tortd.tortd_workflowadmindaythreshold
                AND tortd.tortd_workflowrequired = 1
                AND tor.tors_id = 2
                AND tor.tor_id = @tor_id";

            var parameters = new Dictionary<string, object> { { "@tor_id", torId } };
            var result = await ExecuteQueryAsync(sql, parameters);
            stopwatch.Stop();
            return result.Rows.Count > 0 && Convert.ToInt32(result.Rows[0]["cnt"]) > 0;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in IsTimeOffWorkflowAdminRequiredAsync for torId={TorId}", torId);
            throw;
        }
    }

    public async Task<int> InsertTimeOffRequestServiceRequestsAsync(int torId, int userId, int tortdId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var rnd = new Random();
            string srRequestNumber = DateTime.UtcNow.ToString("yyyyMMdd") + "-" + rnd.Next(1000, 9999).ToString();

            // Step 1: Create Service Request
            const string sqlInsertSR = @"
                DECLARE @xccc_id int 
                DECLARE @l_id int 
                DECLARE @t_id int 
                DECLARE @sr_summary varchar(100)
                DECLARE @sr_callnote varchar(8000)
                DECLARE @tortd_id_local int 

                SELECT @tortd_id_local = tortd_id FROM timeoffrequest WHERE tor_id = @tor_id
                SELECT @xccc_id = xccc_id, @l_id = l_id, @t_id = t_id, @sr_summary = tortd_typedetail 
                    FROM timeoffrequesttypedetail WHERE tortd_id = @tortd_id_local
                SELECT @sr_callnote = tor_note FROM timeoffrequest WHERE tor_id = @tor_id

                INSERT INTO ServiceRequest (xccc_id, l_id, t_id, s_id, ss_id, p_id, lrt_id, sr_summary, sr_requestnumber, sr_callnote, sr_flatorhourly)
                VALUES (@xccc_id, @l_id, @t_id, 5, 17, 1, 1, @sr_summary, @sr_requestnumber, @sr_callnote, 'hourly')

                SELECT SCOPE_IDENTITY()";

            var srParams = new Dictionary<string, object>
            {
                { "@tor_id", torId },
                { "@sr_requestnumber", srRequestNumber }
            };
            var srResult = await ExecuteQueryAsync(sqlInsertSR, srParams);
            int srId = Convert.ToInt32(srResult.Rows[0][0]);

            // Step 2: Link TimeOffRequest to Service Request
            const string sqlUpdateTor = "UPDATE TimeOffRequest SET sr_id = @sr_id WHERE tor_id = @tor_id";
            var updateTorParams = new Dictionary<string, object>
            {
                { "@sr_id", srId },
                { "@tor_id", torId }
            };
            await ExecuteNonQueryAsync(sqlUpdateTor, updateTorParams);

            // Step 3: Get detail rows for creating work orders
            var detailDt = await GetTimeOffRequestDetailAsync(torId);

            // Step 4: Create work orders for each detail day
            int i = 0;
            foreach (System.Data.DataRow row in detailDt.Rows)
            {
                i++;
                string woNumber = srRequestNumber + "-" + i.ToString();
                var tordDate = Convert.ToDateTime(row["tord_date"]);
                int startHour = Convert.ToInt32(row["tord_starthour"]);
                int endHour = Convert.ToInt32(row["tord_endhour"]);

                // Calculate start/end datetimes (UTC offset for Central Time)
                DateTime startDateTime = tordDate.Date.AddHours(startHour + 5);
                DateTime endDateTime = tordDate.Date.AddHours(endHour + 5);

                // Full day override (8 hours)
                if ((endHour - startHour) == 8)
                {
                    int offset = (int)(DateTime.UtcNow - TimeZoneInfo.ConvertTime(DateTime.Now, 
                        TimeZoneInfo.FindSystemTimeZoneById("Central Standard Time"))).TotalHours;
                    startDateTime = tordDate.Date.AddHours(offset);
                    endDateTime = tordDate.Date.AddHours(23).AddMinutes(59).AddSeconds(59).AddHours(offset);
                }

                const string sqlInsertWO = @"
                    DECLARE @sr_summary varchar(100)
                    SELECT @sr_summary = tortd_typedetail FROM timeoffrequesttypedetail WHERE tortd_id = @tortd_id

                    INSERT INTO WORKORDER (sr_id, wo_workordernumber, wo_description, wo_nte, ss_id, wot_id, wo_startdatetime, wo_enddatetime)
                    VALUES (@sr_id, @wo_workordernumber, @sr_summary, 0, 17, 1, @wo_startdatetime, @wo_enddatetime)

                    SELECT SCOPE_IDENTITY()";

                var woParams = new Dictionary<string, object>
                {
                    { "@sr_id", srId },
                    { "@tortd_id", tortdId },
                    { "@wo_workordernumber", woNumber },
                    { "@wo_startdatetime", startDateTime },
                    { "@wo_enddatetime", endDateTime }
                };
                var woResult = await ExecuteQueryAsync(sqlInsertWO, woParams);
                int woId = Convert.ToInt32(woResult.Rows[0][0]);

                // Assign employee to work order
                const string sqlAssign = "INSERT INTO xrefworkorderuser (wo_id, u_id) VALUES (@wo_id, @u_id)";
                var assignParams = new Dictionary<string, object>
                {
                    { "@wo_id", woId },
                    { "@u_id", userId }
                };
                await ExecuteNonQueryAsync(sqlAssign, assignParams);
            }

            stopwatch.Stop();
            _logger.LogInformation("InsertTimeOffRequestServiceRequestsAsync completed in {ElapsedMs}ms for torId={TorId}, srId={SrId}, {WoCount} work orders created",
                stopwatch.ElapsedMilliseconds, torId, srId, i);
            return srId;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in InsertTimeOffRequestServiceRequestsAsync for torId={TorId}", torId);
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "InsertTimeOffRequestServiceRequests",
                Detail = $"Error for torId={torId}: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    public async Task<DataTable> GetZonesAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                SELECT z_id, z_number, u_id
                FROM zone
                ORDER BY z_number";

            var result = await ExecuteQueryAsync(sql);
            stopwatch.Stop();
            _logger.LogInformation("GetZonesAsync completed in {ElapsedMs}ms, {Count} rows",
                stopwatch.ElapsedMilliseconds, result.Rows.Count);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetZonesAsync");
            throw;
        }
    }

    public async Task<DataTable> GetZFMByUserAsync(int userId)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                SELECT zfm.* FROM [user] zfm
                WHERE zfm.u_id IN (
                    SELECT z.u_id FROM zone z
                    INNER JOIN [user] u ON z.z_id = u.z_id
                    WHERE u.u_id = @u_id
                )";

            var parameters = new Dictionary<string, object> { { "@u_id", userId } };
            var result = await ExecuteQueryAsync(sql, parameters);
            stopwatch.Stop();
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetZFMByUserAsync for userId={UserId}", userId);
            throw;
        }
    }

    public async Task<DataTable> GetCalendarEventsAsync()
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            const string sql = @"
                SELECT u.u_firstname, u.u_lastname, tortd.tortd_typedetail,
                       tord.tord_date, tord.tord_starthour, tord.tord_endhour,
                       tor.u_id, ISNULL(u.z_id, 0) AS z_id,
                       CASE WHEN EXISTS (
                           SELECT 1 FROM xrefUserRole xur
                           INNER JOIN xrefRoleFunction xrf ON xur.r_id = xrf.r_id
                           INNER JOIN [function] f ON xrf.f_id = f.f_id
                           WHERE f.f_functionidentifier = 'ADMIN' AND xur.u_id = tor.u_id
                       ) THEN 1 ELSE 0 END AS is_admin
                FROM timeoffrequest tor
                INNER JOIN TimeOffRequestDetail tord ON tor.tor_id = tord.tor_id
                INNER JOIN [user] u ON tor.u_id = u.u_id
                INNER JOIN TimeOffRequestTypeDetail tortd ON tor.tortd_id = tortd.tortd_id
                WHERE tor.tors_id = 1
                AND u.u_active = 1
                ORDER BY tord.tord_date, u.u_lastname";

            var result = await ExecuteQueryAsync(sql);
            stopwatch.Stop();
            _logger.LogInformation("GetCalendarEventsAsync completed in {ElapsedMs}ms, {Count} rows",
                stopwatch.ElapsedMilliseconds, result.Rows.Count);
            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error in GetCalendarEventsAsync");
            await _auditService.LogAsync(new EvoAPI.Shared.Models.AuditEntry
            {
                Name = "DataService",
                Description = "GetCalendarEvents",
                Detail = $"Error: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("F3"),
                MachineName = Environment.MachineName
            });
            throw;
        }
    }

    /// <summary>
    /// Get a config setting value by type and identifier
    /// </summary>
    public async Task<string?> GetConfigSettingValueAsync(string csType, string csIdentifier)
    {
        try
        {
            const string sql = @"
                SELECT TOP 1 cs_value 
                FROM ConfigSetting 
                WHERE cs_type = @cs_type AND cs_identifier = @cs_identifier";

            var parameters = new Dictionary<string, object>
            {
                { "@cs_type", csType },
                { "@cs_identifier", csIdentifier }
            };

            var result = await ExecuteQueryAsync(sql, parameters);
            if (result.Rows.Count > 0 && result.Rows[0]["cs_value"] != DBNull.Value)
            {
                return result.Rows[0]["cs_value"].ToString();
            }
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting config setting {CsType}/{CsIdentifier}", csType, csIdentifier);
            return null;
        }
    }

    /// <summary>
    /// Check if a user has the TECH function role
    /// </summary>
    public async Task<bool> IsUserTechAsync(int userId)
    {
        try
        {
            const string sql = @"
                SELECT COUNT(*)
                FROM xrefUserRole xur
                INNER JOIN xrefRoleFunction xrf ON xur.r_id = xrf.r_id
                INNER JOIN [function] f ON xrf.f_id = f.f_id
                WHERE f.f_functionidentifier = 'TECH'
                AND xur.u_id = @u_id";

            var parameters = new Dictionary<string, object> { { "@u_id", userId } };
            var result = await ExecuteQueryAsync(sql, parameters);
            return result.Rows.Count > 0 && Convert.ToInt32(result.Rows[0][0]) >= 1;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking IsTech for userId={UserId}", userId);
            return false;
        }
    }

    /// <summary>
    /// Get user email info (email, firstname, lastname) for notification purposes
    /// </summary>
    public async Task<DataTable> GetUserEmailInfoAsync(int userId)
    {
        try
        {
            const string sql = @"
                SELECT u_id, u_email, u_firstname, u_lastname
                FROM [user]
                WHERE u_id = @u_id";

            var parameters = new Dictionary<string, object> { { "@u_id", userId } };
            return await ExecuteQueryAsync(sql, parameters);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting user email info for userId={UserId}", userId);
            throw;
        }
    }

    /// <summary>
    /// Check if a time off request is exceeding the user's available balance
    /// </summary>
    public async Task<bool> IsTimeOffExceedingBalanceAsync(int torId)
    {
        try
        {
            const string sql = @"
                SELECT 
                    CASE 
                        WHEN tortd.tortd_id = 1 AND ISNULL(u.u_daysavailablevacation, 0) < tor.tor_totalhours THEN 1
                        WHEN tortd.tortd_id = 2 AND ISNULL(u.u_daysavailablepto, 0) < tor.tor_totalhours THEN 1
                        ELSE 0
                    END AS is_exceeding
                FROM timeoffrequest tor
                INNER JOIN timeoffrequesttypedetail tortd ON tor.tortd_id = tortd.tortd_id
                INNER JOIN [user] u ON tor.u_id = u.u_id
                WHERE tor.tor_id = @tor_id";

            var parameters = new Dictionary<string, object> { { "@tor_id", torId } };
            var result = await ExecuteQueryAsync(sql, parameters);
            return result.Rows.Count > 0 && Convert.ToInt32(result.Rows[0]["is_exceeding"]) == 1;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking balance exceeding for torId={TorId}", torId);
            return false;
        }
    }

    /// <summary>
    /// Get the balance type label for a time off request (Vacation or PTO)
    /// </summary>
    public async Task<string> GetTimeOffBalanceTypeAsync(int torId)
    {
        try
        {
            const string sql = @"
                SELECT tortd.tortd_id
                FROM timeoffrequest tor
                INNER JOIN timeoffrequesttypedetail tortd ON tor.tortd_id = tortd.tortd_id
                WHERE tor.tor_id = @tor_id";

            var parameters = new Dictionary<string, object> { { "@tor_id", torId } };
            var result = await ExecuteQueryAsync(sql, parameters);
            if (result.Rows.Count > 0)
            {
                var tortdId = Convert.ToInt32(result.Rows[0]["tortd_id"]);
                return tortdId == 1 ? "vacation" : tortdId == 2 ? "PTO" : "time off";
            }
            return "time off";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting balance type for torId={TorId}", torId);
            return "time off";
        }
    }

    #endregion

    #region Portal Info Report

    public async Task<PortalInfoReportDto> GetPortalInfoReportAsync()
    {
        var result = new PortalInfoReportDto();

        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
            throw new InvalidOperationException("No connection string found");

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        // Call Centers with at least one portal field populated
        const string callCenterSql = @"
            SELECT cc_id, cc_name, cc_portalname, cc_portalurl, cc_portalcredentials
            FROM CallCenter
            WHERE (cc_portalname IS NOT NULL AND LTRIM(RTRIM(cc_portalname)) <> '')
               OR (cc_portalurl IS NOT NULL AND LTRIM(RTRIM(cc_portalurl)) <> '')
               OR (cc_portalcredentials IS NOT NULL AND LTRIM(RTRIM(cc_portalcredentials)) <> '')
            ORDER BY cc_name";

        using (var cmd = new SqlCommand(callCenterSql, connection))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                result.CallCenters.Add(new PortalInfoCallCenterDto
                {
                    Id = Convert.ToInt32(reader["cc_id"]),
                    Name = reader["cc_name"]?.ToString() ?? string.Empty,
                    PortalName = reader["cc_portalname"] == DBNull.Value ? null : reader["cc_portalname"].ToString(),
                    PortalUrl = reader["cc_portalurl"] == DBNull.Value ? null : reader["cc_portalurl"].ToString(),
                    PortalCredentials = reader["cc_portalcredentials"] == DBNull.Value ? null : reader["cc_portalcredentials"].ToString()
                });
            }
        }

        // Companies with at least one portal field populated
        const string companySql = @"
            SELECT xccc.xccc_id, xccc.c_id, xccc.cc_id,
                   c.c_name, cc.cc_name AS call_center_name,
                   c.c_portalname, c.c_portalurl, c.c_portalcredentials
            FROM xrefCompanyCallCenter xccc
            INNER JOIN Company c ON xccc.c_id = c.c_id
            INNER JOIN CallCenter cc ON xccc.cc_id = cc.cc_id
            WHERE (c.c_portalname IS NOT NULL AND LTRIM(RTRIM(c.c_portalname)) <> '')
               OR (c.c_portalurl IS NOT NULL AND LTRIM(RTRIM(c.c_portalurl)) <> '')
               OR (c.c_portalcredentials IS NOT NULL AND LTRIM(RTRIM(c.c_portalcredentials)) <> '')
            ORDER BY cc.cc_name, c.c_name";

        using (var cmd = new SqlCommand(companySql, connection))
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                result.Companies.Add(new PortalInfoCompanyDto
                {
                    XcccId = Convert.ToInt32(reader["xccc_id"]),
                    CompanyId = Convert.ToInt32(reader["c_id"]),
                    CallCenterId = Convert.ToInt32(reader["cc_id"]),
                    CompanyName = reader["c_name"]?.ToString() ?? string.Empty,
                    CallCenterName = reader["call_center_name"]?.ToString() ?? string.Empty,
                    PortalName = reader["c_portalname"] == DBNull.Value ? null : reader["c_portalname"].ToString(),
                    PortalUrl = reader["c_portalurl"] == DBNull.Value ? null : reader["c_portalurl"].ToString(),
                    PortalCredentials = reader["c_portalcredentials"] == DBNull.Value ? null : reader["c_portalcredentials"].ToString()
                });
            }
        }

        return result;
    }

    #endregion

    #region QuickBooks Troubleshooting

    public async Task<QuickBooksServiceRequestRow?> GetServiceRequestQbInfoByRequestNumberAsync(string requestNumber)
    {
        try
        {
            const string sql = @"
                SELECT TOP 1
                    sr_id,
                    sr_requestnumber,
                    sr_quickbooks_docnumber,
                    sr_quickbooks_synctoken
                FROM servicerequest
                WHERE sr_requestnumber = @sr_requestnumber";

            var parameters = new Dictionary<string, object>
            {
                { "@sr_requestnumber", requestNumber }
            };

            var dt = await ExecuteQueryAsync(sql, parameters);
            if (dt.Rows.Count == 0) return null;

            var row = dt.Rows[0];
            return new QuickBooksServiceRequestRow
            {
                sr_id = Convert.ToInt32(row["sr_id"]),
                sr_requestnumber = row["sr_requestnumber"]?.ToString() ?? string.Empty,
                sr_quickbooks_docnumber = row["sr_quickbooks_docnumber"] == DBNull.Value ? null : row["sr_quickbooks_docnumber"].ToString(),
                sr_quickbooks_synctoken = row["sr_quickbooks_synctoken"] == DBNull.Value ? null : row["sr_quickbooks_synctoken"].ToString()
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading SR QB info for request number {RequestNumber}", requestNumber);
            throw;
        }
    }

    public async Task<bool> UpdateServiceRequestSyncTokenAsync(int srId, string syncToken)
    {
        try
        {
            const string sql = @"
                UPDATE servicerequest
                SET sr_quickbooks_synctoken = @sr_quickbooks_synctoken,
                    sr_modifieddatetime = GETDATE()
                WHERE sr_id = @sr_id";

            var parameters = new Dictionary<string, object>
            {
                { "@sr_quickbooks_synctoken", syncToken },
                { "@sr_id", srId }
            };

            var rows = await ExecuteNonQueryAsync(sql, parameters);
            return rows > 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating sr_quickbooks_synctoken for sr_id {SrId}", srId);
            throw;
        }
    }

    #endregion
}

