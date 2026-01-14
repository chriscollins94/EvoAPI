using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Models;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;
using System.Data;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("EvoApi/zones")]
    [EvoAuthorize]
    public class ZoneController : BaseController
    {
        private readonly IDataService _dataService;

        public ZoneController(IDataService dataService, IAuditService auditService)
        {
            _dataService = dataService;
            InitializeAuditService(auditService);
        }

        [HttpGet]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<ZoneDto>>>> GetAllZones()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT 
                        z.z_id,
                        z.z_number,
                        z.z_description,
                        z.z_acronym,
                        z.z_email,
                        z.u_id,
                        z.reg_id,
                        u.u_firstname + ' ' + u.u_lastname AS zfm_name,
                        u.u_picture AS zfm_picture,
                        r.reg_number AS reg_name,
                        (SELECT COUNT(*) FROM [user] emp WHERE emp.z_id = z.z_id AND emp.u_active = 1) AS employee_count
                    FROM zone z
                    LEFT JOIN [user] u ON z.u_id = u.u_id
                    LEFT JOIN region r ON z.reg_id = r.reg_id
                    ORDER BY z.z_number";

                var result = await _dataService.ExecuteQueryAsync(sql);
                var zones = new List<ZoneDto>();

                foreach (DataRow row in result.Rows)
                {
                    zones.Add(new ZoneDto
                    {
                        ZoneId = ConvertToInt(row["z_id"]),
                        ZoneNumber = row["z_number"]?.ToString() ?? string.Empty,
                        ZoneDescription = row["z_description"]?.ToString() ?? string.Empty,
                        ZoneAcronym = row["z_acronym"]?.ToString() ?? string.Empty,
                        ZoneEmail = row["z_email"]?.ToString(),
                        ZfmUserId = ConvertToNullableInt(row["u_id"]),
                        ZfmName = row["zfm_name"]?.ToString(),
                        ZfmPicture = row["zfm_picture"]?.ToString(),
                        RegionId = ConvertToNullableInt(row["reg_id"]),
                        RegionName = row["reg_name"]?.ToString(),
                        EmployeeCount = ConvertToInt(row["employee_count"])
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetAllZones", new { count = zones.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<ZoneDto>>
                {
                    Success = true,
                    Message = "Zones retrieved successfully",
                    Data = zones,
                    Count = zones.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAllZones", ex);
                
                return StatusCode(500, new ApiResponse<List<ZoneDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve zones"
                });
            }
        }

        [HttpGet("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<ZoneDto>>> GetZone(int id)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT 
                        z.z_id,
                        z.z_number,
                        z.z_description,
                        z.z_acronym,
                        z.z_email,
                        z.u_id,
                        z.reg_id,
                        u.u_firstname + ' ' + u.u_lastname AS zfm_name,
                        u.u_picture AS zfm_picture,
                        r.reg_number AS reg_name,
                        (SELECT COUNT(*) FROM [user] emp WHERE emp.z_id = z.z_id AND emp.u_active = 1) AS employee_count
                    FROM zone z
                    LEFT JOIN [user] u ON z.u_id = u.u_id
                    LEFT JOIN region r ON z.reg_id = r.reg_id
                    WHERE z.z_id = @ZoneId";

                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@ZoneId", id }
                });

                if (result.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<ZoneDto>
                    {
                        Success = false,
                        Message = "Zone not found"
                    });
                }

                var row = result.Rows[0];
                var zone = new ZoneDto
                {
                    ZoneId = ConvertToInt(row["z_id"]),
                    ZoneNumber = row["z_number"]?.ToString() ?? string.Empty,
                    ZoneDescription = row["z_description"]?.ToString() ?? string.Empty,
                    ZoneAcronym = row["z_acronym"]?.ToString() ?? string.Empty,
                    ZoneEmail = row["z_email"]?.ToString(),
                    ZfmUserId = ConvertToNullableInt(row["u_id"]),
                    ZfmName = row["zfm_name"]?.ToString(),
                    ZfmPicture = row["zfm_picture"]?.ToString(),
                    RegionId = ConvertToNullableInt(row["reg_id"]),
                    RegionName = row["reg_name"]?.ToString(),
                    EmployeeCount = ConvertToInt(row["employee_count"])
                };

                stopwatch.Stop();
                await LogAuditAsync("GetZone", new { zoneId = id }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<ZoneDto>
                {
                    Success = true,
                    Message = "Zone retrieved successfully",
                    Data = zone
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetZone", ex, new { zoneId = id });
                
                return StatusCode(500, new ApiResponse<ZoneDto>
                {
                    Success = false,
                    Message = "Failed to retrieve zone"
                });
            }
        }

        [HttpGet("{id}/employees")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<ZoneEmployeeDto>>>> GetZoneEmployees(int id)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT 
                        u.u_id,
                        u.u_firstname,
                        u.u_lastname,
                        u.u_picture,
                        u.u_active,
                        u.u_employeenumber
                    FROM [user] u
                    WHERE u.z_id = @ZoneId
                    ORDER BY u.u_lastname, u.u_firstname";

                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@ZoneId", id }
                });

                var employees = new List<ZoneEmployeeDto>();
                foreach (DataRow row in result.Rows)
                {
                    employees.Add(new ZoneEmployeeDto
                    {
                        UserId = ConvertToInt(row["u_id"]),
                        FirstName = row["u_firstname"]?.ToString() ?? string.Empty,
                        LastName = row["u_lastname"]?.ToString() ?? string.Empty,
                        Picture = row["u_picture"]?.ToString(),
                        Active = ConvertToBool(row["u_active"]),
                        EmployeeNumber = row["u_employeenumber"]?.ToString()
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetZoneEmployees", new { zoneId = id, count = employees.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<ZoneEmployeeDto>>
                {
                    Success = true,
                    Message = "Zone employees retrieved successfully",
                    Data = employees,
                    Count = employees.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetZoneEmployees", ex, new { zoneId = id });
                
                return StatusCode(500, new ApiResponse<List<ZoneEmployeeDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve zone employees"
                });
            }
        }

        [HttpPost]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<ZoneDto>>> CreateZone([FromBody] CreateZoneRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.ZoneNumber) || string.IsNullOrWhiteSpace(request.ZoneAcronym))
                {
                    return BadRequest(new ApiResponse<ZoneDto>
                    {
                        Success = false,
                        Message = "Zone number and acronym are required"
                    });
                }

                const string sql = @"
                    INSERT INTO zone (z_number, z_description, z_acronym, z_email, u_id, reg_id)
                    VALUES (@ZoneNumber, @ZoneDescription, @ZoneAcronym, @ZoneEmail, @UserId, @RegionId);
                    SELECT CAST(SCOPE_IDENTITY() AS INT) AS NewId;";

                var resultTable = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@ZoneNumber", request.ZoneNumber },
                    { "@ZoneDescription", request.ZoneDescription ?? string.Empty },
                    { "@ZoneAcronym", request.ZoneAcronym },
                    { "@ZoneEmail", request.ZoneEmail ?? string.Empty },
                    { "@UserId", request.ZfmUserId.HasValue ? (object)request.ZfmUserId.Value : DBNull.Value },
                    { "@RegionId", request.RegionId.HasValue ? (object)request.RegionId.Value : DBNull.Value }
                });
                var result = ConvertToInt(resultTable.Rows[0]["NewId"]);

                Console.WriteLine($"[ZONE DEBUG] CreateZone - About to log audit with zoneId: {result}");
                Console.WriteLine($"[ZONE DEBUG] CreateZone - zoneNumber: {request.ZoneNumber}, zoneAcronym: {request.ZoneAcronym}, zoneEmail: {request.ZoneEmail}");
                
                stopwatch.Stop();
                await LogAuditAsync("CreateZone", new { 
                    zoneId = result, 
                    zoneNumber = request.ZoneNumber, 
                    zoneDescription = request.ZoneDescription, 
                    zoneAcronym = request.ZoneAcronym,
                    zoneEmail = request.ZoneEmail,
                    zfmUserId = request.ZfmUserId, 
                    regionId = request.RegionId 
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                // Fetch the created zone
                var getResult = await GetZone(result);
                if (getResult.Result is OkObjectResult okResult && okResult.Value is ApiResponse<ZoneDto> response)
                {
                    return CreatedAtAction(nameof(GetZone), new { id = result }, response);
                }

                return StatusCode(201, new ApiResponse<ZoneDto>
                {
                    Success = true,
                    Message = "Zone created successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateZone", ex, request);
                
                return StatusCode(500, new ApiResponse<ZoneDto>
                {
                    Success = false,
                    Message = "Failed to create zone"
                });
            }
        }

        [HttpPut("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<ZoneDto>>> UpdateZone(int id, [FromBody] UpdateZoneRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.ZoneNumber) || string.IsNullOrWhiteSpace(request.ZoneAcronym))
                {
                    return BadRequest(new ApiResponse<ZoneDto>
                    {
                        Success = false,
                        Message = "Zone number and acronym are required"
                    });
                }

                const string sql = @"
                    UPDATE zone 
                    SET z_number = @ZoneNumber,
                        z_description = @ZoneDescription,
                        z_acronym = @ZoneAcronym,
                        z_email = @ZoneEmail,
                        u_id = @UserId,
                        reg_id = @RegionId
                    WHERE z_id = @ZoneId";

                await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@ZoneId", id },
                    { "@ZoneNumber", request.ZoneNumber },
                    { "@ZoneDescription", request.ZoneDescription ?? string.Empty },
                    { "@ZoneAcronym", request.ZoneAcronym },
                    { "@ZoneEmail", request.ZoneEmail ?? string.Empty },
                    { "@UserId", request.ZfmUserId.HasValue ? (object)request.ZfmUserId.Value : DBNull.Value },
                    { "@RegionId", request.RegionId.HasValue ? (object)request.RegionId.Value : DBNull.Value }
                });

                Console.WriteLine($"[ZONE DEBUG] UpdateZone - About to log audit with zoneId: {id}");
                Console.WriteLine($"[ZONE DEBUG] UpdateZone - zoneNumber: {request.ZoneNumber}, zoneAcronym: {request.ZoneAcronym}, zoneEmail: {request.ZoneEmail}");
                
                stopwatch.Stop();
                await LogAuditAsync("UpdateZone", new { 
                    zoneId = id, 
                    zoneNumber = request.ZoneNumber, 
                    zoneDescription = request.ZoneDescription, 
                    zoneAcronym = request.ZoneAcronym,
                    zoneEmail = request.ZoneEmail,
                    zfmUserId = request.ZfmUserId, 
                    regionId = request.RegionId 
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                // Fetch the updated zone
                var getResult = await GetZone(id);
                if (getResult.Result is OkObjectResult okResult && okResult.Value is ApiResponse<ZoneDto> response)
                {
                    return Ok(response);
                }

                return Ok(new ApiResponse<ZoneDto>
                {
                    Success = true,
                    Message = "Zone updated successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateZone", ex, new { zoneId = id, request });
                
                return StatusCode(500, new ApiResponse<ZoneDto>
                {
                    Success = false,
                    Message = "Failed to update zone"
                });
            }
        }

        [HttpDelete("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<object>>> DeleteZone(int id)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                // Check if zone has employees
                const string checkSql = @"
                    SELECT COUNT(*) AS EmployeeCount
                    FROM [user] 
                    WHERE z_id = @ZoneId";

                var checkTable = await _dataService.ExecuteQueryAsync(checkSql, new Dictionary<string, object>
                {
                    { "@ZoneId", id }
                });
                var employeeCount = ConvertToInt(checkTable.Rows[0]["EmployeeCount"]);

                if (employeeCount > 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = $"Cannot delete zone. It has {employeeCount} employee(s) assigned to it."
                    });
                }

                const string deleteSql = @"
                    DELETE FROM zone 
                    WHERE z_id = @ZoneId";

                await _dataService.ExecuteNonQueryAsync(deleteSql, new Dictionary<string, object>
                {
                    { "@ZoneId", id }
                });

                stopwatch.Stop();
                await LogAuditAsync("DeleteZone", new { zoneId = id, action = "Deleted" }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Zone deleted successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("DeleteZone", ex, new { zoneId = id });
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to delete zone"
                });
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

        private static int? ConvertToNullableInt(object value)
        {
            if (value == null || value == DBNull.Value)
                return null;
            
            if (int.TryParse(value.ToString(), out var result))
                return result;
                
            return null;
        }

        private static bool ConvertToBool(object value)
        {
            if (value == null || value == DBNull.Value)
                return false;
            
            if (bool.TryParse(value.ToString(), out var result))
                return result;
                
            return false;
        }
    }

    public class CreateZoneRequest
    {
        public string ZoneNumber { get; set; } = string.Empty;
        public string? ZoneDescription { get; set; }
        public string ZoneAcronym { get; set; } = string.Empty;
        public string? ZoneEmail { get; set; }
        public int? ZfmUserId { get; set; }
        public int? RegionId { get; set; }
    }

    public class UpdateZoneRequest
    {
        public string ZoneNumber { get; set; } = string.Empty;
        public string? ZoneDescription { get; set; }
        public string ZoneAcronym { get; set; } = string.Empty;
        public string? ZoneEmail { get; set; }
        public int? ZfmUserId { get; set; }
        public int? RegionId { get; set; }
    }
}
