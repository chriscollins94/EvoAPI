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
    [Route("EvoApi/regions")]
    [EvoAuthorize]
    public class RegionController : BaseController
    {
        private readonly IDataService _dataService;

        public RegionController(IDataService dataService, IAuditService auditService)
        {
            _dataService = dataService;
            InitializeAuditService(auditService);
        }

        [HttpGet]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<RegionDto>>>> GetAllRegions()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT 
                        r.reg_id,
                        r.reg_number,
                        r.reg_description,
                        r.reg_acronym,
                        r.u_id,
                        u.u_firstname + ' ' + u.u_lastname AS rfm_name,
                        u.u_picture AS rfm_picture,
                        (SELECT COUNT(*) FROM zone z WHERE z.reg_id = r.reg_id) AS zone_count
                    FROM region r
                    LEFT JOIN [user] u ON r.u_id = u.u_id
                    ORDER BY r.reg_number";

                var result = await _dataService.ExecuteQueryAsync(sql);
                var regions = new List<RegionDto>();

                foreach (DataRow row in result.Rows)
                {
                    regions.Add(new RegionDto
                    {
                        RegionId = ConvertToInt(row["reg_id"]),
                        RegionName = row["reg_number"]?.ToString() ?? string.Empty,
                        RegionDescription = row["reg_description"]?.ToString() ?? string.Empty,
                        RegionAcronym = row["reg_acronym"]?.ToString() ?? string.Empty,
                        RfmUserId = ConvertToNullableInt(row["u_id"]),
                        RfmName = row["rfm_name"]?.ToString(),
                        RfmPicture = row["rfm_picture"]?.ToString(),
                        ZoneCount = ConvertToInt(row["zone_count"])
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetAllRegions", new { count = regions.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<RegionDto>>
                {
                    Success = true,
                    Message = "Regions retrieved successfully",
                    Data = regions,
                    Count = regions.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAllRegions", ex);
                
                return StatusCode(500, new ApiResponse<List<RegionDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve regions"
                });
            }
        }

        [HttpGet("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<RegionDto>>> GetRegion(int id)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT 
                        r.reg_id,
                        r.reg_number,
                        r.reg_description,
                        r.reg_acronym,
                        r.u_id,
                        u.u_firstname + ' ' + u.u_lastname AS rfm_name,
                        u.u_picture AS rfm_picture,
                        (SELECT COUNT(*) FROM zone z WHERE z.reg_id = r.reg_id) AS zone_count
                    FROM region r
                    LEFT JOIN [user] u ON r.u_id = u.u_id
                    WHERE r.reg_id = @RegionId";

                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@RegionId", id }
                });

                if (result.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<RegionDto>
                    {
                        Success = false,
                        Message = "Region not found"
                    });
                }

                var row = result.Rows[0];
                var region = new RegionDto
                {
                    RegionId = ConvertToInt(row["reg_id"]),
                    RegionName = row["reg_number"]?.ToString() ?? string.Empty,
                    RegionDescription = row["reg_description"]?.ToString() ?? string.Empty,
                    RegionAcronym = row["reg_acronym"]?.ToString() ?? string.Empty,
                    RfmUserId = ConvertToNullableInt(row["u_id"]),
                    RfmName = row["rfm_name"]?.ToString(),
                    RfmPicture = row["rfm_picture"]?.ToString(),
                    ZoneCount = ConvertToInt(row["zone_count"])
                };

                stopwatch.Stop();
                await LogAuditAsync("GetRegion", new { regionId = id }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<RegionDto>
                {
                    Success = true,
                    Message = "Region retrieved successfully",
                    Data = region
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetRegion", ex, new { regionId = id });
                
                return StatusCode(500, new ApiResponse<RegionDto>
                {
                    Success = false,
                    Message = "Failed to retrieve region"
                });
            }
        }

        [HttpPost]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<RegionDto>>> CreateRegion([FromBody] CreateRegionRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.RegionName))
                {
                    return BadRequest(new ApiResponse<RegionDto>
                    {
                        Success = false,
                        Message = "Region name is required"
                    });
                }

                const string sql = @"
                    INSERT INTO region (reg_number, reg_description, reg_acronym, u_id)
                    VALUES (@RegionNumber, @RegionDescription, @RegionAcronym, @UserId);
                    SELECT CAST(SCOPE_IDENTITY() AS INT) AS NewId;";

                var resultTable = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@RegionNumber", request.RegionName },
                    { "@RegionDescription", string.IsNullOrWhiteSpace(request.RegionDescription) ? (object)DBNull.Value : request.RegionDescription },
                    { "@RegionAcronym", string.IsNullOrWhiteSpace(request.RegionAcronym) ? (object)DBNull.Value : request.RegionAcronym },
                    { "@UserId", request.RfmUserId.HasValue ? (object)request.RfmUserId.Value : DBNull.Value }
                });
                var result = ConvertToInt(resultTable.Rows[0]["NewId"]);

                Console.WriteLine($"[REGION DEBUG] CreateRegion - About to log audit with regionId: {result}");
                Console.WriteLine($"[REGION DEBUG] CreateRegion - regionName: {request.RegionName}, regionAcronym: {request.RegionAcronym}");
                
                stopwatch.Stop();
                await LogAuditAsync("CreateRegion", new { 
                    regionId = result, 
                    regionName = request.RegionName, 
                    regionDescription = request.RegionDescription, 
                    regionAcronym = request.RegionAcronym, 
                    rfmUserId = request.RfmUserId 
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                // Fetch the created region
                var getResult = await GetRegion(result);
                if (getResult.Result is OkObjectResult okResult && okResult.Value is ApiResponse<RegionDto> response)
                {
                    return CreatedAtAction(nameof(GetRegion), new { id = result }, response);
                }

                return StatusCode(201, new ApiResponse<RegionDto>
                {
                    Success = true,
                    Message = "Region created successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateRegion", ex, request);
                
                return StatusCode(500, new ApiResponse<RegionDto>
                {
                    Success = false,
                    Message = "Failed to create region"
                });
            }
        }

        [HttpPut("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<RegionDto>>> UpdateRegion(int id, [FromBody] UpdateRegionRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.RegionName))
                {
                    return BadRequest(new ApiResponse<RegionDto>
                    {
                        Success = false,
                        Message = "Region name is required"
                    });
                }

                const string sql = @"
                    UPDATE region 
                    SET reg_number = @RegionNumber,
                        reg_description = @RegionDescription,
                        reg_acronym = @RegionAcronym,
                        u_id = @UserId
                    WHERE reg_id = @RegionId";

                await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@RegionId", id },
                    { "@RegionNumber", request.RegionName },
                    { "@RegionDescription", string.IsNullOrWhiteSpace(request.RegionDescription) ? (object)DBNull.Value : request.RegionDescription },
                    { "@RegionAcronym", string.IsNullOrWhiteSpace(request.RegionAcronym) ? (object)DBNull.Value : request.RegionAcronym },
                    { "@UserId", request.RfmUserId.HasValue ? (object)request.RfmUserId.Value : DBNull.Value }
                });

                Console.WriteLine($"[REGION DEBUG] UpdateRegion - About to log audit with regionId: {id}");
                Console.WriteLine($"[REGION DEBUG] UpdateRegion - regionName: {request.RegionName}, regionAcronym: {request.RegionAcronym}");
                
                stopwatch.Stop();
                await LogAuditAsync("UpdateRegion", new { 
                    regionId = id, 
                    regionName = request.RegionName, 
                    regionDescription = request.RegionDescription, 
                    regionAcronym = request.RegionAcronym, 
                    rfmUserId = request.RfmUserId 
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                // Fetch the updated region
                var getResult = await GetRegion(id);
                if (getResult.Result is OkObjectResult okResult && okResult.Value is ApiResponse<RegionDto> response)
                {
                    return Ok(response);
                }

                return Ok(new ApiResponse<RegionDto>
                {
                    Success = true,
                    Message = "Region updated successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateRegion", ex, new { regionId = id, request });
                
                return StatusCode(500, new ApiResponse<RegionDto>
                {
                    Success = false,
                    Message = "Failed to update region"
                });
            }
        }

        [HttpDelete("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<object>>> DeleteRegion(int id)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                // Check if region has zones
                const string checkSql = @"
                    SELECT COUNT(*) AS ZoneCount
                    FROM zone 
                    WHERE reg_id = @RegionId";

                var checkTable = await _dataService.ExecuteQueryAsync(checkSql, new Dictionary<string, object>
                {
                    { "@RegionId", id }
                });
                var zoneCount = ConvertToInt(checkTable.Rows[0]["ZoneCount"]);

                if (zoneCount > 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = $"Cannot delete region. It has {zoneCount} zone(s) assigned to it."
                    });
                }

                const string deleteSql = @"
                    DELETE FROM region 
                    WHERE reg_id = @RegionId";

                await _dataService.ExecuteNonQueryAsync(deleteSql, new Dictionary<string, object>
                {
                    { "@RegionId", id }
                });

                stopwatch.Stop();
                await LogAuditAsync("DeleteRegion", new { regionId = id, action = "Deleted" }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Region deleted successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("DeleteRegion", ex, new { regionId = id });
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to delete region"
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
    }

    public class CreateRegionRequest
    {
        public string RegionName { get; set; } = string.Empty;
        public string? RegionDescription { get; set; }
        public string? RegionAcronym { get; set; }
        public int? RfmUserId { get; set; }
    }

    public class UpdateRegionRequest
    {
        public string RegionName { get; set; } = string.Empty;
        public string? RegionDescription { get; set; }
        public string? RegionAcronym { get; set; }
        public int? RfmUserId { get; set; }
    }
}
