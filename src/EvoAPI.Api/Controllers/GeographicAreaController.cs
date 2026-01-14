using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.Models;
using EvoAPI.Shared.DTOs;
using System.Data;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("EvoApi/geographic-areas")]
    [EvoAuthorize]
    public class GeographicAreaController : BaseController
    {
        private readonly IDataService _dataService;
        private readonly ILogger<GeographicAreaController> _logger;

        public GeographicAreaController(IDataService dataService, ILogger<GeographicAreaController> logger, IAuditService auditService)
        {
            _dataService = dataService;
            _logger = logger;
            InitializeAuditService(auditService);
        }

        [HttpGet]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<ZoneMicroDto>>>> GetAllZoneMicros()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT 
                        zm_id,
                        zm_number,
                        zm_description,
                        (SELECT COUNT(*) FROM tax WHERE tax.zm_id = zm.zm_id) AS tax_record_count
                    FROM ZoneMicro zm
                    ORDER BY zm.zm_number";

                var result = await _dataService.ExecuteQueryAsync(sql);
                var zoneMicros = new List<ZoneMicroDto>();

                foreach (DataRow row in result.Rows)
                {
                    zoneMicros.Add(new ZoneMicroDto
                    {
                        ZoneMicroId = ConvertToInt(row["zm_id"]),
                        ZoneMicroNumber = row["zm_number"]?.ToString() ?? string.Empty,
                        ZoneMicroDescription = row["zm_description"]?.ToString(),
                        TaxRecordCount = ConvertToInt(row["tax_record_count"])
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetAllZoneMicros", new { count = zoneMicros.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<ZoneMicroDto>>
                {
                    Success = true,
                    Message = "Zone Micros retrieved successfully",
                    Data = zoneMicros,
                    Count = zoneMicros.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAllZoneMicros", ex);
                
                return StatusCode(500, new ApiResponse<List<ZoneMicroDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve zone micros"
                });
            }
        }

        [HttpGet("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<ZoneMicroDto>>> GetZoneMicro(int id)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT 
                        zm_id,
                        zm_number,
                        zm_description,
                        (SELECT COUNT(*) FROM tax WHERE tax.zm_id = zm.zm_id) AS tax_record_count
                    FROM ZoneMicro zm
                    WHERE zm.zm_id = @ZoneMicroId";

                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@ZoneMicroId", id }
                });

                if (result.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<ZoneMicroDto>
                    {
                        Success = false,
                        Message = "Zone Micro not found"
                    });
                }

                var row = result.Rows[0];
                var zoneMicro = new ZoneMicroDto
                {
                    ZoneMicroId = ConvertToInt(row["zm_id"]),
                    ZoneMicroNumber = row["zm_number"]?.ToString() ?? string.Empty,
                    ZoneMicroDescription = row["zm_description"]?.ToString(),
                    TaxRecordCount = ConvertToInt(row["tax_record_count"])
                };

                stopwatch.Stop();
                await LogAuditAsync("GetZoneMicro", new { zoneMicroId = id }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<ZoneMicroDto>
                {
                    Success = true,
                    Message = "Zone Micro retrieved successfully",
                    Data = zoneMicro
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetZoneMicro", ex, new { zoneMicroId = id });
                
                return StatusCode(500, new ApiResponse<ZoneMicroDto>
                {
                    Success = false,
                    Message = "Failed to retrieve zone micro"
                });
            }
        }

        [HttpPost]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<ZoneMicroDto>>> CreateZoneMicro([FromBody] CreateZoneMicroRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    INSERT INTO ZoneMicro DEFAULT VALUES;
                    SELECT CAST(SCOPE_IDENTITY() AS INT) AS NewId;";

                var resultTable = await _dataService.ExecuteQueryAsync(sql);
                var newId = ConvertToInt(resultTable.Rows[0]["NewId"]);

                stopwatch.Stop();
                await LogAuditAsync("CreateZoneMicro", new { zoneMicroId = newId, action = "Created default zone record" }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                var getResult = await GetZoneMicro(newId);
                if (getResult.Result is OkObjectResult okResult && okResult.Value is ApiResponse<ZoneMicroDto> response)
                {
                    return CreatedAtAction(nameof(GetZoneMicro), new { id = newId }, response);
                }

                return StatusCode(201, new ApiResponse<ZoneMicroDto>
                {
                    Success = true,
                    Message = "Zone Micro created successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateZoneMicro", ex, request);
                
                return StatusCode(500, new ApiResponse<ZoneMicroDto>
                {
                    Success = false,
                    Message = "Failed to create zone micro"
                });
            }
        }

        [HttpDelete("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<object>>> DeleteZoneMicro(int id)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                // Check if zone micro has tax records
                const string checkSql = @"
                    SELECT COUNT(*) AS TaxCount
                    FROM tax 
                    WHERE zm_id = @ZoneMicroId";

                var checkResult = await _dataService.ExecuteQueryAsync(checkSql, new Dictionary<string, object>
                {
                    { "@ZoneMicroId", id }
                });

                var taxCount = ConvertToInt(checkResult.Rows[0]["TaxCount"]);
                if (taxCount > 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = $"Cannot delete Zone Micro with {taxCount} associated tax record(s)"
                    });
                }

                // Delete the zone micro
                const string deleteSql = @"
                    DELETE FROM ZoneMicro 
                    WHERE zm_id = @ZoneMicroId";

                await _dataService.ExecuteNonQueryAsync(deleteSql, new Dictionary<string, object>
                {
                    { "@ZoneMicroId", id }
                });

                stopwatch.Stop();
                await LogAuditAsync("DeleteZoneMicro", new { zoneMicroId = id }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Zone Micro deleted successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("DeleteZoneMicro", ex, new { zoneMicroId = id });
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to delete zone micro"
                });
            }
        }

        [HttpGet("{zmId}/tax-records")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<TaxRecordDto>>>> GetTaxRecords(int zmId)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT 
                        tax_id,
                        tax_zip,
                        tax_city,
                        tax_county,
                        tax_state,
                        zm_id
                    FROM tax
                    WHERE zm_id = @ZoneMicroId
                    ORDER BY tax_state, tax_zip";

                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@ZoneMicroId", zmId }
                });

                var taxRecords = new List<TaxRecordDto>();

                foreach (DataRow row in result.Rows)
                {
                    taxRecords.Add(new TaxRecordDto
                    {
                        TaxId = ConvertToInt(row["tax_id"]),
                        TaxZip = row["tax_zip"]?.ToString() ?? string.Empty,
                        TaxCity = row["tax_city"]?.ToString(),
                        TaxCounty = row["tax_county"]?.ToString(),
                        TaxState = row["tax_state"]?.ToString(),
                        ZoneMicroId = zmId
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetTaxRecords", new { zmId, count = taxRecords.Count, records = taxRecords }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<TaxRecordDto>>
                {
                    Success = true,
                    Message = "Tax records retrieved successfully",
                    Data = taxRecords,
                    Count = taxRecords.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetTaxRecords", ex, new { zmId });
                
                return StatusCode(500, new ApiResponse<List<TaxRecordDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve tax records"
                });
            }
        }

        [HttpGet("tax-records/{taxId}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<TaxRecordDto>>> GetTaxRecord(int taxId)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT 
                        tax_id,
                        tax_zip,
                        tax_city,
                        tax_county,
                        tax_state,
                        zm_id
                    FROM tax
                    WHERE tax_id = @TaxId";

                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@TaxId", taxId }
                });

                if (result.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<TaxRecordDto>
                    {
                        Success = false,
                        Message = "Tax record not found"
                    });
                }

                var row = result.Rows[0];
                var taxRecord = new TaxRecordDto
                {
                    TaxId = ConvertToInt(row["tax_id"]),
                    TaxZip = row["tax_zip"]?.ToString() ?? string.Empty,
                    TaxCity = row["tax_city"]?.ToString(),
                    TaxCounty = row["tax_county"]?.ToString(),
                    TaxState = row["tax_state"]?.ToString(),
                    ZoneMicroId = ConvertToInt(row["zm_id"])
                };

                stopwatch.Stop();
                await LogAuditAsync("GetTaxRecord", new { taxId }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<TaxRecordDto>
                {
                    Success = true,
                    Message = "Tax record retrieved successfully",
                    Data = taxRecord
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetTaxRecord", ex, new { taxId });
                
                return StatusCode(500, new ApiResponse<TaxRecordDto>
                {
                    Success = false,
                    Message = "Failed to retrieve tax record"
                });
            }
        }

        [HttpPost("{zmId}/tax-records")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<TaxRecordDto>>> CreateTaxRecord(int zmId, [FromBody] CreateTaxRecordRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.TaxZip) || string.IsNullOrWhiteSpace(request.TaxState))
                {
                    return BadRequest(new ApiResponse<TaxRecordDto>
                    {
                        Success = false,
                        Message = "Tax Zip and State are required"
                    });
                }

                const string sql = @"
                    INSERT INTO tax (zm_id, tax_zip, tax_city, tax_county, tax_state, tax_insertdatetime)
                    VALUES (@ZoneMicroId, @TaxZip, @TaxCity, @TaxCounty, @TaxState, GETUTCDATE());
                    SELECT CAST(SCOPE_IDENTITY() AS INT) AS NewId;";

                var resultTable = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@ZoneMicroId", zmId },
                    { "@TaxZip", request.TaxZip },
                    { "@TaxCity", string.IsNullOrWhiteSpace(request.TaxCity) ? (object)DBNull.Value : request.TaxCity },
                    { "@TaxCounty", string.IsNullOrWhiteSpace(request.TaxCounty) ? (object)DBNull.Value : request.TaxCounty },
                    { "@TaxState", request.TaxState }
                });

                var newId = ConvertToInt(resultTable.Rows[0]["NewId"]);

                stopwatch.Stop();
                await LogAuditAsync("CreateTaxRecord", new { taxId = newId, zmId = request.ZoneMicroId, taxZip = request.TaxZip, taxCity = request.TaxCity, taxCounty = request.TaxCounty, taxState = request.TaxState }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                var getResult = await GetTaxRecord(newId);
                if (getResult.Result is OkObjectResult okResult && okResult.Value is ApiResponse<TaxRecordDto> response)
                {
                    return CreatedAtAction(nameof(GetTaxRecord), new { taxId = newId }, response);
                }

                return StatusCode(201, new ApiResponse<TaxRecordDto>
                {
                    Success = true,
                    Message = "Tax record created successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateTaxRecord", ex, new { zmId, request });
                
                return StatusCode(500, new ApiResponse<TaxRecordDto>
                {
                    Success = false,
                    Message = "Failed to create tax record"
                });
            }
        }

        [HttpPut("tax-records/{taxId}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<TaxRecordDto>>> UpdateTaxRecord(int taxId, [FromBody] UpdateTaxRecordRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.TaxZip) || string.IsNullOrWhiteSpace(request.TaxState))
                {
                    return BadRequest(new ApiResponse<TaxRecordDto>
                    {
                        Success = false,
                        Message = "Tax Zip and State are required"
                    });
                }

                const string sql = @"
                    UPDATE tax 
                    SET zm_id = @ZoneMicroId,
                        tax_zip = @TaxZip,
                        tax_city = @TaxCity,
                        tax_county = @TaxCounty,
                        tax_state = @TaxState,
                        tax_modifieddatetime = GETUTCDATE()
                    WHERE tax_id = @TaxId";

                await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@TaxId", taxId },
                    { "@ZoneMicroId", request.ZoneMicroId },
                    { "@TaxZip", request.TaxZip },
                    { "@TaxCity", string.IsNullOrWhiteSpace(request.TaxCity) ? (object)DBNull.Value : request.TaxCity },
                    { "@TaxCounty", string.IsNullOrWhiteSpace(request.TaxCounty) ? (object)DBNull.Value : request.TaxCounty },
                    { "@TaxState", request.TaxState }
                });

                stopwatch.Stop();
                await LogAuditAsync("UpdateTaxRecord", new { taxId, zmId = request.ZoneMicroId, taxZip = request.TaxZip, taxCity = request.TaxCity, taxCounty = request.TaxCounty, taxState = request.TaxState }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                var getResult = await GetTaxRecord(taxId);
                if (getResult.Result is OkObjectResult okResult && okResult.Value is ApiResponse<TaxRecordDto> response)
                {
                    return Ok(response);
                }

                return Ok(new ApiResponse<TaxRecordDto>
                {
                    Success = true,
                    Message = "Tax record updated successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateTaxRecord", ex, new { taxId, request });
                
                return StatusCode(500, new ApiResponse<TaxRecordDto>
                {
                    Success = false,
                    Message = "Failed to update tax record"
                });
            }
        }

        [HttpDelete("tax-records/{taxId}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<object>>> DeleteTaxRecord(int taxId)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    DELETE FROM tax 
                    WHERE tax_id = @TaxId";

                await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@TaxId", taxId }
                });

                stopwatch.Stop();
                await LogAuditAsync("DeleteTaxRecord", new { taxId, action = "Deleted" }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Tax record deleted successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("DeleteTaxRecord", ex, new { taxId });
                
                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to delete tax record"
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

    public class CreateZoneMicroRequest
    {
        // Currently just creating empty record, can be extended
    }

    public class CreateTaxRecordRequest
    {
        public int ZoneMicroId { get; set; }
        public string TaxZip { get; set; } = string.Empty;
        public string? TaxCity { get; set; }
        public string? TaxCounty { get; set; }
        public string TaxState { get; set; } = string.Empty;
    }

    public class UpdateTaxRecordRequest
    {
        public int ZoneMicroId { get; set; }
        public string TaxZip { get; set; } = string.Empty;
        public string? TaxCity { get; set; }
        public string? TaxCounty { get; set; }
        public string TaxState { get; set; } = string.Empty;
    }
}
