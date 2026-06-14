using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;
using System.Data;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("EvoApi/serviceitemunits")]
    [EvoAuthorize]
    public class ServiceItemUnitController : BaseController
    {
        private readonly IDataService _dataService;

        public ServiceItemUnitController(IDataService dataService, IAuditService auditService)
        {
            _dataService = dataService;
            InitializeAuditService(auditService);
        }

        [HttpGet]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<ServiceItemUnitLookupDto>>>> GetAll()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = "SELECT siu_id, siu_unit, siu_description, siu_order FROM ServiceItemUnit ORDER BY siu_order, siu_unit";
                var result = await _dataService.ExecuteQueryAsync(sql);
                var items = new List<ServiceItemUnitLookupDto>();
                foreach (DataRow row in result.Rows)
                {
                    items.Add(MapRow(row));
                }

                stopwatch.Stop();
                await LogAuditAsync("GetAllServiceItemUnits", new { count = items.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<ServiceItemUnitLookupDto>>
                {
                    Success = true,
                    Message = "Service item units retrieved successfully",
                    Data = items,
                    Count = items.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAllServiceItemUnits", ex);
                return StatusCode(500, new ApiResponse<List<ServiceItemUnitLookupDto>> { Success = false, Message = "Failed to retrieve service item units" });
            }
        }

        [HttpGet("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<ServiceItemUnitLookupDto>>> Get(int id)
        {
            try
            {
                const string sql = "SELECT siu_id, siu_unit, siu_description, siu_order FROM ServiceItemUnit WHERE siu_id = @Id";
                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object> { { "@Id", id } });
                if (result.Rows.Count == 0)
                    return NotFound(new ApiResponse<ServiceItemUnitLookupDto> { Success = false, Message = "Service item unit not found" });

                return Ok(new ApiResponse<ServiceItemUnitLookupDto> { Success = true, Message = "Service item unit retrieved successfully", Data = MapRow(result.Rows[0]) });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetServiceItemUnit", ex, new { id });
                return StatusCode(500, new ApiResponse<ServiceItemUnitLookupDto> { Success = false, Message = "Failed to retrieve service item unit" });
            }
        }

        [HttpPost]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<ServiceItemUnitLookupDto>>> Create([FromBody] ServiceItemUnitRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Unit))
                    return BadRequest(new ApiResponse<ServiceItemUnitLookupDto> { Success = false, Message = "Unit is required" });

                var order = request.Order == 0 ? 10000 : request.Order;

                const string sql = @"
                    INSERT INTO ServiceItemUnit (siu_unit, siu_description, siu_order)
                    VALUES (@Unit, @Description, @Order);
                    SELECT CAST(SCOPE_IDENTITY() AS INT) AS NewId;";
                var resultTable = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@Unit", request.Unit.Trim() },
                    { "@Description", string.IsNullOrWhiteSpace(request.Description) ? (object)DBNull.Value : request.Description!.Trim() },
                    { "@Order", order }
                });
                var newId = ConvertToInt(resultTable.Rows[0]["NewId"]);

                stopwatch.Stop();
                await LogAuditAsync("CreateServiceItemUnit", new { id = newId, unit = request.Unit, order }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return StatusCode(201, new ApiResponse<ServiceItemUnitLookupDto>
                {
                    Success = true,
                    Message = "Service item unit created successfully",
                    Data = new ServiceItemUnitLookupDto { Id = newId, Unit = request.Unit.Trim(), Description = request.Description?.Trim() ?? string.Empty, Order = order }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateServiceItemUnit", ex, request);
                return StatusCode(500, new ApiResponse<ServiceItemUnitLookupDto> { Success = false, Message = "Failed to create service item unit" });
            }
        }

        [HttpPut("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<ServiceItemUnitLookupDto>>> Update(int id, [FromBody] ServiceItemUnitRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Unit))
                    return BadRequest(new ApiResponse<ServiceItemUnitLookupDto> { Success = false, Message = "Unit is required" });

                const string sql = @"
                    UPDATE ServiceItemUnit
                    SET siu_unit = @Unit, siu_description = @Description, siu_order = @Order
                    WHERE siu_id = @Id";
                await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@Id", id },
                    { "@Unit", request.Unit.Trim() },
                    { "@Description", string.IsNullOrWhiteSpace(request.Description) ? (object)DBNull.Value : request.Description!.Trim() },
                    { "@Order", request.Order }
                });

                stopwatch.Stop();
                await LogAuditAsync("UpdateServiceItemUnit", new { id, unit = request.Unit, order = request.Order }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<ServiceItemUnitLookupDto>
                {
                    Success = true,
                    Message = "Service item unit updated successfully",
                    Data = new ServiceItemUnitLookupDto { Id = id, Unit = request.Unit.Trim(), Description = request.Description?.Trim() ?? string.Empty, Order = request.Order }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateServiceItemUnit", ex, new { id, request });
                return StatusCode(500, new ApiResponse<ServiceItemUnitLookupDto> { Success = false, Message = "Failed to update service item unit" });
            }
        }

        private static ServiceItemUnitLookupDto MapRow(DataRow row)
        {
            return new ServiceItemUnitLookupDto
            {
                Id = ConvertToInt(row["siu_id"]),
                Unit = row["siu_unit"]?.ToString() ?? string.Empty,
                Description = row["siu_description"]?.ToString() ?? string.Empty,
                Order = ConvertToInt(row["siu_order"])
            };
        }

        private static int ConvertToInt(object value)
        {
            if (value == null || value == DBNull.Value) return 0;
            return int.TryParse(value.ToString(), out var result) ? result : 0;
        }
    }

    public class ServiceItemUnitRequest
    {
        public int Id { get; set; }
        public string Unit { get; set; } = string.Empty;
        public string? Description { get; set; }
        public int Order { get; set; }
    }
}
