using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;
using System.Data;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("EvoApi/paymentmethods")]
    [EvoAuthorize]
    public class PaymentMethodController : BaseController
    {
        private readonly IDataService _dataService;

        public PaymentMethodController(IDataService dataService, IAuditService auditService)
        {
            _dataService = dataService;
            InitializeAuditService(auditService);
        }

        [HttpGet]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<PaymentMethodDto>>>> GetAll()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = "SELECT pm_id, pm_name, pm_description, pm_order, pm_active FROM PaymentMethod ORDER BY pm_order, pm_name";
                var result = await _dataService.ExecuteQueryAsync(sql);
                var items = new List<PaymentMethodDto>();
                foreach (DataRow row in result.Rows)
                {
                    items.Add(MapRow(row));
                }

                stopwatch.Stop();
                await LogAuditAsync("GetAllPaymentMethods", new { count = items.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<PaymentMethodDto>>
                {
                    Success = true,
                    Message = "Payment methods retrieved successfully",
                    Data = items,
                    Count = items.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAllPaymentMethods", ex);
                return StatusCode(500, new ApiResponse<List<PaymentMethodDto>> { Success = false, Message = "Failed to retrieve payment methods" });
            }
        }

        [HttpGet("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<PaymentMethodDto>>> Get(int id)
        {
            try
            {
                const string sql = "SELECT pm_id, pm_name, pm_description, pm_order, pm_active FROM PaymentMethod WHERE pm_id = @Id";
                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object> { { "@Id", id } });
                if (result.Rows.Count == 0)
                    return NotFound(new ApiResponse<PaymentMethodDto> { Success = false, Message = "Payment method not found" });

                return Ok(new ApiResponse<PaymentMethodDto> { Success = true, Message = "Payment method retrieved successfully", Data = MapRow(result.Rows[0]) });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetPaymentMethod", ex, new { id });
                return StatusCode(500, new ApiResponse<PaymentMethodDto> { Success = false, Message = "Failed to retrieve payment method" });
            }
        }

        [HttpPost]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<PaymentMethodDto>>> Create([FromBody] PaymentMethodRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Name))
                    return BadRequest(new ApiResponse<PaymentMethodDto> { Success = false, Message = "Name is required" });

                // Legacy default: order falls back to 10000 when unset, so new rows sort to the bottom.
                var order = request.Order == 0 ? 10000 : request.Order;

                const string sql = @"
                    INSERT INTO PaymentMethod (pm_name, pm_description, pm_order, pm_active)
                    VALUES (@Name, @Description, @Order, @Active);
                    SELECT CAST(SCOPE_IDENTITY() AS INT) AS NewId;";
                var resultTable = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@Name", request.Name.Trim() },
                    { "@Description", string.IsNullOrWhiteSpace(request.Description) ? (object)DBNull.Value : request.Description!.Trim() },
                    { "@Order", order },
                    { "@Active", request.Active }
                });
                var newId = ConvertToInt(resultTable.Rows[0]["NewId"]);

                stopwatch.Stop();
                await LogAuditAsync("CreatePaymentMethod", new { id = newId, name = request.Name, order, active = request.Active }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return StatusCode(201, new ApiResponse<PaymentMethodDto>
                {
                    Success = true,
                    Message = "Payment method created successfully",
                    Data = new PaymentMethodDto { Id = newId, Name = request.Name.Trim(), Description = request.Description?.Trim() ?? string.Empty, Order = order, Active = request.Active }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreatePaymentMethod", ex, request);
                return StatusCode(500, new ApiResponse<PaymentMethodDto> { Success = false, Message = "Failed to create payment method" });
            }
        }

        [HttpPut("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<PaymentMethodDto>>> Update(int id, [FromBody] PaymentMethodRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Name))
                    return BadRequest(new ApiResponse<PaymentMethodDto> { Success = false, Message = "Name is required" });

                const string sql = @"
                    UPDATE PaymentMethod
                    SET pm_name = @Name, pm_description = @Description, pm_order = @Order, pm_active = @Active
                    WHERE pm_id = @Id";
                await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@Id", id },
                    { "@Name", request.Name.Trim() },
                    { "@Description", string.IsNullOrWhiteSpace(request.Description) ? (object)DBNull.Value : request.Description!.Trim() },
                    { "@Order", request.Order },
                    { "@Active", request.Active }
                });

                stopwatch.Stop();
                await LogAuditAsync("UpdatePaymentMethod", new { id, name = request.Name, order = request.Order, active = request.Active }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<PaymentMethodDto>
                {
                    Success = true,
                    Message = "Payment method updated successfully",
                    Data = new PaymentMethodDto { Id = id, Name = request.Name.Trim(), Description = request.Description?.Trim() ?? string.Empty, Order = request.Order, Active = request.Active }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdatePaymentMethod", ex, new { id, request });
                return StatusCode(500, new ApiResponse<PaymentMethodDto> { Success = false, Message = "Failed to update payment method" });
            }
        }

        private static PaymentMethodDto MapRow(DataRow row)
        {
            return new PaymentMethodDto
            {
                Id = ConvertToInt(row["pm_id"]),
                Name = row["pm_name"]?.ToString() ?? string.Empty,
                Description = row["pm_description"]?.ToString() ?? string.Empty,
                Order = ConvertToInt(row["pm_order"]),
                Active = ConvertToBool(row["pm_active"])
            };
        }

        private static int ConvertToInt(object value)
        {
            if (value == null || value == DBNull.Value) return 0;
            return int.TryParse(value.ToString(), out var result) ? result : 0;
        }

        private static bool ConvertToBool(object value)
        {
            if (value == null || value == DBNull.Value) return false;
            if (bool.TryParse(value.ToString(), out var b)) return b;
            if (int.TryParse(value.ToString(), out var i)) return i != 0;
            return false;
        }
    }

    public class PaymentMethodRequest
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Description { get; set; }
        public int Order { get; set; }
        public bool Active { get; set; } = true;
    }
}
