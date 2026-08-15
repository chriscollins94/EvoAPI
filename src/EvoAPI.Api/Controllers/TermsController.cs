using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;
using System.Data;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("EvoApi/terms")]
    [EvoAuthorize]
    public class TermsController : BaseController
    {
        private readonly IDataService _dataService;

        public TermsController(IDataService dataService, IAuditService auditService)
        {
            _dataService = dataService;
            InitializeAuditService(auditService);
        }

        [HttpGet]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<TermsLookupDto>>>> GetAll()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = "SELECT terms_id, terms_description, terms_numberofdays, terms_order FROM Terms ORDER BY terms_order, terms_description";
                var result = await _dataService.ExecuteQueryAsync(sql);
                var items = new List<TermsLookupDto>();
                foreach (DataRow row in result.Rows)
                {
                    items.Add(MapRow(row));
                }

                stopwatch.Stop();
                await LogAuditAsync("GetAllTerms", new { count = items.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<TermsLookupDto>>
                {
                    Success = true,
                    Message = "Terms retrieved successfully",
                    Data = items,
                    Count = items.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAllTerms", ex);
                return StatusCode(500, new ApiResponse<List<TermsLookupDto>> { Success = false, Message = "Failed to retrieve terms" });
            }
        }

        [HttpGet("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<TermsLookupDto>>> Get(int id)
        {
            try
            {
                const string sql = "SELECT terms_id, terms_description, terms_numberofdays, terms_order FROM Terms WHERE terms_id = @Id";
                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object> { { "@Id", id } });
                if (result.Rows.Count == 0)
                    return NotFound(new ApiResponse<TermsLookupDto> { Success = false, Message = "Terms not found" });

                return Ok(new ApiResponse<TermsLookupDto> { Success = true, Message = "Terms retrieved successfully", Data = MapRow(result.Rows[0]) });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetTerms", ex, new { id });
                return StatusCode(500, new ApiResponse<TermsLookupDto> { Success = false, Message = "Failed to retrieve terms" });
            }
        }

        [HttpPost]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<TermsLookupDto>>> Create([FromBody] TermsRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Description))
                    return BadRequest(new ApiResponse<TermsLookupDto> { Success = false, Message = "Description is required" });

                var order = request.Order == 0 ? 10000 : request.Order;

                const string sql = @"
                    INSERT INTO Terms (terms_description, terms_numberofdays, terms_order)
                    VALUES (@Description, @NumberOfDays, @Order);
                    SELECT CAST(SCOPE_IDENTITY() AS INT) AS NewId;";
                var resultTable = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@Description", request.Description.Trim() },
                    { "@NumberOfDays", request.NumberOfDays },
                    { "@Order", order }
                });
                var newId = ConvertToInt(resultTable.Rows[0]["NewId"]);

                stopwatch.Stop();
                await LogAuditAsync("CreateTerms", new { id = newId, description = request.Description, numberOfDays = request.NumberOfDays, order }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return StatusCode(201, new ApiResponse<TermsLookupDto>
                {
                    Success = true,
                    Message = "Terms created successfully",
                    Data = new TermsLookupDto { Id = newId, Description = request.Description.Trim(), NumberOfDays = request.NumberOfDays, Order = order }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateTerms", ex, request);
                return StatusCode(500, new ApiResponse<TermsLookupDto> { Success = false, Message = "Failed to create terms" });
            }
        }

        [HttpPut("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<TermsLookupDto>>> Update(int id, [FromBody] TermsRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Description))
                    return BadRequest(new ApiResponse<TermsLookupDto> { Success = false, Message = "Description is required" });

                const string sql = @"
                    UPDATE Terms
                    SET terms_description = @Description, terms_numberofdays = @NumberOfDays, terms_order = @Order
                    WHERE terms_id = @Id";
                await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@Id", id },
                    { "@Description", request.Description.Trim() },
                    { "@NumberOfDays", request.NumberOfDays },
                    { "@Order", request.Order }
                });

                stopwatch.Stop();
                await LogAuditAsync("UpdateTerms", new { id, description = request.Description, numberOfDays = request.NumberOfDays, order = request.Order }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<TermsLookupDto>
                {
                    Success = true,
                    Message = "Terms updated successfully",
                    Data = new TermsLookupDto { Id = id, Description = request.Description.Trim(), NumberOfDays = request.NumberOfDays, Order = request.Order }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateTerms", ex, new { id, request });
                return StatusCode(500, new ApiResponse<TermsLookupDto> { Success = false, Message = "Failed to update terms" });
            }
        }

        private static TermsLookupDto MapRow(DataRow row)
        {
            return new TermsLookupDto
            {
                Id = ConvertToInt(row["terms_id"]),
                Description = row["terms_description"]?.ToString() ?? string.Empty,
                NumberOfDays = ConvertToInt(row["terms_numberofdays"]),
                Order = ConvertToInt(row["terms_order"])
            };
        }

        private static int ConvertToInt(object value)
        {
            if (value == null || value == DBNull.Value) return 0;
            return int.TryParse(value.ToString(), out var result) ? result : 0;
        }
    }

    public class TermsRequest
    {
        public int Id { get; set; }
        public string Description { get; set; } = string.Empty;
        public int NumberOfDays { get; set; }
        public int Order { get; set; }
    }
}
