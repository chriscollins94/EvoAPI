using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;
using System.Data;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("EvoApi/addresstitles")]
    [EvoAuthorize]
    public class AddressTitleController : BaseController
    {
        private readonly IDataService _dataService;

        public AddressTitleController(IDataService dataService, IAuditService auditService)
        {
            _dataService = dataService;
            InitializeAuditService(auditService);
        }

        [HttpGet]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<AddressTitleLookupDto>>>> GetAll()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = "SELECT at_id, at_title FROM AddressTitle ORDER BY at_title";
                var result = await _dataService.ExecuteQueryAsync(sql);
                var items = new List<AddressTitleLookupDto>();
                foreach (DataRow row in result.Rows)
                {
                    items.Add(new AddressTitleLookupDto
                    {
                        Id = ConvertToInt(row["at_id"]),
                        Title = row["at_title"]?.ToString() ?? string.Empty
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetAllAddressTitles", new { count = items.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<AddressTitleLookupDto>>
                {
                    Success = true,
                    Message = "Address titles retrieved successfully",
                    Data = items,
                    Count = items.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAllAddressTitles", ex);
                return StatusCode(500, new ApiResponse<List<AddressTitleLookupDto>> { Success = false, Message = "Failed to retrieve address titles" });
            }
        }

        [HttpGet("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<AddressTitleLookupDto>>> Get(int id)
        {
            try
            {
                const string sql = "SELECT at_id, at_title FROM AddressTitle WHERE at_id = @Id";
                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object> { { "@Id", id } });
                if (result.Rows.Count == 0)
                    return NotFound(new ApiResponse<AddressTitleLookupDto> { Success = false, Message = "Address title not found" });

                var row = result.Rows[0];
                return Ok(new ApiResponse<AddressTitleLookupDto>
                {
                    Success = true,
                    Message = "Address title retrieved successfully",
                    Data = new AddressTitleLookupDto { Id = ConvertToInt(row["at_id"]), Title = row["at_title"]?.ToString() ?? string.Empty }
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetAddressTitle", ex, new { id });
                return StatusCode(500, new ApiResponse<AddressTitleLookupDto> { Success = false, Message = "Failed to retrieve address title" });
            }
        }

        [HttpPost]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<AddressTitleLookupDto>>> Create([FromBody] AddressTitleRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Title))
                    return BadRequest(new ApiResponse<AddressTitleLookupDto> { Success = false, Message = "Title is required" });

                // o_id is NOT NULL; legacy app hardcodes 1. at_active is forced true (not surfaced in the UI).
                const string sql = @"
                    INSERT INTO AddressTitle (o_id, at_title, at_active)
                    VALUES (1, @Title, 1);
                    SELECT CAST(SCOPE_IDENTITY() AS INT) AS NewId;";
                var resultTable = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@Title", request.Title.Trim() }
                });
                var newId = ConvertToInt(resultTable.Rows[0]["NewId"]);

                stopwatch.Stop();
                await LogAuditAsync("CreateAddressTitle", new { id = newId, title = request.Title }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return StatusCode(201, new ApiResponse<AddressTitleLookupDto>
                {
                    Success = true,
                    Message = "Address title created successfully",
                    Data = new AddressTitleLookupDto { Id = newId, Title = request.Title.Trim() }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateAddressTitle", ex, request);
                return StatusCode(500, new ApiResponse<AddressTitleLookupDto> { Success = false, Message = "Failed to create address title" });
            }
        }

        [HttpPut("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<AddressTitleLookupDto>>> Update(int id, [FromBody] AddressTitleRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Title))
                    return BadRequest(new ApiResponse<AddressTitleLookupDto> { Success = false, Message = "Title is required" });

                const string sql = "UPDATE AddressTitle SET at_title = @Title WHERE at_id = @Id";
                await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@Id", id },
                    { "@Title", request.Title.Trim() }
                });

                stopwatch.Stop();
                await LogAuditAsync("UpdateAddressTitle", new { id, title = request.Title }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<AddressTitleLookupDto>
                {
                    Success = true,
                    Message = "Address title updated successfully",
                    Data = new AddressTitleLookupDto { Id = id, Title = request.Title.Trim() }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateAddressTitle", ex, new { id, request });
                return StatusCode(500, new ApiResponse<AddressTitleLookupDto> { Success = false, Message = "Failed to update address title" });
            }
        }

        private static int ConvertToInt(object value)
        {
            if (value == null || value == DBNull.Value) return 0;
            return int.TryParse(value.ToString(), out var result) ? result : 0;
        }
    }

    public class AddressTitleRequest
    {
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
    }
}
