using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;
using System.Data;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("EvoApi/contacttitles")]
    [EvoAuthorize]
    public class ContactTitleController : BaseController
    {
        private readonly IDataService _dataService;

        public ContactTitleController(IDataService dataService, IAuditService auditService)
        {
            _dataService = dataService;
            InitializeAuditService(auditService);
        }

        [HttpGet]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<ContactTitleLookupDto>>>> GetAll()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = "SELECT ct_id, ct_title FROM ContactTitle ORDER BY ct_title";
                var result = await _dataService.ExecuteQueryAsync(sql);
                var items = new List<ContactTitleLookupDto>();
                foreach (DataRow row in result.Rows)
                {
                    items.Add(new ContactTitleLookupDto
                    {
                        Id = ConvertToInt(row["ct_id"]),
                        Title = row["ct_title"]?.ToString() ?? string.Empty
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetAllContactTitles", new { count = items.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<ContactTitleLookupDto>>
                {
                    Success = true,
                    Message = "Contact titles retrieved successfully",
                    Data = items,
                    Count = items.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAllContactTitles", ex);
                return StatusCode(500, new ApiResponse<List<ContactTitleLookupDto>> { Success = false, Message = "Failed to retrieve contact titles" });
            }
        }

        [HttpGet("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<ContactTitleLookupDto>>> Get(int id)
        {
            try
            {
                const string sql = "SELECT ct_id, ct_title FROM ContactTitle WHERE ct_id = @Id";
                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object> { { "@Id", id } });
                if (result.Rows.Count == 0)
                    return NotFound(new ApiResponse<ContactTitleLookupDto> { Success = false, Message = "Contact title not found" });

                var row = result.Rows[0];
                return Ok(new ApiResponse<ContactTitleLookupDto>
                {
                    Success = true,
                    Message = "Contact title retrieved successfully",
                    Data = new ContactTitleLookupDto { Id = ConvertToInt(row["ct_id"]), Title = row["ct_title"]?.ToString() ?? string.Empty }
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetContactTitle", ex, new { id });
                return StatusCode(500, new ApiResponse<ContactTitleLookupDto> { Success = false, Message = "Failed to retrieve contact title" });
            }
        }

        [HttpPost]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<ContactTitleLookupDto>>> Create([FromBody] ContactTitleRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Title))
                    return BadRequest(new ApiResponse<ContactTitleLookupDto> { Success = false, Message = "Title is required" });

                // o_id is NOT NULL; legacy app hardcodes 1. ct_active is forced true (not surfaced in the UI).
                const string sql = @"
                    INSERT INTO ContactTitle (o_id, ct_title, ct_active)
                    VALUES (1, @Title, 1);
                    SELECT CAST(SCOPE_IDENTITY() AS INT) AS NewId;";
                var resultTable = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@Title", request.Title.Trim() }
                });
                var newId = ConvertToInt(resultTable.Rows[0]["NewId"]);

                stopwatch.Stop();
                await LogAuditAsync("CreateContactTitle", new { id = newId, title = request.Title }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return StatusCode(201, new ApiResponse<ContactTitleLookupDto>
                {
                    Success = true,
                    Message = "Contact title created successfully",
                    Data = new ContactTitleLookupDto { Id = newId, Title = request.Title.Trim() }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateContactTitle", ex, request);
                return StatusCode(500, new ApiResponse<ContactTitleLookupDto> { Success = false, Message = "Failed to create contact title" });
            }
        }

        [HttpPut("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<ContactTitleLookupDto>>> Update(int id, [FromBody] ContactTitleRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Title))
                    return BadRequest(new ApiResponse<ContactTitleLookupDto> { Success = false, Message = "Title is required" });

                const string sql = "UPDATE ContactTitle SET ct_title = @Title WHERE ct_id = @Id";
                await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@Id", id },
                    { "@Title", request.Title.Trim() }
                });

                stopwatch.Stop();
                await LogAuditAsync("UpdateContactTitle", new { id, title = request.Title }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<ContactTitleLookupDto>
                {
                    Success = true,
                    Message = "Contact title updated successfully",
                    Data = new ContactTitleLookupDto { Id = id, Title = request.Title.Trim() }
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateContactTitle", ex, new { id, request });
                return StatusCode(500, new ApiResponse<ContactTitleLookupDto> { Success = false, Message = "Failed to update contact title" });
            }
        }

        private static int ConvertToInt(object value)
        {
            if (value == null || value == DBNull.Value) return 0;
            return int.TryParse(value.ToString(), out var result) ? result : 0;
        }
    }

    public class ContactTitleRequest
    {
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
    }
}
