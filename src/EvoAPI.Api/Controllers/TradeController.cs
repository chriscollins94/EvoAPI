using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;
using System.Data;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("EvoApi/trades")]
    [EvoAuthorize]
    public class TradeController : BaseController
    {
        private readonly IDataService _dataService;

        public TradeController(IDataService dataService, IAuditService auditService)
        {
            _dataService = dataService;
            InitializeAuditService(auditService);
        }

        [HttpGet]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<TradeLookupDto>>>> GetAllTrades()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT
                        t.t_id,
                        t.t_trade,
                        t.t_description,
                        t.t_nte,
                        t.t_id_parent,
                        p.t_trade AS parent_trade,
                        t.t_parentonly,
                        t.t_active
                    FROM trade t
                    LEFT JOIN trade p ON t.t_id_parent = p.t_id
                    ORDER BY t.t_trade";

                var result = await _dataService.ExecuteQueryAsync(sql);
                var trades = new List<TradeLookupDto>();

                foreach (DataRow row in result.Rows)
                {
                    trades.Add(MapTrade(row));
                }

                stopwatch.Stop();
                await LogAuditAsync("GetAllTrades", new { count = trades.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<TradeLookupDto>>
                {
                    Success = true,
                    Message = "Trades retrieved successfully",
                    Data = trades,
                    Count = trades.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAllTrades", ex);

                return StatusCode(500, new ApiResponse<List<TradeLookupDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve trades"
                });
            }
        }

        [HttpGet("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<TradeLookupDto>>> GetTrade(int id)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT
                        t.t_id,
                        t.t_trade,
                        t.t_description,
                        t.t_nte,
                        t.t_id_parent,
                        p.t_trade AS parent_trade,
                        t.t_parentonly,
                        t.t_active
                    FROM trade t
                    LEFT JOIN trade p ON t.t_id_parent = p.t_id
                    WHERE t.t_id = @TradeId";

                var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@TradeId", id }
                });

                if (result.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<TradeLookupDto>
                    {
                        Success = false,
                        Message = "Trade not found"
                    });
                }

                var trade = MapTrade(result.Rows[0]);

                stopwatch.Stop();
                await LogAuditAsync("GetTrade", new { tradeId = id }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<TradeLookupDto>
                {
                    Success = true,
                    Message = "Trade retrieved successfully",
                    Data = trade
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetTrade", ex, new { tradeId = id });

                return StatusCode(500, new ApiResponse<TradeLookupDto>
                {
                    Success = false,
                    Message = "Failed to retrieve trade"
                });
            }
        }

        [HttpPost]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<TradeLookupDto>>> CreateTrade([FromBody] CreateTradeRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Trade))
                {
                    return BadRequest(new ApiResponse<TradeLookupDto>
                    {
                        Success = false,
                        Message = "Trade name is required"
                    });
                }

                // o_id is NOT NULL on the trade table; the legacy app hardcodes 1.
                const string sql = @"
                    INSERT INTO trade (o_id, t_trade, t_description, t_nte, t_id_parent, t_parentonly, t_active)
                    VALUES (1, @Trade, @Description, @Nte, @ParentTradeId, @ParentOnly, @Active);
                    SELECT CAST(SCOPE_IDENTITY() AS INT) AS NewId;";

                var resultTable = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@Trade", request.Trade.Trim() },
                    { "@Description", string.IsNullOrWhiteSpace(request.Description) ? (object)DBNull.Value : request.Description!.Trim() },
                    { "@Nte", request.Nte.HasValue ? (object)request.Nte.Value : 0 },
                    { "@ParentTradeId", request.ParentTradeId.HasValue ? (object)request.ParentTradeId.Value : DBNull.Value },
                    { "@ParentOnly", request.ParentOnly },
                    { "@Active", request.Active }
                });
                var newId = ConvertToInt(resultTable.Rows[0]["NewId"]);

                stopwatch.Stop();
                await LogAuditAsync("CreateTrade", new
                {
                    tradeId = newId,
                    trade = request.Trade,
                    description = request.Description,
                    nte = request.Nte,
                    parentTradeId = request.ParentTradeId,
                    parentOnly = request.ParentOnly,
                    active = request.Active
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                var getResult = await GetTrade(newId);
                if (getResult.Result is OkObjectResult okResult && okResult.Value is ApiResponse<TradeLookupDto> response)
                {
                    return CreatedAtAction(nameof(GetTrade), new { id = newId }, response);
                }

                return StatusCode(201, new ApiResponse<TradeLookupDto>
                {
                    Success = true,
                    Message = "Trade created successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateTrade", ex, request);

                return StatusCode(500, new ApiResponse<TradeLookupDto>
                {
                    Success = false,
                    Message = "Failed to create trade"
                });
            }
        }

        [HttpPut("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<TradeLookupDto>>> UpdateTrade(int id, [FromBody] UpdateTradeRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.Trade))
                {
                    return BadRequest(new ApiResponse<TradeLookupDto>
                    {
                        Success = false,
                        Message = "Trade name is required"
                    });
                }

                const string sql = @"
                    UPDATE trade
                    SET t_trade = @Trade,
                        t_description = @Description,
                        t_nte = @Nte,
                        t_id_parent = @ParentTradeId,
                        t_parentonly = @ParentOnly,
                        t_active = @Active
                    WHERE t_id = @TradeId";

                await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@TradeId", id },
                    { "@Trade", request.Trade.Trim() },
                    { "@Description", string.IsNullOrWhiteSpace(request.Description) ? (object)DBNull.Value : request.Description!.Trim() },
                    { "@Nte", request.Nte.HasValue ? (object)request.Nte.Value : 0 },
                    { "@ParentTradeId", request.ParentTradeId.HasValue ? (object)request.ParentTradeId.Value : DBNull.Value },
                    { "@ParentOnly", request.ParentOnly },
                    { "@Active", request.Active }
                });

                stopwatch.Stop();
                await LogAuditAsync("UpdateTrade", new
                {
                    tradeId = id,
                    trade = request.Trade,
                    description = request.Description,
                    nte = request.Nte,
                    parentTradeId = request.ParentTradeId,
                    parentOnly = request.ParentOnly,
                    active = request.Active
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                var getResult = await GetTrade(id);
                if (getResult.Result is OkObjectResult okResult && okResult.Value is ApiResponse<TradeLookupDto> response)
                {
                    return Ok(response);
                }

                return Ok(new ApiResponse<TradeLookupDto>
                {
                    Success = true,
                    Message = "Trade updated successfully"
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateTrade", ex, new { tradeId = id, request });

                return StatusCode(500, new ApiResponse<TradeLookupDto>
                {
                    Success = false,
                    Message = "Failed to update trade"
                });
            }
        }

        private static TradeLookupDto MapTrade(DataRow row)
        {
            return new TradeLookupDto
            {
                TradeId = ConvertToInt(row["t_id"]),
                Trade = row["t_trade"]?.ToString() ?? string.Empty,
                Description = row["t_description"]?.ToString() ?? string.Empty,
                Nte = ConvertToNullableInt(row["t_nte"]),
                ParentTradeId = ConvertToNullableInt(row["t_id_parent"]),
                ParentTradeName = row["parent_trade"]?.ToString(),
                ParentOnly = ConvertToBool(row["t_parentonly"]),
                Active = ConvertToBool(row["t_active"])
            };
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

            if (int.TryParse(value.ToString(), out var intResult))
                return intResult != 0;

            return false;
        }
    }

    public class CreateTradeRequest
    {
        public string Trade { get; set; } = string.Empty;
        public string? Description { get; set; }
        public int? Nte { get; set; }
        public int? ParentTradeId { get; set; }
        public bool ParentOnly { get; set; }
        public bool Active { get; set; } = true;
    }

    public class UpdateTradeRequest
    {
        public int Id { get; set; }
        public string Trade { get; set; } = string.Empty;
        public string? Description { get; set; }
        public int? Nte { get; set; }
        public int? ParentTradeId { get; set; }
        public bool ParentOnly { get; set; }
        public bool Active { get; set; }
    }
}
