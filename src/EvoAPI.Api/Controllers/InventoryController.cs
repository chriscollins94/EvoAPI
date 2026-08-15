using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using Microsoft.AspNetCore.Mvc;

namespace EvoAPI.Api.Controllers
{
    /// <summary>
    /// Service item inventory (stock on hand). Replaces the legacy AngularJS
    /// default.html#/inventorymanagement page and its EvoWS endpoints.
    ///
    /// Reads need any logged-in user; writes need the "Admin - Service Items"
    /// function, the same permission the legacy page checked client-side.
    /// </summary>
    [ApiController]
    [Route("EvoApi/inventory")]
    [EvoAuthorize]
    public class InventoryController : BaseController
    {
        private readonly IServiceItemInventoryRepository _inventoryRepository;

        public InventoryController(IServiceItemInventoryRepository inventoryRepository, IAuditService auditService)
        {
            _inventoryRepository = inventoryRepository;
            InitializeAuditService(auditService);
        }

        /// <summary>
        /// Stock rows plus totals for the whole filtered set (not just the returned page).
        /// </summary>
        [HttpGet]
        public async Task<ActionResult<ApiResponse<ServiceItemInventoryListDto>>> GetInventory(
            [FromQuery] string? filterText = null,
            [FromQuery] int? facilityId = null,
            [FromQuery] int? rackId = null,
            [FromQuery] bool lowStockOnly = false,
            [FromQuery] int lowStockThreshold = 0,
            [FromQuery] string? sortField = null,
            [FromQuery] string? sortDirection = null,
            [FromQuery] int limit = 500)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var result = await _inventoryRepository.GetInventoryAsync(
                    filterText, facilityId, rackId, lowStockOnly, lowStockThreshold, sortField, sortDirection, limit);
                stopwatch.Stop();

                await LogAuditAsync(
                    "GetInventory",
                    new { filterText, facilityId, rackId, lowStockOnly, count = result.Items.Count, total = result.Summary.RowCount },
                    stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<ServiceItemInventoryListDto>
                {
                    Success = true,
                    Message = "Inventory retrieved successfully",
                    Data = result,
                    Count = result.Items.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetInventory", ex, new { filterText, facilityId, rackId });

                return StatusCode(500, new ApiResponse<ServiceItemInventoryListDto>
                {
                    Success = false,
                    Message = "Failed to retrieve inventory"
                });
            }
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<ApiResponse<ServiceItemInventoryDto>>> GetById(int id)
        {
            try
            {
                var row = await _inventoryRepository.GetByIdAsync(id);
                if (row == null)
                {
                    return NotFound(new ApiResponse<ServiceItemInventoryDto>
                    {
                        Success = false,
                        Message = $"Inventory record {id} not found"
                    });
                }

                return Ok(new ApiResponse<ServiceItemInventoryDto>
                {
                    Success = true,
                    Message = "Inventory record retrieved successfully",
                    Data = row,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetInventoryById", ex, new { id });
                return StatusCode(500, new ApiResponse<ServiceItemInventoryDto>
                {
                    Success = false,
                    Message = "Failed to retrieve inventory record"
                });
            }
        }

        /// <summary>Movement history, newest first. Filter by inventory row or by service item.</summary>
        [HttpGet("transactions")]
        public async Task<ActionResult<ApiResponse<List<ServiceItemInventoryTransactionDto>>>> GetTransactions(
            [FromQuery] int? inventoryId = null,
            [FromQuery] int? serviceItemId = null,
            [FromQuery] int limit = 200)
        {
            try
            {
                var history = await _inventoryRepository.GetTransactionsAsync(inventoryId, serviceItemId, limit);

                return Ok(new ApiResponse<List<ServiceItemInventoryTransactionDto>>
                {
                    Success = true,
                    Message = "Inventory history retrieved successfully",
                    Data = history,
                    Count = history.Count
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetInventoryTransactions", ex, new { inventoryId, serviceItemId });
                return StatusCode(500, new ApiResponse<List<ServiceItemInventoryTransactionDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve inventory history"
                });
            }
        }

        [HttpPost]
        [ServiceItemsOnly]
        public async Task<ActionResult<ApiResponse<int>>> Create([FromBody] CreateServiceItemInventoryRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (request.si_id <= 0)
                    return BadRequest(new ApiResponse<int> { Success = false, Message = "A service item is required" });
                if (request.sif_id <= 0)
                    return BadRequest(new ApiResponse<int> { Success = false, Message = "Facility is required" });
                if (request.sir_id <= 0)
                    return BadRequest(new ApiResponse<int> { Success = false, Message = "Rack is required" });

                var (newId, error) = await _inventoryRepository.CreateAsync(request, UserId, Username);
                stopwatch.Stop();

                if (error != null)
                    return BadRequest(new ApiResponse<int> { Success = false, Message = error });

                await LogAuditAsync("CreateInventory", new { request, newId }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<int>
                {
                    Success = true,
                    Message = "Inventory record created successfully",
                    Data = newId ?? 0,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateInventory", ex, request);
                return StatusCode(500, new ApiResponse<int> { Success = false, Message = "Failed to create inventory record" });
            }
        }

        [HttpPut("{id}")]
        [ServiceItemsOnly]
        public async Task<ActionResult<ApiResponse<bool>>> Update(int id, [FromBody] UpdateServiceItemInventoryRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                request.sii_id = id;

                if (request.sif_id <= 0)
                    return BadRequest(new ApiResponse<bool> { Success = false, Message = "Facility is required" });
                if (request.sir_id <= 0)
                    return BadRequest(new ApiResponse<bool> { Success = false, Message = "Rack is required" });

                var (success, error) = await _inventoryRepository.UpdateAsync(request, UserId, Username);
                stopwatch.Stop();

                if (!success)
                {
                    var message = error ?? "Failed to update inventory record";
                    return message.Contains("not found", StringComparison.OrdinalIgnoreCase)
                        ? NotFound(new ApiResponse<bool> { Success = false, Message = message })
                        : BadRequest(new ApiResponse<bool> { Success = false, Message = message });
                }

                await LogAuditAsync("UpdateInventory", request, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<bool>
                {
                    Success = true,
                    Message = "Inventory record updated successfully",
                    Data = true,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateInventory", ex, request);
                return StatusCode(500, new ApiResponse<bool> { Success = false, Message = "Failed to update inventory record" });
            }
        }

        [HttpDelete("{id}")]
        [ServiceItemsOnly]
        public async Task<ActionResult<ApiResponse<bool>>> Delete(int id, [FromQuery] string? reason = null)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var deleted = await _inventoryRepository.DeleteAsync(id, UserId, Username, reason);
                stopwatch.Stop();

                if (!deleted)
                    return NotFound(new ApiResponse<bool> { Success = false, Message = $"Inventory record {id} not found" });

                await LogAuditAsync("DeleteInventory", new { id, reason }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<bool>
                {
                    Success = true,
                    Message = "Inventory record deleted successfully",
                    Data = true,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("DeleteInventory", ex, new { id });
                return StatusCode(500, new ApiResponse<bool> { Success = false, Message = "Failed to delete inventory record" });
            }
        }

        /// <summary>
        /// Move stock from available to allocated when a part leaves the facility.
        /// Port of the EvoWS DecrementInventory, with two fixes: it draws the requested
        /// total from specific rows instead of applying the full quantity to every row
        /// sharing the si_id, and it reports any shortfall instead of returning a bare OK.
        /// </summary>
        [HttpPost("decrement")]
        [ServiceItemsOnly]
        public async Task<ActionResult<ApiResponse<InventoryDecrementResultDto>>> Decrement([FromBody] InventoryDecrementRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var (result, error) = await _inventoryRepository.DecrementAsync(request, UserId, Username);
                stopwatch.Stop();

                if (error != null)
                {
                    return error.Contains("No inventory record", StringComparison.OrdinalIgnoreCase)
                        ? NotFound(new ApiResponse<InventoryDecrementResultDto> { Success = false, Message = error })
                        : BadRequest(new ApiResponse<InventoryDecrementResultDto> { Success = false, Message = error });
                }

                await LogAuditAsync("DecrementInventory",
                    new { request, result!.Decremented, result.Shortfall },
                    stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                // A partial fill is still a 200 - the work happened. Success stays true
                // and the caller decides what to do about Shortfall.
                return Ok(new ApiResponse<InventoryDecrementResultDto>
                {
                    Success = true,
                    Message = result!.FullyFulfilled
                        ? "Inventory decremented successfully"
                        : $"Only {result.Decremented} of {result.Requested} units were available; {result.Shortfall} short",
                    Data = result,
                    Count = result.AffectedRows.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("DecrementInventory", ex, request);
                return StatusCode(500, new ApiResponse<InventoryDecrementResultDto> { Success = false, Message = "Failed to decrement inventory" });
            }
        }
    }
}
