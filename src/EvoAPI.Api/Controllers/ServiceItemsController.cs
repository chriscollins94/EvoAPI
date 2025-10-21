using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using EvoAPI.Api.Controllers;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using Microsoft.AspNetCore.Mvc;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("EvoApi/serviceitems")]
    [EvoAuthorize]
    public class ServiceItemsController : BaseController
    {
        private readonly IServiceItemRepository _serviceItemRepository;

        public ServiceItemsController(IServiceItemRepository serviceItemRepository, IAuditService auditService)
        {
            _serviceItemRepository = serviceItemRepository;
            InitializeAuditService(auditService);
        }

        /// <summary>
        /// Get service items with optional filtering
        /// </summary>
        [HttpGet]
        public async Task<ActionResult<ApiResponse<List<ServiceItemDto>>>> GetServiceItems(
            [FromQuery] string? filterText = null,
            [FromQuery] string? filterStatus = null,
            [FromQuery] int? filterTradeParent = null,
            [FromQuery] int? filterServiceItemType = null,
            [FromQuery] int limit = 10000)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var serviceItems = await _serviceItemRepository.GetServiceItemsAsync(filterText, filterStatus, filterTradeParent, filterServiceItemType, limit);
                stopwatch.Stop();

                await LogAuditAsync(
                    "GetServiceItems",
                    new { filterText, filterStatus, filterTradeParent, count = serviceItems.Count },
                    stopwatch.Elapsed.TotalSeconds.ToString("0.00")
                );

                return Ok(new ApiResponse<List<ServiceItemDto>>
                {
                    Success = true,
                    Message = "Service items retrieved successfully",
                    Data = serviceItems,
                    Count = serviceItems.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetServiceItems", ex, new { filterText, filterStatus, filterTradeParent });

                return StatusCode(500, new ApiResponse<List<ServiceItemDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve service items"
                });
            }
        }

        /// <summary>
        /// Get a single service item by ID
        /// </summary>
        [HttpGet("{id}")]
        public async Task<ActionResult<ApiResponse<ServiceItemDto>>> GetServiceItemById(int id)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var serviceItem = await _serviceItemRepository.GetServiceItemByIdAsync(id);
                stopwatch.Stop();

                if (serviceItem == null)
                {
                    return NotFound(new ApiResponse<ServiceItemDto>
                    {
                        Success = false,
                        Message = $"Service item with ID {id} not found"
                    });
                }

                await LogAuditAsync("GetServiceItemById", new { id }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<ServiceItemDto>
                {
                    Success = true,
                    Message = "Service item retrieved successfully",
                    Data = serviceItem,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetServiceItemById", ex, new { id });

                return StatusCode(500, new ApiResponse<ServiceItemDto>
                {
                    Success = false,
                    Message = "Failed to retrieve service item"
                });
            }
        }

        /// <summary>
        /// Create a new service item
        /// </summary>
        [HttpPost]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<int>>> CreateServiceItem([FromBody] CreateServiceItemRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (string.IsNullOrWhiteSpace(request.si_name))
                {
                    return BadRequest(new ApiResponse<int>
                    {
                        Success = false,
                        Message = "Service item name is required"
                    });
                }

                var newId = await _serviceItemRepository.CreateServiceItemAsync(request);
                stopwatch.Stop();

                await LogAuditAsync(
                    "CreateServiceItem",
                    new { newId, request.si_name },
                    stopwatch.Elapsed.TotalSeconds.ToString("0.00")
                );

                return Ok(new ApiResponse<int>
                {
                    Success = true,
                    Message = "Service item created successfully",
                    Data = newId,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CreateServiceItem", ex, request);

                return StatusCode(500, new ApiResponse<int>
                {
                    Success = false,
                    Message = "Failed to create service item"
                });
            }
        }

        /// <summary>
        /// Update an existing service item
        /// </summary>
        [HttpPut("{id}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<bool>>> UpdateServiceItem(int id, [FromBody] UpdateServiceItemRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (id != request.si_id)
                {
                    return BadRequest(new ApiResponse<bool>
                    {
                        Success = false,
                        Message = "ID mismatch"
                    });
                }

                if (string.IsNullOrWhiteSpace(request.si_name))
                {
                    return BadRequest(new ApiResponse<bool>
                    {
                        Success = false,
                        Message = "Service item name is required"
                    });
                }

                var updated = await _serviceItemRepository.UpdateServiceItemAsync(request);
                stopwatch.Stop();

                if (!updated)
                {
                    return NotFound(new ApiResponse<bool>
                    {
                        Success = false,
                        Message = $"Service item with ID {id} not found"
                    });
                }

                await LogAuditAsync(
                    "UpdateServiceItem",
                    new { id, request.si_name },
                    stopwatch.Elapsed.TotalSeconds.ToString("0.00")
                );

                return Ok(new ApiResponse<bool>
                {
                    Success = true,
                    Message = "Service item updated successfully",
                    Data = true,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("UpdateServiceItem", ex, new { id, request });

                return StatusCode(500, new ApiResponse<bool>
                {
                    Success = false,
                    Message = "Failed to update service item"
                });
            }
        }

        /// <summary>
        /// Get all service item units
        /// </summary>
        [HttpGet("units")]
        public async Task<ActionResult<ApiResponse<List<ServiceItemUnitDto>>>> GetServiceItemUnits()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var units = await _serviceItemRepository.GetServiceItemUnitsAsync();
                stopwatch.Stop();

                await LogAuditAsync("GetServiceItemUnits", new { count = units.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<ServiceItemUnitDto>>
                {
                    Success = true,
                    Message = "Service item units retrieved successfully",
                    Data = units,
                    Count = units.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetServiceItemUnits", ex);

                return StatusCode(500, new ApiResponse<List<ServiceItemUnitDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve service item units"
                });
            }
        }

        /// <summary>
        /// Get all service item manufacturers
        /// </summary>
        [HttpGet("manufacturers")]
        public async Task<ActionResult<ApiResponse<List<ServiceItemManufacturerDto>>>> GetServiceItemManufacturers()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var manufacturers = await _serviceItemRepository.GetServiceItemManufacturersAsync();
                stopwatch.Stop();

                await LogAuditAsync("GetServiceItemManufacturers", new { count = manufacturers.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<ServiceItemManufacturerDto>>
                {
                    Success = true,
                    Message = "Service item manufacturers retrieved successfully",
                    Data = manufacturers,
                    Count = manufacturers.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetServiceItemManufacturers", ex);

                return StatusCode(500, new ApiResponse<List<ServiceItemManufacturerDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve service item manufacturers"
                });
            }
        }

        /// <summary>
        /// Get all trades (optionally parent-only trades)
        /// </summary>
        [HttpGet("trades")]
        public async Task<ActionResult<ApiResponse<List<TradeDto>>>> GetTrades([FromQuery] bool parentOnly = false)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var trades = await _serviceItemRepository.GetTradesAsync(parentOnly);
                stopwatch.Stop();

                await LogAuditAsync("GetTrades", new { parentOnly, count = trades.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<TradeDto>>
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
                await LogAuditErrorAsync("GetTrades", ex, new { parentOnly });

                return StatusCode(500, new ApiResponse<List<TradeDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve trades"
                });
            }
        }

        /// <summary>
        /// Get all service item types
        /// </summary>
        [HttpGet("types")]
        public async Task<ActionResult<ApiResponse<List<ServiceItemTypeDto>>>> GetServiceItemTypes()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var types = await _serviceItemRepository.GetServiceItemTypesAsync();
                stopwatch.Stop();

                await LogAuditAsync("GetServiceItemTypes", new { count = types.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<ServiceItemTypeDto>>
                {
                    Success = true,
                    Message = "Service item types retrieved successfully",
                    Data = types,
                    Count = types.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetServiceItemTypes", ex);

                return StatusCode(500, new ApiResponse<List<ServiceItemTypeDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve service item types"
                });
            }
        }

        /// <summary>
        /// Get service request usage for a specific service item
        /// </summary>
        [HttpGet("{id}/usage")]
        public async Task<ActionResult<ApiResponse<List<ServiceItemUsageDto>>>> GetServiceItemUsage(
            int id,
            [FromQuery] DateTime? startDate = null,
            [FromQuery] DateTime? endDate = null)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var usage = await _serviceItemRepository.GetServiceItemUsageAsync(id, startDate, endDate);
                stopwatch.Stop();

                await LogAuditAsync(
                    "GetServiceItemUsage",
                    new { serviceItemId = id, startDate, endDate, count = usage.Count },
                    stopwatch.Elapsed.TotalSeconds.ToString("0.00")
                );

                return Ok(new ApiResponse<List<ServiceItemUsageDto>>
                {
                    Success = true,
                    Message = "Service item usage retrieved successfully",
                    Data = usage,
                    Count = usage.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetServiceItemUsage", ex, new { serviceItemId = id, startDate, endDate });

                return StatusCode(500, new ApiResponse<List<ServiceItemUsageDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve service item usage"
                });
            }
        }
    }
}
