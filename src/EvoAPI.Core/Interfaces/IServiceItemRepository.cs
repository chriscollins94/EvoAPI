using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces
{
    public interface IServiceItemRepository
    {
        Task<List<ServiceItemDto>> GetServiceItemsAsync(string? filterText, string? filterStatus, int? filterTradeParent, int? filterServiceItemType, int limit = 10000);
        Task<ServiceItemDto?> GetServiceItemByIdAsync(int serviceItemId);
        Task<int> CreateServiceItemAsync(CreateServiceItemRequest request);
        Task<bool> UpdateServiceItemAsync(UpdateServiceItemRequest request);
        Task<List<ServiceItemUnitDto>> GetServiceItemUnitsAsync();
        Task<List<ServiceItemManufacturerDto>> GetServiceItemManufacturersAsync();
        Task<List<TradeDto>> GetTradesAsync(bool parentOnlyFilter = false);
        Task<List<ServiceItemTypeDto>> GetServiceItemTypesAsync();
        Task<List<ServiceItemUsageDto>> GetServiceItemUsageAsync(int serviceItemId, DateTime? startDate, DateTime? endDate);
    }
}
