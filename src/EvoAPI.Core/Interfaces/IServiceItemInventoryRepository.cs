using System.Collections.Generic;
using System.Threading.Tasks;
using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces
{
    public interface IServiceItemInventoryRepository
    {
        Task<ServiceItemInventoryListDto> GetInventoryAsync(
            string? filterText,
            int? facilityId,
            int? rackId,
            bool lowStockOnly,
            int lowStockThreshold,
            string? sortField,
            string? sortDirection,
            int limit);

        Task<ServiceItemInventoryDto?> GetByIdAsync(int inventoryId);

        /// <summary>Returns the new sii_id, or an error message when the location is already taken.</summary>
        Task<(int? Id, string? Error)> CreateAsync(CreateServiceItemInventoryRequest request, int userId, string username);

        Task<(bool Success, string? Error)> UpdateAsync(UpdateServiceItemInventoryRequest request, int userId, string username);

        Task<bool> DeleteAsync(int inventoryId, int userId, string username, string? reason);

        Task<(InventoryDecrementResultDto? Result, string? Error)> DecrementAsync(InventoryDecrementRequest request, int userId, string username);

        Task<List<ServiceItemInventoryTransactionDto>> GetTransactionsAsync(int? inventoryId, int? serviceItemId, int limit);
    }
}
