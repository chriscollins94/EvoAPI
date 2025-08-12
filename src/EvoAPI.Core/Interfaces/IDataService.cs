using EvoAPI.Shared.DTOs;
using System.Data;

namespace EvoAPI.Core.Interfaces;

public interface IDataService
{
    Task<DataTable> GetWorkOrdersAsync(int numberOfDays);
    Task<DataTable> GetWorkOrdersScheduleAsync(int numberOfDays);
    Task<DataTable> GetAllPrioritiesAsync();
    Task<bool> UpdatePriorityAsync(UpdatePriorityRequest request);
    Task<DataTable> GetAllStatusSecondariesAsync();
    Task<bool> UpdateStatusSecondaryAsync(UpdateStatusSecondaryRequest request);
    Task<DataTable> GetAllCallCentersAsync();
    Task<bool> UpdateCallCenterAsync(UpdateCallCenterRequest request);
    Task<int?> CreateCallCenterAsync(CreateCallCenterRequest request);
    Task<DataTable> ExecuteQueryAsync(string sql, Dictionary<string, object>? parameters = null);
}
