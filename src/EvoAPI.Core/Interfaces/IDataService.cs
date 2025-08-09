using EvoAPI.Shared.DTOs;
using System.Data;

namespace EvoAPI.Core.Interfaces;

public interface IDataService
{
    Task<DataTable> GetWorkOrdersAsync(int numberOfDays);
    Task<DataTable> GetWorkOrdersScheduleAsync(int numberOfDays);
    Task<DataTable> ExecuteQueryAsync(string sql, Dictionary<string, object>? parameters = null);
}
