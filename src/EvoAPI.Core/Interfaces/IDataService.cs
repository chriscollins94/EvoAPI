using EvoAPI.Shared.DTOs;
using System.Data;

namespace EvoAPI.Core.Interfaces;

public interface IDataService
{
    Task<DataTable> GetWorkOrdersAsync(int numberOfDays);
    Task<DataTable> GetWorkOrdersScheduleAsync(int numberOfDays);
    Task<bool> UpdateWorkOrderEscalatedAsync(UpdateWorkOrderEscalatedRequest request);
    Task<DataTable> GetAllPrioritiesAsync();
    Task<bool> UpdatePriorityAsync(UpdatePriorityRequest request);
    Task<DataTable> GetAllStatusSecondariesAsync();
    Task<bool> UpdateStatusSecondaryAsync(UpdateStatusSecondaryRequest request);
    Task<DataTable> GetAllCallCentersAsync();
    Task<bool> UpdateCallCenterAsync(UpdateCallCenterRequest request);
    Task<int?> CreateCallCenterAsync(CreateCallCenterRequest request);
    Task<DataTable> GetAllAttackPointNotesAsync();
    Task<bool> UpdateAttackPointNoteAsync(UpdateAttackPointNoteRequest request);
    Task<int?> CreateAttackPointNoteAsync(CreateAttackPointNoteRequest request);
    Task<DataTable> GetAllAttackPointActionableDatesAsync();
    Task<bool> UpdateAttackPointActionableDateAsync(UpdateAttackPointActionableDateRequest request);
    Task<int?> CreateAttackPointActionableDateAsync(CreateAttackPointActionableDateRequest request);
    Task<DataTable> GetAllAttackPointStatusAsync();
    Task<bool> UpdateAttackPointStatusAsync(UpdateAttackPointStatusRequest request);
    Task<int?> CreateAttackPointStatusAsync(CreateAttackPointStatusRequest request);
    Task<DataTable> GetAttackPointsAsync(int topCount = 15);
    
    // Status Assignment methods
    Task<DataTable> GetAllZonesAsync();
    Task<DataTable> GetAllUsersAsync();
    Task<DataTable> GetAdminUsersAsync();
    Task<DataTable> GetActiveTechniciansAsync();
    Task<DataTable> GetAdminZoneStatusAssignmentsAsync();
    Task<int?> CreateAdminZoneStatusAssignmentAsync(CreateAdminZoneStatusAssignmentRequest request);
    Task<bool> UpdateAdminZoneStatusAssignmentAsync(UpdateAdminZoneStatusAssignmentRequest request);
    Task<bool> DeleteAdminZoneStatusAssignmentAsync(DeleteAdminZoneStatusAssignmentRequest request);
    
    // Reports methods
    Task<DataTable> GetHighVolumeDashboardAsync();
    Task<DataTable> GetReceiptsDashboardAsync();
    Task<DataTable> GetTechDetailDashboardAsync();
    Task<DataTable> GetTechActivityDashboardAsync();
    
    Task<DataTable> ExecuteQueryAsync(string sql, Dictionary<string, object>? parameters = null);
}
