using EvoAPI.Shared.DTOs;
using System.Data;

namespace EvoAPI.Core.Interfaces;

public interface IDataService
{
    Task<DataTable> GetWorkOrdersAsync(int numberOfDays);
    Task<DataTable> GetWorkOrdersScheduleAsync(int numberOfDays, int? technicianId = null);
    Task<bool> UpdateWorkOrderEscalatedAsync(UpdateWorkOrderEscalatedRequest request);
    Task<bool> UpdateWorkOrderScheduleLockAsync(UpdateWorkOrderScheduleLockRequest request);
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
    
    // User Management methods
    Task<DataTable> GetAllUsersForManagementAsync();
    Task<DataTable> GetUserByIdAsync(int userId);
    Task<int?> CreateUserAsync(CreateUserRequest request);
    Task<bool> UpdateUserAsync(UpdateUserRequest request);
    Task<bool> UpdateUserDashboardNoteAsync(int userId, string? dashboardNote);
    
    // Employee Management methods (optimized)
    Task<DataTable> GetAllEmployeesWithRolesAsync();
    Task<DataTable> GetAllEmployeesWithRolesAndTradeGeneralsAsync();
    
    // Reports methods
    Task<DataTable> GetHighVolumeDashboardAsync();
    Task<DataTable> GetReceiptsDashboardAsync();
    Task<DataTable> GetTechDetailDashboardAsync();
    Task<DataTable> GetTechDetailByTechnicianAsync(int technicianId);
    Task<DataTable> GetTechActivityDashboardAsync(DateTime? startDate = null, DateTime? endDate = null);
    Task<DataTable> GetServiceRequestNumberChangesAsync();
    Task<DataTable> GetActiveServiceRequestsAsync();
    
    // Missing Receipts methods
    Task<List<MissingReceiptDashboardDto>> GetMissingReceiptsAsync();
    Task<List<MissingReceiptDashboardDto>> GetMissingReceiptsByUserAsync(int userId);
    Task<int> UploadMissingReceiptsAsync(List<MissingReceiptUploadDto> receipts);
    
    // Vehicle Maintenance methods
    Task<List<VehicleMaintenanceDto>> GetVehicleMaintenanceRecordsAsync();
    Task<VehicleMaintenanceDto?> GetVehicleMaintenanceByEmployeeNumberAsync(string employeeNumber);
    Task<int> UploadVehicleMaintenanceRecordsAsync(List<VehicleMaintenanceUploadDto> records);
    
    // Work Order Scheduling Conflicts methods
    Task<DataTable> GetWorkOrderSchedulingConflictsAsync();
    Task<DataTable> GetWorkOrderSchedulingConflictsSummaryAsync();
    
    // Timecard Discrepancies methods
    Task<DataTable> GetTimecardDiscrepanciesAsync(DateTime startDate, DateTime endDate);
    
    // Arriving Late Report methods
    Task<DataTable> GetArrivingLateReportAsync();
    
    // Attachments methods
    Task<DataTable> GetAttachmentsByServiceRequestAsync(int srId);
    
    // Pending Tech Info methods
    Task<DataTable> GetPendingTechInfoAsync(int userId);
    
    // Mapping/Distance Cache methods
    Task<MapDistanceDto?> GetCachedDistanceAsync(string fromAddress, string toAddress);
    Task<int> SaveCachedDistanceAsync(SaveMapDistanceRequest request);
    Task<int> CleanupCachedDistanceAsync(int olderThanDays);
    
    // Driving Scorecard methods
    Task<DrivingScorecard> GetDrivingScorecardAsync(int userId);
    Task<List<DrivingScorecardWithTechnicianInfo>> GetAllDrivingScorecardsAsync();
    
    // Fleetmatics User methods
    Task<List<UserFleetmaticsDto>> GetUsersForFleetmaticsSyncAsync();
    Task<bool> UpdateUserVehicleNumberAsync(int userId, string vehicleNumber);
    
    // Employee Management methods
    Task<DataTable> GetAllEmployeesAsync();
    Task<DataTable> GetEmployeeByIdAsync(int userId);
    Task<DataTable> GetAllRolesAsync();
    Task<DataTable> GetUserRolesByUserIdAsync(int userId);
    Task<DataTable> GetAddressByIdAsync(int addressId);
    Task<int?> CreateEmployeeAsync(CreateEmployeeRequest request);
    Task<bool> UpdateEmployeeAsync(UpdateEmployeeRequest request);
    Task<bool> UpdateEmployeeRolesAsync(int userId, List<int> roleIds);
    Task<int?> CreateAddressAsync(CreateAddressRequest request);
    Task<bool> UpdateAddressAsync(UpdateAddressRequest request);
    
    // TradeGeneral Management methods
    Task<DataTable> GetAllTradeGeneralsAsync();
    Task<DataTable> GetUserTradeGeneralsByUserIdAsync(int userId);
    Task<bool> UpdateEmployeeTradeGeneralsAsync(int userId, List<int> tradeGeneralIds);
    
    Task<DataTable> ExecuteQueryAsync(string sql, Dictionary<string, object>? parameters = null);
    
    // Time Tracking Detail methods
    Task<bool> InsertTimeTrackingDetailAsync(int userId, int tttId, int? woId, decimal? latBrowser = null, decimal? lonBrowser = null, string? ttdType = null);
    
    // Company Administration methods
    Task<List<CompanyListDto>> GetCallCenterCompaniesAsync(int callCenterId);
    Task<CompanyDetailDto?> GetCompanyDetailAsync(int xcccId);
    Task<bool> UpdateCompanyGeneralInfoAsync(UpdateCompanyGeneralInfoRequest request);
    Task<int?> CreateMaterialsMarkupAsync(CreateMaterialsMarkupRequest request);
    Task<bool> UpdateMaterialsMarkupAsync(UpdateMaterialsMarkupRequest request);
    Task<bool> DeleteMaterialsMarkupAsync(int mmId);
    Task<bool> ResetMaterialsMarkupToDefaultAsync(int xcccId);
    
    // User Attachment Type methods
    Task<DataTable> GetAllUserAttachmentTypesAsync();
    Task<int?> CreateUserAttachmentTypeAsync(CreateUserAttachmentTypeRequest request);
    Task<bool> UpdateUserAttachmentTypeAsync(UpdateUserAttachmentTypeRequest request);
}
