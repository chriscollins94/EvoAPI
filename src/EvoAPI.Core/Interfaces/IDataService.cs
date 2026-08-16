using EvoAPI.Shared.DTOs;
using System.Data;

namespace EvoAPI.Core.Interfaces;

public interface IDataService
{
    Task<DataTable> GetWorkOrdersAsync(int numberOfDays);
    Task<DataTable> GetWorkOrdersScheduleAsync(int numberOfDays, int? technicianId = null);
    Task<bool> UpdateWorkOrderEscalatedAsync(UpdateWorkOrderEscalatedRequest request);
    Task<bool> UpdateWorkOrderScheduleLockAsync(UpdateWorkOrderScheduleLockRequest request);
    Task<CreateCustomerInquiryResult> CreateCustomerInquiryAsync(CreateCustomerInquiryRequest request, int userId, string userFullName);
    Task<CustomerInquiryEligibilityDto?> GetCustomerInquiryEligibilityAsync(int srId);
    Task<DataTable> GetAllPrioritiesAsync();
    Task<bool> UpdatePriorityAsync(UpdatePriorityRequest request);
    Task<DataTable> GetAllStatusSecondariesAsync();
    Task<bool> UpdateStatusSecondaryAsync(UpdateStatusSecondaryRequest request);
    Task<DataTable> GetAllCallCentersAsync();
    Task<bool> UpdateCallCenterAsync(UpdateCallCenterRequest request);
    Task<int?> CreateCallCenterAsync(CreateCallCenterRequest request);
    Task<ConfigSettingDto?> GetConfigSettingAsync(string identifier);
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

    // Backlog methods
    Task<DataTable> GetBacklogItemsAsync(bool includeInactive = false);
    Task<DataTable> GetBacklogItemByIdAsync(int backlogItemId);
    Task<int?> CreateBacklogItemAsync(CreateBacklogItemRequest request, int userId, string username);
    Task<bool> UpdateBacklogItemAsync(int backlogItemId, UpdateBacklogItemRequest request, int userId, string username);
    Task<bool> DeleteBacklogItemAsync(int backlogItemId, int userId, string username);
    
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
    Task<DataTable> GetReceiptsDashboardAsync(int? days = null);
    Task<DataTable> GetServiceRequestReportAsync(DateTime startDate, DateTime endDate);
    Task<DataTable> GetTechReceiptsDashboardAsync(int userId);
    Task<DataTable> GetTechDetailDashboardAsync(int? userId = null);
    Task<DataTable> GetTechDetailByTechnicianAsync(int technicianId);
    Task<DataTable> GetTechActivityDashboardAsync(DateTime? startDate = null, DateTime? endDate = null, int? userId = null);
    Task<DataTable> GetServiceRequestNumberChangesAsync();
    Task<DataTable> GetActiveServiceRequestsAsync();
    Task<DataTable> GetPendingTechInfoCurrentAsync();
    Task<DataTable> GetPendingTechInfoHistoricAsync();
    
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
    Task<DataTable> GetAttachmentsByCallCenterAsync(int ccId);
    Task<DataTable> GetAllCallCenterAttachmentsAsync();
    Task<DataTable?> GetAttachmentByIdAsync(int attId);
    Task<bool> UpdateAttachmentDescriptionAsync(int attId, string description);
    Task<bool> DeleteAttachmentAsync(int attId);
    
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
    
    // TradeGeneral Management methods
    Task<DataTable> GetAllTradeGeneralsAsync();
    Task<DataTable> GetUserTradeGeneralsByUserIdAsync(int userId);
    Task<bool> UpdateEmployeeTradeGeneralsAsync(int userId, List<int> tradeGeneralIds);
    
    Task<DataTable> ExecuteQueryAsync(string sql, Dictionary<string, object>? parameters = null);
    Task<int> ExecuteNonQueryAsync(string sql, Dictionary<string, object>? parameters = null);
    
    // Time Tracking Detail methods
    Task<bool> InsertTimeTrackingDetailAsync(int userId, int tttId, int? woId, decimal? latBrowser = null, decimal? lonBrowser = null, string? ttdType = null);
    
    // Company Administration methods
    Task<List<CompanyListDto>> GetCallCenterCompaniesAsync(int callCenterId);
    Task<CompanyDetailDto?> GetCompanyDetailAsync(int xcccId);
    Task<bool> UpdateCompanyGeneralInfoAsync(UpdateCompanyGeneralInfoRequest request);
    Task<int?> CreateCompanyAsync(string companyName);
    Task<List<CompanyWithCallCentersDto>> GetCompaniesWithCallCentersAsync();
    Task<int?> AssignCompanyToCallCenterAsync(int cId, int ccId);
    Task<(bool Success, string? ErrorMessage, int CId, int CcId, string CompanyName, string CallCenterName)> UnassignCompanyFromCallCenterAsync(int xcccId);
    Task<int?> CreateMaterialsMarkupAsync(CreateMaterialsMarkupRequest request);
    Task<UpdateMaterialsMarkupRequest> GetMaterialsMarkupByIdAsync(int mmId);
    Task<(UpdateMaterialsMarkupRequest? MarkupData, string? CompanyName)> GetMaterialsMarkupWithCompanyByIdAsync(int mmId);
    Task<bool> UpdateMaterialsMarkupAsync(UpdateMaterialsMarkupRequest request);
    Task<bool> DeleteMaterialsMarkupAsync(int mmId);
    Task<bool> ResetMaterialsMarkupToDefaultAsync(int xcccId);
    
    // Company Priority methods
    Task<List<CompanyPriorityDto>> GetCompanyPrioritiesAsync(int companyId);
    Task<bool> UpdateCompanyPriorityAsync(UpdateCompanyPriorityRequest request);
    
    // User Attachment Type methods
    Task<DataTable> GetAllUserAttachmentTypesAsync();
    Task<int?> CreateUserAttachmentTypeAsync(CreateUserAttachmentTypeRequest request);
    Task<bool> UpdateUserAttachmentTypeAsync(UpdateUserAttachmentTypeRequest request);

    // User Clothing Size methods
    Task<DataTable> GetAllUserClothingSizesAsync();
    Task<int?> CreateUserClothingSizeAsync(CreateUserClothingSizeRequest request);
    Task<bool> UpdateUserClothingSizeAsync(UpdateUserClothingSizeRequest request);

    // User Pants Waist methods
    Task<DataTable> GetAllUserPantsWaistAsync();
    Task<int?> CreateUserPantsWaistAsync(CreateUserPantsWaistRequest request);
    Task<bool> UpdateUserPantsWaistAsync(UpdateUserPantsWaistRequest request);

    // User Pants Length methods
    Task<DataTable> GetAllUserPantsLengthAsync();
    Task<int?> CreateUserPantsLengthAsync(CreateUserPantsLengthRequest request);
    Task<bool> UpdateUserPantsLengthAsync(UpdateUserPantsLengthRequest request);

    // Service Item Rack methods
    Task<DataTable> GetAllServiceItemRacksAsync();
    Task<int?> CreateServiceItemRackAsync(CreateServiceItemRackRequest request);
    Task<bool> UpdateServiceItemRackAsync(UpdateServiceItemRackRequest request);

    // Service Item Facility methods
    Task<DataTable> GetAllServiceItemFacilitiesAsync();
    Task<int?> CreateServiceItemFacilityAsync(CreateServiceItemFacilityRequest request);
    Task<bool> UpdateServiceItemFacilityAsync(UpdateServiceItemFacilityRequest request);

    // User Relationship methods
    Task<DataTable> GetAllUserRelationshipsAsync();
    Task<int?> CreateUserRelationshipAsync(CreateUserRelationshipRequest request);
    Task<bool> UpdateUserRelationshipAsync(UpdateUserRelationshipRequest request);

    // User Emergency Contact methods
    Task<Dictionary<int, List<UserEmergencyContactDto>>> GetAllEmergencyContactsAsync();
    Task<List<UserEmergencyContactDto>> GetUserEmergencyContactsAsync(int userId);
    Task<int?> CreateUserEmergencyContactAsync(int userId, CreateUserEmergencyContactRequest request);
    Task<bool> UpdateUserEmergencyContactAsync(int userId, UpdateUserEmergencyContactRequest request);
    Task<bool> DeleteUserEmergencyContactAsync(int userId, int xuecId);

    // Employee Attachments methods
    Task<List<EmployeeAttachmentDto>> GetEmployeeAttachmentsAsync(int userId);
    Task<int?> CreateEmployeeAttachmentAsync(int userId, CreateEmployeeAttachmentRequest request, int attachmentId);
    Task<bool> UpdateEmployeeAttachmentAsync(int userId, int xuaId, UpdateEmployeeAttachmentRequest request, int? newAttachmentId = null);
    Task<List<CertificationsLicensingReportDto>> GetCertificationsLicensingReportAsync();
    Task<List<CertificationsLicensingReportDto>> GetTechCertificationsLicensingReportAsync(int userId);

    // Company Trades Management methods
    Task<List<LaborRateDto>> GetCompanyTradesAsync(int xcccId);
    Task<List<CompanyTradeDto>> GetAvailableTradesForCompanyAsync(int xcccId);
    Task<List<CheckListDto>> GetCompanyChecklistsAsync(int xcccId);
    Task<LaborRateDto> CreateCompanyTradeAsync(int xcccId, CreateLaborRateRequest request);
    Task<(LaborRateDto? LaborRate, string? CompanyName, string? TradeName)> GetLaborRateWithCompanyByIdAsync(int lrId);
    Task<List<string>> GetChecklistNamesByIdsAsync(int xcccId, List<int> checklistIds);
    Task<LaborRateDto?> UpdateCompanyTradeAsync(int xcccId, int lrId, UpdateLaborRateRequest request);
    Task<List<int>> GetTradeChecklistsAsync(int lrId);
    Task UpdateTradeChecklistsAsync(int lrId, List<int> checklistIds);
    
    // Company Checklist/Rulebook Management methods
    Task<List<CheckListDto>> GetCompanyChecklistsWithQuestionsAsync(int xcccId);
    Task<List<CheckListTypeDto>> GetCheckListTypesAsync();
    Task<List<CheckListAnswerTypeDto>> GetCheckListAnswerTypesAsync();
    Task<CheckListDto> CreateCheckListAsync(int xcccId, CreateCheckListRequest request);
    Task<CheckListDto?> UpdateCheckListAsync(int clId, UpdateCheckListRequest request);
    Task<CheckListQuestionDto> CreateCheckListQuestionAsync(int clId, CreateCheckListQuestionRequest request);
    Task<CheckListQuestionDto?> UpdateCheckListQuestionAsync(int clqId, UpdateCheckListQuestionRequest request);
    Task CloneCheckListsAsync(int sourceXcccId, int targetXcccId, List<int>? checklistIds = null);

    // Contact management
    Task<List<ContactDto>> GetCompanyContactsAsync(int cId);
    Task<List<ContactTitleDto>> GetContactTitlesAsync();
    Task<(ContactDto? Contact, string? CompanyName)> GetContactWithCompanyByIdAsync(int conId);
    Task<ContactDto> CreateContactAsync(int cId, CreateContactRequest request);
    Task<ContactDto?> UpdateContactAsync(int conId, UpdateContactRequest request);
    
    // Address Management
    Task<List<AddressDto>> GetCompanyAddressesAsync(int cId);
    Task<List<AddressTitleDto>> GetAddressTitlesAsync();
    Task<(AddressDto? Address, string? CompanyName)> GetAddressWithCompanyByIdAsync(int aId);
    Task<AddressDto> CreateAddressAsync(int cId, CreateAddressRequest request);
    Task<AddressDto?> UpdateAddressAsync(int aId, UpdateAddressRequest request);
    
    // Location Management
    Task<List<LocationDto>> GetCompanyLocationsAsync(int cId);
    Task<LocationDto> CreateLocationAsync(int cId, CreateLocationRequest request);
    Task<LocationDto?> UpdateLocationAsync(int lId, UpdateLocationRequest request);

    // Service Request creation (New Service Request flow)
    Task<bool> ServiceRequestNumberExistsAsync(string requestNumber);
    Task<CreateServiceRequestResponse> InsertServiceRequestAsync(CreateServiceRequestRequest request, int createdByUserId);
    Task<AssignServiceRequestTechniciansResponse> AssignServiceRequestTechniciansAsync(AssignServiceRequestTechniciansRequest request, int assignedByUserId);
    Task<List<TechScheduleConflictDto>> GetTechScheduleConflictsAsync(TechScheduleConflictsRequest request);
    Task<List<TechUtilizationDto>> GetTechUtilizationAsync(int tId, string customerZip);
    Task<NteEstimateDto> GetNteEstimateAsync(int xcccId, int tId);

    // Call Center Contact Management
    Task<List<ContactDto>> GetCallCenterContactsAsync(int ccId);
    Task<ContactDto> CreateCallCenterContactAsync(int ccId, CreateContactRequest request);
    Task<ContactDto?> UpdateCallCenterContactAsync(int conId, UpdateContactRequest request);
    Task<bool> DeleteCallCenterContactXrefAsync(int ccId, int conId);

    // Call Center Address Management
    Task<List<AddressDto>> GetCallCenterAddressesAsync(int ccId);
    Task<AddressDto> CreateCallCenterAddressAsync(int ccId, CreateAddressRequest request);
    Task<AddressDto?> UpdateCallCenterAddressAsync(int aId, UpdateAddressRequest request);
    Task<bool> DeleteCallCenterAddressXrefAsync(int ccId, int aId);

    // Time Off Request methods
    Task<DataTable> GetTimeOffRequestTypesAsync();
    Task<DataTable> GetTimeOffRequestTypeDetailsAsync(int tortId);
    Task<DataTable> GetTimeOffBalanceAsync(int userId);
    Task<DataTable> GetTimeOffRequestsAsync(int userId);
    Task<DataTable> GetAllTimeOffRequestsAsync();
    Task<DataTable> GetTimeOffRequestDetailAsync(int torId);
    Task<int?> InsertTimeOffRequestAsync(CreateTimeOffRequestDto request, int statusId);
    Task<bool> InsertTimeOffRequestDetailsAsync(int torId, List<CreateTimeOffRequestDetailDto> details);
    Task<bool> CancelTimeOffRequestAsync(int torId, int userId);
    Task<bool> UpdateTimeOffRequestStatusAsync(int torId, int torsId, string noteReason);
    Task<bool> DeleteTimeOffRequestAsync(int torId);
    Task<bool> IsTimeOffWorkflowCurrentlyZFMReviewAsync(int torId);
    Task<bool> IsTimeOffWorkflowAdminRequiredAsync(int torId);
    Task<int> InsertTimeOffRequestServiceRequestsAsync(int torId, int userId, int tortdId);
    Task<DataTable> GetZonesAsync();
    Task<DataTable> GetZFMByUserAsync(int userId);
    Task<DataTable> GetActiveEmployeesForTimeOffAsync();
    Task<DataTable> GetCalendarEventsAsync();
    Task<string?> GetConfigSettingValueAsync(string csType, string csIdentifier);
    Task<QuickBooksServiceRequestRow?> GetServiceRequestQbInfoByRequestNumberAsync(string requestNumber);
    Task<bool> UpdateServiceRequestSyncTokenAsync(int srId, string syncToken);
    Task<bool> IsUserTechAsync(int userId);
    Task<DataTable> GetUserEmailInfoAsync(int userId);
    Task<bool> IsTimeOffExceedingBalanceAsync(int torId);
    Task<string> GetTimeOffBalanceTypeAsync(int torId);

    // Portal Info Report
    Task<PortalInfoReportDto> GetPortalInfoReportAsync();
}
