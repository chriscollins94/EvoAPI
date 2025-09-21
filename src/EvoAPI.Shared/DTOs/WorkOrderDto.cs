namespace EvoAPI.Shared.DTOs;

public class WorkOrderDto
{
    public int sr_id { get; set; }
    public int? wo_id { get; set; }
    public string CreateDate { get; set; } = string.Empty;
    public string CallCenter { get; set; } = string.Empty;
    public string Company { get; set; } = string.Empty;
    public string Trade { get; set; } = string.Empty;
    public string StartDate { get; set; } = string.Empty;
    public string EndDate { get; set; } = string.Empty;
    public string RequestNumber { get; set; } = string.Empty;
    public decimal? TotalDue { get; set; }
    public string Priority { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string SecondaryStatus { get; set; } = string.Empty;
    public string StatusColor { get; set; } = string.Empty;
    public string AssignedFirstName { get; set; } = string.Empty;
    public string AssignedLastName { get; set; } = string.Empty;
    public string Location { get; set; } = string.Empty;
    public string Address { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string State { get; set; } = string.Empty;
    public string Zip { get; set; } = string.Empty;
    public string Zone { get; set; } = string.Empty;
    public string CreatedBy { get; set; } = string.Empty;
    public DateTime? Escalated { get; set; }
    public bool ScheduleLock { get; set; }
    public string ActionableNote { get; set; } = string.Empty;
   
}

public class ActiveServiceRequestDto
{
    public int SrId { get; set; }
    public string RequestNumber { get; set; } = string.Empty;
    public DateTime InsertDateTime { get; set; }
    public string Status { get; set; } = string.Empty;
    public string TechFirstName { get; set; } = string.Empty;
    public string TechLastName { get; set; } = string.Empty;
    public bool IsActive { get; set; }
}

public class WorkOrderRequest
{
    public int NumberOfDays { get; set; } = 30;
}

public class UpdateWorkOrderEscalatedRequest
{
    public int ServiceRequestId { get; set; }
    public bool IsEscalated { get; set; }
}

public class UpdateWorkOrderScheduleLockRequest
{
    public int ServiceRequestId { get; set; }
    public bool IsScheduleLocked { get; set; }
}

public class VehicleMaintenanceDto
{
    public int vd_id { get; set; }
    public DateTime vd_insertdatetime { get; set; }
    public DateTime? vd_modifieddatetime { get; set; }
    public DateTime? vd_dateupload { get; set; }
    public string? vd_maintproduct { get; set; }
    public int? vd_monthsoncurrentservice { get; set; }
    public string? vd_custname { get; set; }
    public string? vd_driver { get; set; }
    public string? vd_vin { get; set; }
    public string? vd_maintcostcode { get; set; }
    public string? vd_customervehicleid { get; set; }
    public int? vd_year { get; set; }
    public string? vd_make { get; set; }
    public string? vd_model { get; set; }
    public string? vd_series { get; set; }
    public string? vd_vehicle { get; set; }
    public string? vd_openrecall { get; set; }
    public DateTime? vd_oilchangedate { get; set; }
    public int? vd_oilchangemileage { get; set; }
    public int? vd_estmileagesinceoilchange { get; set; }
    public int? vd_contractedbrakesets { get; set; }
    public int? vd_availablebrakesets { get; set; }
    public DateTime? vd_brakereplacementdate { get; set; }
    public string? vd_frontrearboth { get; set; }
    public int? vd_brakereplacementmileage { get; set; }
    public int? vd_estmileagesincebrakereplacement { get; set; }
    public int? vd_contractedtires { get; set; }
    public int? vd_availabletires { get; set; }
    public DateTime? vd_tirereplacementdate { get; set; }
    public int? vd_tirereplacementmileage { get; set; }
    public int? vd_estmileagesincetirereplacement { get; set; }
    public int? vd_estimatedcurrentmileage { get; set; }
    public string? u_employeenumber { get; set; }

    // Frontend-friendly property names
    public string? vdMaintProduct => vd_maintproduct;
    public int? vdMonthsOnCurrentService => vd_monthsoncurrentservice;
    public string? vdCustName => vd_custname;
    public string? vdDriver => vd_driver;
    public string? vdVin => vd_vin;
    public string? vdMaintCostCode => vd_maintcostcode;
    public string? vdCustomerVehicleId => vd_customervehicleid;
    public int? vdYear => vd_year;
    public string? vdMake => vd_make;
    public string? vdModel => vd_model;
    public string? vdSeries => vd_series;
    public string? vdVehicle => vd_vehicle;
    public string? vdOpenRecall => vd_openrecall;
    public DateTime? vdOilChangeDate => vd_oilchangedate;
    public int? vdOilChangeMileage => vd_oilchangemileage;
    public int? vdEstMileageSinceOilChange => vd_estmileagesinceoilchange;
    public int? vdContractedBrakeSets => vd_contractedbrakesets;
    public int? vdAvailableBrakeSets => vd_availablebrakesets;
    public DateTime? vdBrakeReplacementDate => vd_brakereplacementdate;
    public string? vdFrontRearBoth => vd_frontrearboth;
    public int? vdBrakeReplacementMileage => vd_brakereplacementmileage;
    public int? vdEstMileageSinceBrakeReplacement => vd_estmileagesincebrakereplacement;
    public int? vdContractedTires => vd_contractedtires;
    public int? vdAvailableTires => vd_availabletires;
    public DateTime? vdTireReplacementDate => vd_tirereplacementdate;
    public int? vdTireReplacementMileage => vd_tirereplacementmileage;
    public int? vdEstMileageSinceTireReplacement => vd_estmileagesincetirereplacement;
    public int? vdEstimatedCurrentMileage => vd_estimatedcurrentmileage;
    public string? uEmployeeNumber => u_employeenumber;
    public DateTime? vdDateUpload => vd_dateupload;
    public int vdId => vd_id;
}

public class VehicleMaintenanceUploadDto
{
    public string? vdMaintProduct { get; set; }
    public int? vdMonthsOnCurrentService { get; set; }
    public string? vdCustName { get; set; }
    public string? vdDriver { get; set; }
    public string? vdVin { get; set; }
    public string? vdMaintCostCode { get; set; }
    public string? vdCustomerVehicleId { get; set; }
    public int? vdYear { get; set; }
    public string? vdMake { get; set; }
    public string? vdModel { get; set; }
    public string? vdSeries { get; set; }
    public string? vdVehicle { get; set; }
    public string? vdOpenRecall { get; set; }
    public string? vdOilChangeDate { get; set; } // Will be parsed to DateTime in backend
    public int? vdOilChangeMileage { get; set; }
    public int? vdEstMileageSinceOilChange { get; set; }
    public int? vdContractedBrakeSets { get; set; }
    public int? vdAvailableBrakeSets { get; set; }
    public string? vdBrakeReplacementDate { get; set; } // Will be parsed to DateTime in backend
    public string? vdFrontRearBoth { get; set; }
    public int? vdBrakeReplacementMileage { get; set; }
    public int? vdEstMileageSinceBrakeReplacement { get; set; }
    public int? vdContractedTires { get; set; }
    public int? vdAvailableTires { get; set; }
    public string? vdTireReplacementDate { get; set; } // Will be parsed to DateTime in backend
    public int? vdTireReplacementMileage { get; set; }
    public int? vdEstMileageSinceTireReplacement { get; set; }
    public int? vdEstimatedCurrentMileage { get; set; }
    public string? uEmployeeNumber { get; set; }
}

public class ApiResponse<T>
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public T? Data { get; set; }
    public int Count { get; set; }
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}
