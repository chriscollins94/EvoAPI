using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

/// <summary>
/// Ticket and visit layer of the PM workflow (build slice 3): the ServiceType lookup, the PM sub-trades a company can
/// enter a Preventative ticket under, first-time detection, and the PMVisit / PMVisitUnit snapshot created at ticket entry.
/// Tables: sql/migrations/2026-09-24_create_pm_ticket_tables.sql.
/// </summary>
public interface IPmVisitRepository
{
    Task<List<ServiceTypeDto>> GetServiceTypesAsync();
    Task<int?> GetServiceTypeIdAsync(string code);

    /// <summary>PM sub-trades the company has a labor rate for, under parent trades with an active form rule, with the rule's season for each.</summary>
    Task<List<PmTicketTradeDto>> GetTicketTradesAsync(int xcccId);

    /// <summary>Has this location had a PM for the parent trade before (PMVisit rows, or legacy completed tickets under a PM sub-trade)?</summary>
    Task<PmFirstTimeDto> GetFirstTimeAsync(int lId, int parentTId);

    Task<bool> ServiceRequestExistsAsync(int srId);
    Task<PmVisitDto?> GetVisitBySrAsync(int srId);

    /// <summary>Creates the visit snapshot from the rule and request. Returns the pmv_id (the existing one when the SR already has a visit).</summary>
    Task<int> CreateVisitAsync(CreatePmVisitRequest request, FormRuleDto? rule, int? userId);

    // ---- the tech's visit (slice 4a) ----

    /// <summary>Preventative work orders assigned to the user (or every tech's when allTechs), with visit state.</summary>
    Task<List<PmVisitListItemDto>> GetMyVisitsAsync(int userId, bool allTechs);

    /// <summary>Ticket / site / work-order header for one SR, from the caller's point of view (their WO, whether they are checked in).</summary>
    Task<PmVisitHeaderDto?> GetHeaderAsync(int srId, int userId);

    Task<PmVisitUnitOpenDto?> GetUnitAsync(int pmvuId);
    Task<int> AddUnitAsync(int pmvId, int asId, int? ascId, decimal? capacityTons, string? label);
    Task<bool> UpdateUnitAsync(int pmvuId, SavePmVisitUnitRequest request);
    Task<bool> LinkUnitAssetAsync(int pmvuId, int asId, int? ascId, decimal? capacityTons, string? label);

    Task<List<PmVisitAnswerDto>> GetAnswersAsync(int pmvId);
    Task<PmVisitAnswerDto> UpsertAnswerAsync(int pmvId, SavePmVisitAnswerRequest request, string? code, int? userId);

    /// <summary>Marks the visit in progress and stores the weather captured at check-in (null weather keeps the status change only).</summary>
    Task<bool> RecordCheckInAsync(int pmvId, WeatherResultDto? weather);

    // ---- checkout (slice 4b) ----
    Task<List<PmVisitFindingDto>> GetFindingsAsync(int pmvId);
    Task<PmVisitFindingDto?> GetFindingAsync(int pmvfId);
    Task<int> CreateFindingAsync(int pmvId, SavePmVisitFindingRequest request, int? userId);
    Task<bool> UpdateFindingAsync(int pmvfId, SavePmVisitFindingRequest request, int? userId);
    Task<bool> DeleteFindingAsync(int pmvfId);
    Task<List<PmVisitMaterialDto>> GetMaterialsAsync(int srId);
    Task<PmVisitSrBasicsDto?> GetSrBasicsAsync(int srId);
    /// <summary>The trade a proposal ticket goes under: a non-PM sub-trade of the parent the company has a labor rate for (HVAC Repair first), else null.</summary>
    Task<int?> GetProposalTradeAsync(int xcccId, int parentTId);
    Task<bool> SetWorkOrderStatusByCodeAsync(int srId, string ssCode);
    Task<int> LinkFindingsToProposalAsync(int pmvId, int srIdProposal);
    /// <summary>Writes the checkout fields and the final status (Submitted / Incomplete).</summary>
    Task<bool> SubmitAsync(int pmvId, string status, bool? fullScopeCompleted, string? incompleteReason, string? callCenterNotified, string? notificationDetails,
        string? customerRep, bool? customerFormCompleted, string? ivrCheckout, string? managerName, string? managerNotified, int? userId);
}
