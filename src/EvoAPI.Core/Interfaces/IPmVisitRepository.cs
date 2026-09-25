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
}
