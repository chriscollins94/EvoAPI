using System.Diagnostics;
using EvoAPI.Core.Interfaces;
using EvoAPI.Core.Services;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using Microsoft.AspNetCore.Mvc;

namespace EvoAPI.Api.Controllers
{
    /// <summary>
    /// Ticket entry for the Preventative Maintenance workflow (PM build slice 3, plan section 6.2 T1-T6).
    ///
    /// The New Service Request wizard runs as a logged-in office user, not an admin, so this controller re-exposes the
    /// reads it needs from the admin-only Form and Asset controllers (the customer's PM rule, the location's PM profile
    /// and units) at [EvoAuthorize] level, the same way the labor-rates mirror under servicerequests/ does. It adds the
    /// pieces that are new to ticket entry: the Service Type lookup, the PM sub-trades a company can enter a ticket
    /// under, first-time detection, the price calculator, and the PMVisit snapshot created right after the SR.
    ///
    /// Routes live under EvoApi/pm/tickets. The UI hides all of this behind the PreventativeMaintenance flag.
    /// </summary>
    [ApiController]
    [Route("EvoApi/pm/tickets")]
    [EvoAuthorize]
    public class PmTicketController : BaseController
    {
        private readonly IPmVisitRepository _visits;
        private readonly IFormRepository _forms;
        private readonly IAssetRepository _assets;
        private readonly IAuditCriticalService _auditCriticalService;

        public PmTicketController(IPmVisitRepository visits, IFormRepository forms, IAssetRepository assets,
            IAuditService auditService, IAuditCriticalService auditCriticalService)
        {
            _visits = visits;
            _forms = forms;
            _assets = assets;
            _auditCriticalService = auditCriticalService;
            InitializeAuditService(auditService);
        }

        private void SetAuditCriticalUserContext()
        {
            _auditCriticalService.Username = Username;
            _auditCriticalService.UserFullName = UserFullName;
            _auditCriticalService.IPAddress = ClientIPAddress;
            _auditCriticalService.UserAgent = UserAgent;
        }

        private static ActionResult<ApiResponse<T>> Fail<T>(int status, string message) =>
            new ObjectResult(new ApiResponse<T> { Success = false, Message = message }) { StatusCode = status };

        private static ApiResponse<T> Ok<T>(T data, string message, int? count = null) =>
            new() { Success = true, Message = message, Data = data, Count = count ?? (data is System.Collections.ICollection c ? c.Count : 1) };

        #region lookups

        [HttpGet("service-types")]
        public async Task<ActionResult<ApiResponse<List<ServiceTypeDto>>>> GetServiceTypes()
        {
            try
            {
                var rows = await _visits.GetServiceTypesAsync();
                return Ok(Ok(rows, $"Retrieved {rows.Count} service types"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmTicketGetServiceTypes", ex, null);
                return Fail<List<ServiceTypeDto>>(500, "Failed to retrieve service types");
            }
        }

        /// <summary>PM sub-trades this company can enter a Preventative ticket under (has a labor rate, parent trade has an active rule).</summary>
        [HttpGet("companies/{xcccId:int}/trades")]
        public async Task<ActionResult<ApiResponse<List<PmTicketTradeDto>>>> GetTicketTrades(int xcccId)
        {
            try
            {
                var rows = await _visits.GetTicketTradesAsync(xcccId);
                return Ok(Ok(rows, $"Retrieved {rows.Count} PM trades"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmTicketGetTrades", ex, new { xcccId });
                return Fail<List<PmTicketTradeDto>>(500, "Failed to retrieve the company's PM trades");
            }
        }

        /// <summary>The customer's PM rule for a parent trade: billing, tiers, seasons, parameters. Question overrides are dropped (not needed here).</summary>
        [HttpGet("companies/{xcccId:int}/rules/{tId:int}")]
        public async Task<ActionResult<ApiResponse<FormRuleDto>>> GetRule(int xcccId, int tId)
        {
            try
            {
                var rule = await _forms.GetRuleAsync(xcccId, tId);
                if (rule == null) return Fail<FormRuleDto>(404, "No form rule for this company and trade");
                rule.QuestionOverrides = new List<FormRuleQuestionDto>();
                return Ok(Ok(rule, "Rule retrieved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmTicketGetRule", ex, new { xcccId, tId });
                return Fail<FormRuleDto>(500, "Failed to retrieve the PM rule");
            }
        }

        [HttpGet("locations/{lId:int}/profile/{tId:int}")]
        public async Task<ActionResult<ApiResponse<LocationTradeProfileDto>>> GetProfile(int lId, int tId)
        {
            try
            {
                var profile = await _assets.GetProfileAsync(lId, tId);
                if (profile == null) return Fail<LocationTradeProfileDto>(404, "No PM profile for this location and trade yet");
                return Ok(Ok(profile, "PM profile retrieved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmTicketGetProfile", ex, new { lId, tId });
                return Fail<LocationTradeProfileDto>(500, "Failed to retrieve the PM profile");
            }
        }

        /// <summary>Write-through of the profile fields shown on the ticket (unit count, access, mounting, access note). Same validation and audit as the admin route.</summary>
        [HttpPut("locations/{lId:int}/profile/{tId:int}")]
        public async Task<ActionResult<ApiResponse<LocationTradeProfileDto>>> SaveProfile(int lId, int tId, [FromBody] SaveLocationTradeProfileRequest request)
        {
            if (request.UnitCount is < 0 or > 999) return Fail<LocationTradeProfileDto>(400, "Unit count must be between 0 and 999");
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (!await _assets.LocationExistsAsync(lId)) return Fail<LocationTradeProfileDto>(404, "Location not found");
                if (!string.IsNullOrWhiteSpace(request.AccessRequirements) && string.IsNullOrWhiteSpace(request.AccessNote))
                {
                    var needsNote = (await _assets.GetAccessRequirementsAsync()).Where(a => a.RequiresNote).Select(a => a.LarId.ToString()).ToHashSet();
                    if (request.AccessRequirements.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Any(needsNote.Contains))
                        return Fail<LocationTradeProfileDto>(400, "An access note is required when 'Other' is one of the access requirements");
                }
                var before = await _assets.GetProfileAsync(lId, tId);
                // Ticket entry only edits the fields it shows; keep the rest of an existing profile as it is.
                if (before != null)
                {
                    request.AttIdAerial ??= before.AttIdAerial;
                    request.ManagerName ??= before.ManagerName;
                    request.ManagerPhone ??= before.ManagerPhone;
                    request.Note ??= before.Note;
                }
                var after = await _assets.UpsertProfileAsync(lId, tId, request);
                var (oldValues, newValues) = ChangeDiff.Diff(before, request);
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync(
                    $"Location Trade Profile {(before == null ? "Created" : "Updated")} (ticket entry) - ID: {after.LtpId} - location {lId} - trade {tId}",
                    oldValues, newValues, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                await LogAuditAsync("PmTicketSaveProfile", new { lId, tId, after.LtpId, changed = newValues.Keys });
                return Ok(Ok(after, "PM profile saved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmTicketSaveProfile", ex, new { lId, tId, request });
                return Fail<LocationTradeProfileDto>(500, "Failed to save the PM profile");
            }
        }

        [HttpGet("locations/{lId:int}/assets")]
        public async Task<ActionResult<ApiResponse<List<AssetDto>>>> GetLocationAssets(int lId, [FromQuery] int? tId)
        {
            try
            {
                var rows = await _assets.GetLocationAssetsAsync(lId, tId, false);
                return Ok(Ok(rows, $"Retrieved {rows.Count} assets"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmTicketGetAssets", ex, new { lId, tId });
                return Fail<List<AssetDto>>(500, "Failed to retrieve the location's assets");
            }
        }

        // Lookups the unit builder and profile panel need, mirrored from the admin-only Asset / Form controllers.
        [HttpGet("lookups/mount-locations")]
        public async Task<ActionResult<ApiResponse<List<AssetMountLocationDto>>>> GetMountLocations()
        {
            try { var rows = await _assets.GetMountLocationsAsync(); return Ok(Ok(rows, $"Retrieved {rows.Count} mount locations")); }
            catch (Exception ex) { await LogAuditErrorAsync("PmTicketGetMountLocations", ex, null); return Fail<List<AssetMountLocationDto>>(500, "Failed to retrieve mount locations"); }
        }

        [HttpGet("lookups/access-requirements")]
        public async Task<ActionResult<ApiResponse<List<LocationAccessRequirementDto>>>> GetAccessRequirements()
        {
            try { var rows = await _assets.GetAccessRequirementsAsync(); return Ok(Ok(rows, $"Retrieved {rows.Count} access requirements")); }
            catch (Exception ex) { await LogAuditErrorAsync("PmTicketGetAccessRequirements", ex, null); return Fail<List<LocationAccessRequirementDto>>(500, "Failed to retrieve access requirements"); }
        }

        [HttpGet("trades/{tId:int}/asset-categories")]
        public async Task<ActionResult<ApiResponse<List<FormAssetCategoryDto>>>> GetAssetCategories(int tId)
        {
            try { var rows = await _forms.GetAssetCategoriesAsync(tId); return Ok(Ok(rows, $"Retrieved {rows.Count} equipment types")); }
            catch (Exception ex) { await LogAuditErrorAsync("PmTicketGetAssetCategories", ex, new { tId }); return Fail<List<FormAssetCategoryDto>>(500, "Failed to retrieve equipment types"); }
        }

        [HttpGet("locations/{lId:int}/first-time/{tId:int}")]
        public async Task<ActionResult<ApiResponse<PmFirstTimeDto>>> GetFirstTime(int lId, int tId)
        {
            try
            {
                var result = await _visits.GetFirstTimeAsync(lId, tId);
                return Ok(Ok(result, result.Detail));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmTicketFirstTime", ex, new { lId, tId });
                return Fail<PmFirstTimeDto>(500, "Failed to check earlier PM visits");
            }
        }

        #endregion

        #region pricing

        /// <summary>Prices the ticket's units and add-ons from the rule's tiers. Pure calculation; nothing is stored.</summary>
        [HttpPost("price")]
        public async Task<ActionResult<ApiResponse<PmPriceResponse>>> Price([FromBody] PmPriceRequest request)
        {
            if (request.XcccId <= 0 || request.TId <= 0) return Fail<PmPriceResponse>(400, "Company and parent trade are required");
            try
            {
                var rule = await _forms.GetRuleAsync(request.XcccId, request.TId);
                var result = PmPricingCalculator.Calculate(rule, request);
                return Ok(Ok(result, result.Warnings.Count == 0 ? "Priced" : string.Join(" ", result.Warnings)));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmTicketPrice", ex, request);
                return Fail<PmPriceResponse>(500, "Failed to price the ticket");
            }
        }

        #endregion

        #region visit snapshot

        [HttpGet("visits/{srId:int}")]
        public async Task<ActionResult<ApiResponse<PmVisitDto>>> GetVisit(int srId)
        {
            try
            {
                var visit = await _visits.GetVisitBySrAsync(srId);
                if (visit == null) return Fail<PmVisitDto>(404, "No PM visit for this service request");
                return Ok(Ok(visit, "Visit retrieved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmTicketGetVisit", ex, new { srId });
                return Fail<PmVisitDto>(500, "Failed to retrieve the PM visit");
            }
        }

        /// <summary>
        /// Creates the PMVisit + PMVisitUnit snapshot for a just-created Preventative SR: pins the rule, template version and
        /// season, copies the customer's parameters and the pricing breakdown. Safe to retry: an SR that already has a visit
        /// gets its existing visit back (200) and nothing is changed.
        /// </summary>
        [HttpPost("visits")]
        public async Task<ActionResult<ApiResponse<PmVisitDto>>> CreateVisit([FromBody] CreatePmVisitRequest request)
        {
            if (request.SrId <= 0 || request.XcccId <= 0 || request.TId <= 0) return Fail<PmVisitDto>(400, "Service request, company and parent trade are required");
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (!await _visits.ServiceRequestExistsAsync(request.SrId)) return Fail<PmVisitDto>(404, "Service request not found");
                var rule = await _forms.GetRuleAsync(request.XcccId, request.TId);
                var existed = await _visits.GetVisitBySrAsync(request.SrId) != null;
                var pmvId = await _visits.CreateVisitAsync(request, rule, UserId > 0 ? UserId : null);
                var visit = (await _visits.GetVisitBySrAsync(request.SrId))!;
                if (!existed)
                {
                    SetAuditCriticalUserContext();
                    var (oldValues, newValues) = ChangeDiff.Diff(null, new
                    {
                        request.SrId, rule?.FrId, request.TIdSeason, request.PmsId, request.VisitType, request.FirstTime, request.FirstTimeSource,
                        request.ParamsConfirmed, request.ContractTotal, Units = request.Units.Count
                    });
                    await _auditCriticalService.LogChangeAsync($"PM Visit Created - ID: {pmvId} - SR {request.SrId}", oldValues, newValues, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                }
                await LogAuditAsync("PmTicketCreateVisit", new { request.SrId, pmvId, existed, units = request.Units.Count });
                return Ok(Ok(visit, existed ? "Visit already existed" : "Visit created"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmTicketCreateVisit", ex, request);
                return Fail<PmVisitDto>(500, "Failed to create the PM visit");
            }
        }

        #endregion
    }
}
