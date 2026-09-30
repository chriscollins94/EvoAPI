using System.Diagnostics;
using EvoAPI.Core.Interfaces;
using EvoAPI.Core.Services;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using Microsoft.AspNetCore.Mvc;

namespace EvoAPI.Api.Controllers
{
    /// <summary>
    /// The technician's PM visit (PM build slice 4a, plan section 6.3 / 8d).
    ///
    /// A Preventative ticket is an ordinary service request with a work order; the tech's time (check-in / check-out),
    /// photos and materials still go through the legacy EvoWS calls exactly as the legacy schedule page makes them.
    /// This controller owns what is new: the visit record and its answers, the units worked, the template resolved per
    /// unit against the real asset, the weather captured at check-in, and the "my PM visits" list. One GET opens the
    /// whole visit; answers are upserted one at a time (save-as-you-go).
    ///
    /// Access: the caller must be assigned to a work order on the SR, or be an admin. Logged-in level; the UI hides
    /// everything behind the PreventativeMaintenance flag.
    /// </summary>
    [ApiController]
    [Route("EvoApi/pm/visits")]
    [EvoAuthorize]
    public class PmVisitController : BaseController
    {
        private static readonly string[] UnitStatuses = { "Pending", "Serviced", "NotServiced" };
        private static readonly string[] HeatingFallback = { "Gas", "Electric", "Heat Pump", "Hot Water", "Steam", "Oil", "None", "Other" };

        private readonly IPmVisitRepository _visits;
        private readonly IFormRepository _forms;
        private readonly IAssetRepository _assets;
        private readonly IWeatherService _weather;
        private readonly IAuditCriticalService _auditCriticalService;
        private readonly IDataService _dataService;

        // Dictionary rows that describe a finding (F181-F191, P25) are captured on the Findings panel, not as questions.
        private static readonly HashSet<string> FindingCodes = new(StringComparer.OrdinalIgnoreCase) { "F181", "F182", "F183", "F184", "F185", "F186", "F187", "F188", "F189", "F190", "F191", "P25" };

        public PmVisitController(IPmVisitRepository visits, IFormRepository forms, IAssetRepository assets, IWeatherService weather,
            IAuditService auditService, IAuditCriticalService auditCriticalService, IDataService dataService)
        {
            _visits = visits;
            _forms = forms;
            _assets = assets;
            _weather = weather;
            _auditCriticalService = auditCriticalService;
            _dataService = dataService;
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

        #region helpers

        /// <summary>Header + visit for an SR the caller may work on; null result carries the failure to return.</summary>
        private async Task<(PmVisitHeaderDto? header, PmVisitDto? visit, ActionResult<ApiResponse<T>>? fail)> LoadAsync<T>(int srId)
        {
            var header = await _visits.GetHeaderAsync(srId, UserId);
            if (header == null) return (null, null, Fail<T>(404, "Service request not found"));
            var visit = await _visits.GetVisitBySrAsync(srId);
            if (visit == null) return (header, null, Fail<T>(404, "This ticket has no PM visit record. It was not created through the Preventative path; open it in the legacy work order instead."));
            if (!header.AssignedToCaller && !IsAdmin) return (header, visit, Fail<T>(403, "You are not assigned to this ticket"));
            return (header, visit, null);
        }

        private async Task<(FormTemplateDetailDto? template, FormRuleDto? rule)> LoadTemplateAndRuleAsync(PmVisitHeaderDto header, PmVisitDto visit)
        {
            var rule = await _forms.GetRuleAsync(header.XcccId, header.TIdParent);
            var ftId = visit.FtId ?? rule?.FtId;
            if (!ftId.HasValue)
            {
                var templates = await _forms.GetTemplatesAsync();
                ftId = templates.Where(t => t.TId == header.TIdParent && t.Active).OrderByDescending(t => t.Version).Select(t => (int?)t.FtId).FirstOrDefault();
            }
            var template = ftId.HasValue ? await _forms.GetTemplateAsync(ftId.Value) : null;
            return (template, rule);
        }

        private static List<FormPreviewSectionDto> Phase(FormPreviewDto preview, string phase) =>
            preview.Sections.Where(s => s.Included && string.Equals(s.Phase, phase, StringComparison.OrdinalIgnoreCase))
                .Select(s => { s.Questions = s.Questions.Where(q => q.Included && !FindingCodes.Contains(q.Code)).OrderBy(q => q.Order).ToList(); return s; })
                .Where(s => s.Questions.Count > 0)
                .ToList();

        /// <summary>Pick lists for a finding from the template's own finding questions, so the wording is the dictionary's.</summary>
        private static PmFindingOptionsDto FindingOptions(FormTemplateDetailDto? template)
        {
            var o = new PmFindingOptionsDto();
            if (template == null) return o;
            List<string> Values(string code)
            {
                var q = template.Sections.SelectMany(x => x.Questions).FirstOrDefault(x => string.Equals(x.Code, code, StringComparison.OrdinalIgnoreCase));
                return (q?.AnswerListValues ?? q?.AnswerValues ?? "").Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
            }
            o.Severity = Values("F184");
            o.Impact = Values("F185");
            o.Risk = Values("F186");
            o.Action = Values("F187");
            return o;
        }

        /// <summary>Resolves the Unit-phase sections for one visit unit against its asset's facts (or just its equipment type when it has no asset yet).</summary>
        private async Task FillUnitAsync(PmVisitUnitOpenDto unit, FormTemplateDetailDto? template, FormRuleDto? rule, string? season)
        {
            FormResolveFacts facts;
            if (unit.AsId.HasValue)
            {
                unit.Asset = await _assets.GetAssetAsync(unit.AsId.Value);
                var f = await _assets.GetAssetFactsAsync(unit.AsId.Value);
                facts = f == null
                    ? new FormResolveFacts { Season = season, EquipmentType = unit.EquipmentType }
                    : new FormResolveFacts { Season = season, EquipmentType = f.EquipmentType, HeatingType = f.HeatingType, Mount = f.Mount, AsId = f.AsId, AssetLabel = f.Label, Attributes = new(f.Attributes, StringComparer.OrdinalIgnoreCase), RepeatCounts = new(f.RepeatCounts, StringComparer.OrdinalIgnoreCase) };
                if (unit.Asset != null)
                {
                    unit.EquipmentType = unit.Asset.Category;
                    unit.CapacityTons ??= unit.Asset.CapacityTons;
                }
            }
            else
            {
                facts = new FormResolveFacts { Season = season, EquipmentType = unit.EquipmentType };
            }
            unit.HeatingType = facts.HeatingType;
            unit.Mount = facts.Mount;
            unit.RepeatCounts = facts.RepeatCounts.Count > 0 ? new Dictionary<string, int>(facts.RepeatCounts) : null;
            unit.Sections = template == null ? new List<FormPreviewSectionDto>() : Phase(FormTemplateResolver.Resolve(template, rule, facts), "Unit");
        }

        private static string UnitLabel(AssetDto a) =>
            string.Join(" · ", new[] { string.IsNullOrWhiteSpace(a.UnitTag) ? $"Unit {a.AsId}" : a.UnitTag!.Trim(), a.Category, string.Join(" ", new[] { a.ManufacturerName ?? a.Manufacturer, a.ModelNumber }.Where(s => !string.IsNullOrWhiteSpace(s))) }.Where(s => !string.IsNullOrWhiteSpace(s)));

        #endregion

        /// <summary>Feature flag at logged-in level so the legacy tech schedule can decide whether to route a PM job here.</summary>
        [HttpGet("enabled")]
        public async Task<ActionResult<ApiResponse<bool>>> Enabled()
        {
            try { return Ok(Ok(await _forms.IsFeatureEnabledAsync(), "ok")); }
            catch (Exception ex) { await LogAuditErrorAsync("PmVisitEnabled", ex, null); return Fail<bool>(500, "Failed to read the feature flag"); }
        }

        /// <summary>Preventative work orders assigned to the caller (admins may pass ?all=true for every tech).</summary>
        [HttpGet("mine")]
        public async Task<ActionResult<ApiResponse<List<PmVisitListItemDto>>>> Mine([FromQuery] bool all = false)
        {
            try
            {
                var rows = await _visits.GetMyVisitsAsync(UserId, all && IsAdmin);
                return Ok(Ok(rows, $"Retrieved {rows.Count} PM visits"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitMine", ex, new { all });
                return Fail<List<PmVisitListItemDto>>(500, "Failed to retrieve your PM visits");
            }
        }

        /// <summary>Does this SR have a PM visit record? Used by the legacy tech schedule before routing to the visit page.</summary>
        [HttpGet("{srId:int}/exists")]
        public async Task<ActionResult<ApiResponse<PmVisitExistsDto>>> Exists(int srId)
        {
            try
            {
                var v = await _visits.GetVisitBySrAsync(srId);
                return Ok(Ok(new PmVisitExistsDto { Exists = v != null, PmvId = v?.PmvId, Status = v?.Status }, v != null ? "PM visit" : "No PM visit"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitExists", ex, new { srId });
                return Fail<PmVisitExistsDto>(500, "Failed to check the PM visit");
            }
        }

        /// <summary>Everything the visit page needs: header, snapshot, profile, Site and Checkout sections, units with their resolved sections, answers, lookups.</summary>
        [HttpGet("{srId:int}")]
        public async Task<ActionResult<ApiResponse<PmVisitOpenDto>>> Open(int srId)
        {
            try
            {
                var (header, visit, fail) = await LoadAsync<PmVisitOpenDto>(srId);
                if (fail != null) return fail;
                var (template, rule) = await LoadTemplateAndRuleAsync(header!, visit!);

                var open = new PmVisitOpenDto { Header = header!, Visit = visit!, TemplateName = template?.Name, TemplateVersion = template?.Version };

                var profile = await _assets.GetProfileAsync(header!.LId, header.TIdParent);
                open.Profile = profile;
                var mounts = await _assets.GetMountLocationsAsync();
                var access = await _assets.GetAccessRequirementsAsync();
                if (profile != null)
                {
                    var accessIds = (profile.AccessRequirements ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(x => int.TryParse(x, out var i) ? i : 0).ToHashSet();
                    var mountIds = (profile.MountLocations ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Select(x => int.TryParse(x, out var i) ? i : 0).ToHashSet();
                    open.ProfileAccessNames = access.Where(a => accessIds.Contains(a.LarId)).Select(a => a.Requirement).ToList();
                    open.ProfileMountNames = mounts.Where(m => mountIds.Contains(m.AmlId)).Select(m => m.Location).ToList();
                }

                if (template != null)
                {
                    var site = FormTemplateResolver.Resolve(template, rule, new FormResolveFacts { Season = visit!.VisitType });
                    open.SiteSections = Phase(site, "Site");
                    open.CheckoutSections = Phase(site, "Checkout");
                }

                foreach (var u in visit!.Units)
                {
                    var unit = await _visits.GetUnitAsync(u.PmvuId);
                    if (unit == null) continue;
                    await FillUnitAsync(unit, template, rule, visit.VisitType);
                    open.Units.Add(unit);
                }

                open.Answers = await _visits.GetAnswersAsync(visit.PmvId);
                open.Findings = await _visits.GetFindingsAsync(visit.PmvId);
                open.Materials = await _visits.GetMaterialsAsync(srId);
                open.FindingOptions = FindingOptions(template);
                open.TechName = UserFullName;
                if (rule?.Pm != null)
                    open.Inclusions = new PmVisitInclusionsDto { FiltersIncluded = rule.Pm.FiltersIncluded == true, NoFilterChange = rule.Pm.NoFilterChange == true, FreeFilters = rule.Pm.FreeFilters, FreeBeltChangesPerYear = rule.Pm.FreeBeltChangesPerYear, BeltsChargeable = rule.Pm.BeltsChargeable == true };

                var lists = await _forms.GetAnswerListsAsync();
                var heat = lists.FirstOrDefault(l => string.Equals(l.Name, "Heating Type", StringComparison.OrdinalIgnoreCase));
                open.Lookups = new PmVisitLookupsDto
                {
                    Categories = await _forms.GetAssetCategoriesAsync(header.TIdParent),
                    ComponentTypes = (await _assets.GetComponentTypesAsync(header.TIdParent)).Where(c => c.Active).ToList(),
                    AttributeTypes = (await _assets.GetAttributeTypesAsync(header.TIdParent)).Where(a => a.Active).ToList(),
                    MountLocations = mounts.Where(m => m.Active).ToList(),
                    HeatingTypes = heat?.Values?.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList() ?? HeatingFallback.ToList()
                };
                return Ok(Ok(open, "Visit opened"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitOpen", ex, new { srId });
                return Fail<PmVisitOpenDto>(500, "Failed to open the PM visit");
            }
        }

        [HttpGet("lookups/manufacturers/{ascId:int}")]
        public async Task<ActionResult<ApiResponse<List<AssetManufacturerDto>>>> Manufacturers(int ascId)
        {
            try { var rows = await _assets.GetManufacturersAsync(ascId); return Ok(Ok(rows, $"Retrieved {rows.Count} manufacturers")); }
            catch (Exception ex) { await LogAuditErrorAsync("PmVisitManufacturers", ex, new { ascId }); return Fail<List<AssetManufacturerDto>>(500, "Failed to retrieve manufacturers"); }
        }

        /// <summary>
        /// Called right after the legacy check-in succeeds: marks the visit in progress and captures the weather and outdoor
        /// temperature at the device's position (falling back to the site's address coordinates). Weather failure never fails the check-in.
        /// </summary>
        [HttpPost("{srId:int}/checkin")]
        public async Task<ActionResult<ApiResponse<PmCheckInResultDto>>> CheckIn(int srId, [FromBody] PmCheckInRequest request)
        {
            try
            {
                var (header, visit, fail) = await LoadAsync<PmCheckInResultDto>(srId);
                if (fail != null) return fail;
                var lat = request?.Latitude ?? header!.Latitude;
                var lon = request?.Longitude ?? header!.Longitude;
                WeatherResultDto? weather = null;
                string? weatherError = null;
                if (lat.HasValue && lon.HasValue && lat != 0 && lon != 0)
                {
                    weather = await _weather.GetCurrentAsync(lat.Value, lon.Value);
                    if (weather == null) weatherError = "Weather service did not answer; try again from the Site tab.";
                }
                else weatherError = "No position available for the weather (device location denied and the site has no coordinates).";

                await _visits.RecordCheckInAsync(visit!.PmvId, weather);
                await LogAuditAsync("PmVisitCheckIn", new { srId, visit.PmvId, lat, lon, weather = weather?.Description, weather?.TemperatureF });
                var after = (await _visits.GetVisitBySrAsync(srId))!;
                return Ok(Ok(new PmCheckInResultDto
                {
                    Status = after.Status,
                    Weather = weather?.Description ?? null,
                    OutdoorTempF = weather?.TemperatureF,
                    WeatherSource = weather?.Source,
                    WeatherDateTime = weather?.ObservedAt,
                    WeatherError = weatherError
                }, weather != null ? "Checked in; weather captured" : "Checked in"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitCheckIn", ex, new { srId, request });
                return Fail<PmCheckInResultDto>(500, "Failed to record the check-in");
            }
        }

        /// <summary>Saves one answer (site, checkout or unit level, with repeat index). Save-as-you-go; the first answer moves the visit to In Progress.</summary>
        [HttpPut("{srId:int}/answers")]
        public async Task<ActionResult<ApiResponse<PmVisitAnswerDto>>> SaveAnswer(int srId, [FromBody] SavePmVisitAnswerRequest request)
        {
            if (request.FqId <= 0) return Fail<PmVisitAnswerDto>(400, "Question is required");
            try
            {
                var (header, visit, fail) = await LoadAsync<PmVisitAnswerDto>(srId);
                if (fail != null) return fail;
                if (request.PmvuId.HasValue && !visit!.Units.Any(u => u.PmvuId == request.PmvuId.Value))
                    return Fail<PmVisitAnswerDto>(400, "That unit is not on this visit");
                var question = await _forms.GetQuestionAsync(request.FqId);
                if (question == null) return Fail<PmVisitAnswerDto>(404, "Question not found");
                if (string.IsNullOrWhiteSpace(request.Question)) request.Question = question.Question;
                if (request.Numeric == null && !string.IsNullOrWhiteSpace(request.Answer)
                    && decimal.TryParse(request.Answer, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var n)
                    && (string.Equals(question.DataType, "Number", StringComparison.OrdinalIgnoreCase) || string.Equals(question.DataType, "Decimal", StringComparison.OrdinalIgnoreCase)))
                    request.Numeric = n;
                var saved = await _visits.UpsertAnswerAsync(visit!.PmvId, request, question.Code, UserId > 0 ? UserId : null);
                return Ok(Ok(saved, "Saved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitSaveAnswer", ex, new { srId, request });
                return Fail<PmVisitAnswerDto>(500, "Failed to save the answer");
            }
        }

        /// <summary>One unit re-resolved (after its asset was edited, the conditions and repeat counts may have changed).</summary>
        [HttpGet("{srId:int}/units/{pmvuId:int}")]
        public async Task<ActionResult<ApiResponse<PmVisitUnitOpenDto>>> GetUnit(int srId, int pmvuId)
        {
            try
            {
                var (header, visit, fail) = await LoadAsync<PmVisitUnitOpenDto>(srId);
                if (fail != null) return fail;
                var unit = await _visits.GetUnitAsync(pmvuId);
                if (unit == null || unit.PmvId != visit!.PmvId) return Fail<PmVisitUnitOpenDto>(404, "Unit not found on this visit");
                var (template, rule) = await LoadTemplateAndRuleAsync(header!, visit);
                await FillUnitAsync(unit, template, rule, visit.VisitType);
                return Ok(Ok(unit, "Unit retrieved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitGetUnit", ex, new { srId, pmvuId });
                return Fail<PmVisitUnitOpenDto>(500, "Failed to retrieve the unit");
            }
        }

        /// <summary>Unit status (serviced / not serviced with a reason), asset confirmation, note.</summary>
        [HttpPut("{srId:int}/units/{pmvuId:int}")]
        public async Task<ActionResult<ApiResponse<PmVisitUnitOpenDto>>> UpdateUnit(int srId, int pmvuId, [FromBody] SavePmVisitUnitRequest request)
        {
            if (!string.IsNullOrWhiteSpace(request.Status) && !UnitStatuses.Contains(request.Status)) return Fail<PmVisitUnitOpenDto>(400, "Status must be Pending, Serviced or NotServiced");
            if (request.Status == "NotServiced" && string.IsNullOrWhiteSpace(request.NotServicedReason)) return Fail<PmVisitUnitOpenDto>(400, "A reason is required when a unit is not serviced");
            try
            {
                var (header, visit, fail) = await LoadAsync<PmVisitUnitOpenDto>(srId);
                if (fail != null) return fail;
                var unit = await _visits.GetUnitAsync(pmvuId);
                if (unit == null || unit.PmvId != visit!.PmvId) return Fail<PmVisitUnitOpenDto>(404, "Unit not found on this visit");
                if (request.AssetConfirmed == true && unit.AsId.HasValue)
                {
                    // confirming the unit stamps the asset as verified by the tech
                    var asset = await _assets.GetAssetAsync(unit.AsId.Value);
                    if (asset != null) await _assets.UpdateAssetAsync(unit.AsId.Value, ToSaveRequest(asset, markVerified: true), UserId > 0 ? UserId : null);
                }
                await _visits.UpdateUnitAsync(pmvuId, request);
                await LogAuditAsync("PmVisitUpdateUnit", new { srId, pmvuId, request.Status, request.AssetConfirmed });
                var after = (await _visits.GetUnitAsync(pmvuId))!;
                var (template, rule) = await LoadTemplateAndRuleAsync(header!, visit);
                await FillUnitAsync(after, template, rule, visit.VisitType);
                return Ok(Ok(after, "Unit updated"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitUpdateUnit", ex, new { srId, pmvuId, request });
                return Fail<PmVisitUnitOpenDto>(500, "Failed to update the unit");
            }
        }

        /// <summary>
        /// The tech's asset edit for a unit. Writes to the asset master (identity, components, attributes) with the verification
        /// stamp; a unit that was only typed at ticket entry gets its asset created and linked. Returns the unit re-resolved.
        /// </summary>
        [HttpPut("{srId:int}/units/{pmvuId:int}/asset")]
        public async Task<ActionResult<ApiResponse<PmVisitUnitOpenDto>>> SaveUnitAsset(int srId, int pmvuId, [FromBody] SaveAssetRequest request)
        {
            var err = ValidateAsset(request);
            if (err != null) return Fail<PmVisitUnitOpenDto>(400, err);
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var (header, visit, fail) = await LoadAsync<PmVisitUnitOpenDto>(srId);
                if (fail != null) return fail;
                var unit = await _visits.GetUnitAsync(pmvuId);
                if (unit == null || unit.PmvId != visit!.PmvId) return Fail<PmVisitUnitOpenDto>(404, "Unit not found on this visit");
                if (await _assets.GetCategoryAsync(request.AscId) == null) return Fail<PmVisitUnitOpenDto>(400, "Equipment type not found");
                request.MarkVerified = true;
                request.Active = true;
                var userId = UserId > 0 ? UserId : (int?)null;
                AssetDto? before = null;
                int asId;
                if (unit.AsId.HasValue)
                {
                    before = await _assets.GetAssetAsync(unit.AsId.Value);
                    if (request.AsIdConnected == unit.AsId) return Fail<PmVisitUnitOpenDto>(400, "A unit cannot be connected to itself");
                    await _assets.UpdateAssetAsync(unit.AsId.Value, request, userId);
                    asId = unit.AsId.Value;
                }
                else
                {
                    asId = await _assets.CreateAssetAsync(header!.LId, request, userId);
                }
                var after = (await _assets.GetAssetAsync(asId))!;
                await _visits.LinkUnitAssetAsync(pmvuId, asId, after.AscId, after.CapacityTons, UnitLabel(after));
                await _visits.UpdateUnitAsync(pmvuId, new SavePmVisitUnitRequest { AssetConfirmed = true });

                var (oldValues, newValues) = ChangeDiff.Diff(before, request);
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync($"Asset {(before == null ? "Created" : "Updated")} by tech on PM visit - ID: {asId} - {after.Category} {after.UnitTag} - SR {srId}", oldValues, newValues, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                await LogAuditAsync("PmVisitSaveUnitAsset", new { srId, pmvuId, asId, created = before == null, changed = newValues.Keys });

                var result = (await _visits.GetUnitAsync(pmvuId))!;
                var (template, rule) = await LoadTemplateAndRuleAsync(header!, visit);
                await FillUnitAsync(result, template, rule, visit.VisitType);
                return Ok(Ok(result, before == null ? "Unit captured" : "Unit updated"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitSaveUnitAsset", ex, new { srId, pmvuId, request });
                return Fail<PmVisitUnitOpenDto>(500, "Failed to save the unit");
            }
        }

        /// <summary>A unit found on site that was not on the ticket: creates the asset at the location and adds it to the visit.</summary>
        [HttpPost("{srId:int}/units")]
        public async Task<ActionResult<ApiResponse<PmVisitUnitOpenDto>>> AddUnit(int srId, [FromBody] AddPmVisitUnitRequest request)
        {
            var err = ValidateAsset(request.Asset);
            if (err != null) return Fail<PmVisitUnitOpenDto>(400, err);
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var (header, visit, fail) = await LoadAsync<PmVisitUnitOpenDto>(srId);
                if (fail != null) return fail;
                if (await _assets.GetCategoryAsync(request.Asset.AscId) == null) return Fail<PmVisitUnitOpenDto>(400, "Equipment type not found");
                request.Asset.MarkVerified = true;
                request.Asset.Active = true;
                var userId = UserId > 0 ? UserId : (int?)null;
                var asId = await _assets.CreateAssetAsync(header!.LId, request.Asset, userId);
                var asset = (await _assets.GetAssetAsync(asId))!;
                var pmvuId = await _visits.AddUnitAsync(visit!.PmvId, asId, asset.AscId, asset.CapacityTons, UnitLabel(asset));
                await _visits.UpdateUnitAsync(pmvuId, new SavePmVisitUnitRequest { AssetConfirmed = true });

                var (oldValues, newValues) = ChangeDiff.Diff(null, request.Asset);
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync($"Asset Created by tech on PM visit - ID: {asId} - {asset.Category} {asset.UnitTag} - SR {srId}", oldValues, newValues, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                await LogAuditAsync("PmVisitAddUnit", new { srId, pmvuId, asId });

                var unit = (await _visits.GetUnitAsync(pmvuId))!;
                var (template, rule) = await LoadTemplateAndRuleAsync(header!, visit);
                await FillUnitAsync(unit, template, rule, visit.VisitType);
                return Ok(Ok(unit, "Unit added"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitAddUnit", ex, new { srId, request });
                return Fail<PmVisitUnitOpenDto>(500, "Failed to add the unit");
            }
        }

        #region checkout (slice 4b)

        [HttpPost("{srId:int}/findings")]
        public async Task<ActionResult<ApiResponse<PmVisitFindingDto>>> AddFinding(int srId, [FromBody] SavePmVisitFindingRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Description) && string.IsNullOrWhiteSpace(request.Component)) return Fail<PmVisitFindingDto>(400, "Describe the finding");
            try
            {
                var (header, visit, fail) = await LoadAsync<PmVisitFindingDto>(srId);
                if (fail != null) return fail;
                if (request.PmvuId.HasValue && !visit!.Units.Any(u => u.PmvuId == request.PmvuId.Value)) return Fail<PmVisitFindingDto>(400, "That unit is not on this visit");
                var id = await _visits.CreateFindingAsync(visit!.PmvId, request, UserId > 0 ? UserId : null);
                await LogAuditAsync("PmVisitAddFinding", new { srId, id, request.PmvuId, request.Severity, request.QuoteRequired });
                return Ok(Ok((await _visits.GetFindingAsync(id))!, "Finding added"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitAddFinding", ex, new { srId, request });
                return Fail<PmVisitFindingDto>(500, "Failed to add the finding");
            }
        }

        [HttpPut("{srId:int}/findings/{pmvfId:int}")]
        public async Task<ActionResult<ApiResponse<PmVisitFindingDto>>> UpdateFinding(int srId, int pmvfId, [FromBody] SavePmVisitFindingRequest request)
        {
            try
            {
                var (header, visit, fail) = await LoadAsync<PmVisitFindingDto>(srId);
                if (fail != null) return fail;
                var existing = await _visits.GetFindingAsync(pmvfId);
                if (existing == null || existing.PmvId != visit!.PmvId) return Fail<PmVisitFindingDto>(404, "Finding not found on this visit");
                if (existing.SrIdProposal.HasValue && !request.QuoteRequired) request.QuoteRequired = true;   // already on a proposal ticket; stays there
                await _visits.UpdateFindingAsync(pmvfId, request, UserId > 0 ? UserId : null);
                await LogAuditAsync("PmVisitUpdateFinding", new { srId, pmvfId, request.Severity, request.QuoteRequired });
                return Ok(Ok((await _visits.GetFindingAsync(pmvfId))!, "Finding saved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitUpdateFinding", ex, new { srId, pmvfId, request });
                return Fail<PmVisitFindingDto>(500, "Failed to save the finding");
            }
        }

        [HttpDelete("{srId:int}/findings/{pmvfId:int}")]
        public async Task<ActionResult<ApiResponse<bool>>> DeleteFinding(int srId, int pmvfId)
        {
            try
            {
                var (header, visit, fail) = await LoadAsync<bool>(srId);
                if (fail != null) return fail;
                var existing = await _visits.GetFindingAsync(pmvfId);
                if (existing == null || existing.PmvId != visit!.PmvId) return Fail<bool>(404, "Finding not found on this visit");
                if (existing.SrIdProposal.HasValue) return Fail<bool>(409, "This finding is already on a proposal ticket and cannot be removed here");
                await _visits.DeleteFindingAsync(pmvfId);
                await LogAuditAsync("PmVisitDeleteFinding", new { srId, pmvfId });
                return Ok(Ok(true, "Finding removed"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitDeleteFinding", ex, new { srId, pmvfId });
                return Fail<bool>(500, "Failed to remove the finding");
            }
        }

        /// <summary>Materials on the ticket's work orders. Written through the legacy service by the page; this is the read side.</summary>
        [HttpGet("{srId:int}/materials")]
        public async Task<ActionResult<ApiResponse<List<PmVisitMaterialDto>>>> Materials(int srId)
        {
            try
            {
                var (header, visit, fail) = await LoadAsync<List<PmVisitMaterialDto>>(srId);
                if (fail != null) return fail;
                var rows = await _visits.GetMaterialsAsync(srId);
                return Ok(Ok(rows, $"Retrieved {rows.Count} materials"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitMaterials", ex, new { srId });
                return Fail<List<PmVisitMaterialDto>>(500, "Failed to retrieve materials");
            }
        }

        /// <summary>
        /// Submits the visit: every unit serviced or not serviced, every required site / unit / checkout question answered
        /// (the tech signature always; the manager signature when the customer's closeout documents include a sign-off), the
        /// customer's form confirmed when one is required. Then writes the checkout fields, creates one proposal ticket for the
        /// findings that need a quote, and answers with the secondary status the page hands to the legacy check-out.
        /// Blocking items come back as a list with Submitted = false; nothing is written in that case.
        /// </summary>
        [HttpPost("{srId:int}/submit")]
        public async Task<ActionResult<ApiResponse<PmVisitSubmitResultDto>>> Submit(int srId, [FromBody] PmVisitSubmitRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var (header, visit, fail) = await LoadAsync<PmVisitSubmitResultDto>(srId);
                if (fail != null) return fail;
                if (visit!.Status is "Submitted" or "QCApproved" or "Sent") return Fail<PmVisitSubmitResultDto>(409, "This visit was already submitted");
                var (template, rule) = await LoadTemplateAndRuleAsync(header!, visit);
                var answers = await _visits.GetAnswersAsync(visit.PmvId);
                var result = new PmVisitSubmitResultDto { Status = visit.Status };

                List<FormPreviewSectionDto> siteSections = new(), checkoutSections = new();
                if (template != null)
                {
                    var resolved = FormTemplateResolver.Resolve(template, rule, new FormResolveFacts { Season = visit.VisitType });
                    siteSections = Phase(resolved, "Site");
                    checkoutSections = Phase(resolved, "Checkout");
                }
                var site = PmVisitValidator.BuildScope("Site", null, siteSections, answers, null);
                result.Blocking.AddRange(PmVisitValidator.Check(site).Select(ToItem));

                var signoffRequired = (visit.CloseoutDocs ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Any(d => string.Equals(d, "Signoff", StringComparison.OrdinalIgnoreCase));
                var forceRequired = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "F204" };
                var forceOptional = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "F203", "P28", "F029" };
                if (signoffRequired) forceRequired.Add("F202"); else forceOptional.Add("F202");
                var checkout = PmVisitValidator.BuildScope("Checkout", null, checkoutSections, answers, null);
                result.Blocking.AddRange(PmVisitValidator.Check(checkout, forceRequired, forceOptional).Select(ToItem));

                foreach (var u in visit.Units)
                {
                    if (u.Status == "Pending")
                    {
                        result.Blocking.Add(new PmVisitValidatorItemDto { Scope = u.Label ?? $"Unit {u.Sequence}", PmvuId = u.PmvuId, Code = "UNIT", Question = "Mark the unit serviced or not serviced" });
                        continue;
                    }
                    if (u.Status != "Serviced") continue;
                    var unit = await _visits.GetUnitAsync(u.PmvuId);
                    if (unit == null) continue;
                    await FillUnitAsync(unit, template, rule, visit.VisitType);
                    var scope = PmVisitValidator.BuildScope(unit.Label ?? $"Unit {unit.Sequence}", unit.PmvuId, unit.Sections, answers, site.ByCode);
                    result.Blocking.AddRange(PmVisitValidator.Check(scope).Select(ToItem));
                }
                if (visit.Units.Count == 0)
                    result.Blocking.Add(new PmVisitValidatorItemDto { Scope = "Assets", Code = "UNITS", Question = "No units are on this visit" });

                var formRequired = !string.IsNullOrWhiteSpace(visit.CustomerForm) && !string.Equals(visit.CustomerForm, "None", StringComparison.OrdinalIgnoreCase);
                if (formRequired && request.CustomerFormCompleted != true)
                    result.Blocking.Add(new PmVisitValidatorItemDto { Scope = "Checkout", Code = "FORM", Question = "Confirm the customer's own form was completed" });

                if (result.Blocking.Count > 0)
                {
                    result.Submitted = false;
                    return Ok(Ok(result, $"{result.Blocking.Count} item(s) must be completed before submitting"));
                }

                // checkout answers -> visit columns
                string? A(string code) => checkout.ByCode.TryGetValue(code, out var a) ? a.Answer : null;
                string? S(string code) => site.ByCode.TryGetValue(code, out var a) ? a.Answer : null;
                var fullScope = string.Equals(A("F030"), "Yes", StringComparison.OrdinalIgnoreCase) ? true : string.Equals(A("F030"), "No", StringComparison.OrdinalIgnoreCase) ? false : (bool?)null;
                var status = fullScope == false ? "Incomplete" : "Submitted";
                result.SsCode = fullScope == false ? "Incomplete-7" : "Complete-3";

                // proposal ticket for the findings that need a quote
                var findings = await _visits.GetFindingsAsync(visit.PmvId);
                var toQuote = findings.Where(f => f.QuoteRequired && !f.SrIdProposal.HasValue).ToList();
                var existingProposal = findings.FirstOrDefault(f => f.SrIdProposal.HasValue);
                if (toQuote.Count > 0)
                {
                    if (existingProposal != null)
                    {
                        await _visits.LinkFindingsToProposalAsync(visit.PmvId, existingProposal.SrIdProposal!.Value);
                        result.ProposalSrId = existingProposal.SrIdProposal;
                        result.ProposalRequestNumber = existingProposal.ProposalRequestNumber;
                        result.ProposalNote = "Added to the proposal ticket created earlier";
                    }
                    else
                    {
                        var basics = await _visits.GetSrBasicsAsync(srId);
                        if (basics != null)
                        {
                            var tradeId = await _visits.GetProposalTradeAsync(basics.XcccId, basics.TIdParent);
                            var note = new System.Text.StringBuilder();
                            note.AppendLine($"Proposal from PM visit {basics.RequestNumber} at {header!.Location} on {DateTime.Now:MM/dd/yyyy} by {UserFullName}.");
                            if (!tradeId.HasValue) note.AppendLine("NOTE: the company has no non-PM trade with a labor rate under this parent trade; the ticket was created under the PM trade. Please move it to the right trade.");
                            var n = 0;
                            foreach (var f in toQuote)
                            {
                                n++;
                                var parts = new[] { f.Severity, f.Action, string.IsNullOrWhiteSpace(f.Parts) ? null : "parts: " + f.Parts, string.IsNullOrWhiteSpace(f.LaborEstimate) ? null : "labor: " + f.LaborEstimate }.Where(x => !string.IsNullOrWhiteSpace(x));
                                note.AppendLine($"{n}. {(string.IsNullOrWhiteSpace(f.UnitLabel) ? "Site" : f.UnitLabel)}{(string.IsNullOrWhiteSpace(f.Component) ? "" : " / " + f.Component)}: {f.Description} ({string.Join("; ", parts)})");
                            }
                            var proposal = await _dataService.InsertServiceRequestAsync(new CreateServiceRequestRequest
                            {
                                XcccId = basics.XcccId,
                                LId = basics.LId,
                                TId = tradeId ?? basics.TId,
                                PId = basics.PId,
                                LrtId = 1,
                                SsId = 10,   // Needs to be quoted (Incomplete-2)
                                SrSummary = $"Proposal - PM findings - {basics.RequestNumber}",
                                SrRequestNumber = basics.RequestNumber + "-Quote",
                                WoWorkOrderNumber = basics.RequestNumber + "-Quote-1",
                                SrIvrRequestNumber = basics.IvrRequestNumber,
                                SrCallNote = note.ToString(),
                                SrOfficeNote = $"Auto-created from PM visit {basics.RequestNumber}: {toQuote.Count} finding(s) need a quote.",
                                SrNte = 0,
                                SrTripChargeWorked = basics.TripCharge,
                                SrSiteContactName = basics.SiteContactName,
                                SrSiteContactPhone = basics.SiteContactPhone,
                                SrSiteContactEmail = basics.SiteContactEmail,
                                SvtId = await _visits.GetServiceTypeIdAsync("Proposal")
                            }, UserId);
                            await _visits.SetWorkOrderStatusByCodeAsync(proposal.SrId, "Incomplete-2");
                            await _visits.LinkFindingsToProposalAsync(visit.PmvId, proposal.SrId);
                            result.ProposalSrId = proposal.SrId;
                            result.ProposalRequestNumber = proposal.SrRequestNumber;
                            result.ProposalNote = tradeId.HasValue ? "Proposal ticket created for the office to quote" : "Proposal ticket created under the PM trade (no repair trade with a rate on this company); the office must move it";
                        }
                    }
                }

                await _visits.SubmitAsync(visit.PmvId, status, fullScope, A("F031"), A("F032"), A("F033"), A("F201"), formRequired ? request.CustomerFormCompleted : null, A("T084"), S("F023"), S("F024"), UserId > 0 ? UserId : null);
                result.Submitted = true;
                result.Status = status;

                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync($"PM Visit Submitted - ID: {visit.PmvId} - SR {srId} - {status}", null,
                    new Dictionary<string, object?> { ["Status"] = status, ["FullScopeCompleted"] = fullScope, ["SsCode"] = result.SsCode, ["ProposalSrId"] = result.ProposalSrId, ["Units"] = visit.Units.Count, ["Findings"] = findings.Count },
                    stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                await LogAuditAsync("PmVisitSubmit", new { srId, visit.PmvId, status, result.SsCode, result.ProposalSrId });
                return Ok(Ok(result, status == "Submitted" ? "Visit submitted" : "Visit submitted as incomplete"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("PmVisitSubmit", ex, new { srId, request });
                return Fail<PmVisitSubmitResultDto>(500, "Failed to submit the visit");
            }
        }

        private static PmVisitValidatorItemDto ToItem(PmVisitValidator.Unresolved u) => new() { Scope = u.Scope, PmvuId = u.PmvuId, Code = u.Code, Question = u.Question, RepeatIndex = u.RepeatIndex };

        #endregion

        private static string? ValidateAsset(SaveAssetRequest r)
        {
            if (r.AscId <= 0) return "Equipment type is required";
            if (r.CapacityTons is < 0 or > 9999) return "Tonnage must be between 0 and 9999";
            if (r.ManufactureYear is < 1900 or > 2100) return "Manufacture year must be a four-digit year";
            if (r.Components.Any(c => c.Quantity is < 0)) return "Component quantities cannot be negative";
            return null;
        }

        /// <summary>The asset as a save request so "confirm" can re-save it unchanged with the verification stamp.</summary>
        private static SaveAssetRequest ToSaveRequest(AssetDto a, bool markVerified) => new()
        {
            AscId = a.AscId, AsmId = a.AsmId, Manufacturer = a.AsmId.HasValue ? null : a.Manufacturer, ModelNumber = a.ModelNumber, SerialNumber = a.SerialNumber,
            Description = a.Description, SpecialInstructions = a.SpecialInstructions, UnitTag = a.UnitTag, AssetTag = a.AssetTag, AsIdConnected = a.AsIdConnected,
            CapacityTons = a.CapacityTons, HeatingType = a.HeatingType, RefrigerantType = a.RefrigerantType, ManufactureYear = a.ManufactureYear, InstallDate = a.InstallDate,
            ServedArea = a.ServedArea, AmlId = a.AmlId, Active = a.Active, MarkVerified = markVerified,
            Components = a.Components.Where(c => c.Active).Select(c => new SaveAssetComponentRequest { AsctId = c.AsctId, Quantity = c.Quantity, Size = c.Size, ComponentType = c.ComponentType, PartNumber = c.PartNumber, Note = c.Note, Active = true }).ToList(),
            Attributes = a.Attributes.Select(x => new SaveAssetAttributeRequest { AsatId = x.AsatId, Value = x.Value }).ToList()
        };
    }
}
