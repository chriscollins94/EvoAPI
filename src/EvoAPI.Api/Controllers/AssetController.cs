using System.Diagnostics;
using EvoAPI.Core.Interfaces;
using EvoAPI.Core.Services;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using Microsoft.AspNetCore.Mvc;

namespace EvoAPI.Api.Controllers
{
    /// <summary>
    /// Asset and location layer (PM build slice 2).
    ///
    /// Definitions per parent trade (Settings > Assets): equipment types (AssetCategory), manufacturers per type,
    /// component types (filters, belts) and attribute types (the yes/no features, counts and facts that template
    /// conditions and repeat groups read). Two org-wide lookups: asset mount locations and location access requirements.
    ///
    /// Per location (Company Admin > LOCATIONS): the trade profile (shown as the PM profile) per parent trade and the assets at the location with
    /// their components and attributes. Asset and profile saves go to the critical audit log with before/after values.
    ///
    /// Definition routes live under EvoApi/assets; location routes use absolute /EvoApi/locations/{lId}/... so they
    /// sit beside the existing location endpoints. Admin only, behind the PreventativeMaintenance flag in the UI.
    /// </summary>
    [ApiController]
    [Route("EvoApi/assets")]
    [AdminOnly]
    public class AssetController : BaseController
    {
        private static readonly string[] DataTypes = { "Bool", "Number", "Text", "List" };
        private static readonly string[] RepeatKeys = { "Circuit", "Compressor", "HeatStage", "Phase", "FilterBank", "BeltDrive" };

        private readonly IAssetRepository _repo;
        private readonly IAuditCriticalService _auditCriticalService;

        public AssetController(IAssetRepository repo, IAuditService auditService, IAuditCriticalService auditCriticalService)
        {
            _repo = repo;
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

        #region definitions per parent trade

        [HttpGet("trades")]
        public async Task<ActionResult<ApiResponse<List<AssetTradeDto>>>> GetTrades()
        {
            try
            {
                var trades = await _repo.GetTradesAsync();
                return Ok(Ok(trades, $"Retrieved {trades.Count} parent trades"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetGetTrades", ex);
                return Fail<List<AssetTradeDto>>(500, "Failed to retrieve parent trades");
            }
        }

        [HttpGet("trades/{tId:int}/categories")]
        public async Task<ActionResult<ApiResponse<List<AssetCategoryDto>>>> GetCategories(int tId)
        {
            try
            {
                var rows = await _repo.GetCategoriesAsync(tId);
                return Ok(Ok(rows, $"Retrieved {rows.Count} equipment types"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetGetCategories", ex, new { tId });
                return Fail<List<AssetCategoryDto>>(500, "Failed to retrieve equipment types");
            }
        }

        [HttpPost("trades/{tId:int}/categories")]
        public async Task<ActionResult<ApiResponse<AssetCategoryDto>>> CreateCategory(int tId, [FromBody] SaveAssetCategoryRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Category)) return Fail<AssetCategoryDto>(400, "Equipment type name is required");
            if (request.Category.Trim().Length > 50) return Fail<AssetCategoryDto>(400, "Equipment type name must be 50 characters or fewer");
            try
            {
                var existing = await _repo.GetCategoriesAsync(tId);
                if (existing.Any(c => string.Equals(c.Category, request.Category.Trim(), StringComparison.OrdinalIgnoreCase)))
                    return Fail<AssetCategoryDto>(409, $"Equipment type '{request.Category.Trim()}' already exists for this trade");
                var id = await _repo.CreateCategoryAsync(tId, request);
                await LogAuditAsync("AssetCreateCategory", new { tId, id, request.Category });
                return Ok(Ok((await _repo.GetCategoryAsync(id))!, "Equipment type created"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetCreateCategory", ex, new { tId, request });
                return Fail<AssetCategoryDto>(500, "Failed to create the equipment type");
            }
        }

        [HttpPut("categories/{ascId:int}")]
        public async Task<ActionResult<ApiResponse<AssetCategoryDto>>> UpdateCategory(int ascId, [FromBody] SaveAssetCategoryRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Category)) return Fail<AssetCategoryDto>(400, "Equipment type name is required");
            if (request.Category.Trim().Length > 50) return Fail<AssetCategoryDto>(400, "Equipment type name must be 50 characters or fewer");
            try
            {
                var before = await _repo.GetCategoryAsync(ascId);
                if (before == null) return Fail<AssetCategoryDto>(404, "Equipment type not found");
                var siblings = await _repo.GetCategoriesAsync(before.TId);
                if (siblings.Any(c => c.AscId != ascId && string.Equals(c.Category, request.Category.Trim(), StringComparison.OrdinalIgnoreCase)))
                    return Fail<AssetCategoryDto>(409, $"Equipment type '{request.Category.Trim()}' already exists for this trade");
                await _repo.UpdateCategoryAsync(ascId, request);
                await LogAuditAsync("AssetUpdateCategory", new { ascId, request.Category });
                return Ok(Ok((await _repo.GetCategoryAsync(ascId))!, "Equipment type updated"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetUpdateCategory", ex, new { ascId, request });
                return Fail<AssetCategoryDto>(500, "Failed to update the equipment type");
            }
        }

        [HttpGet("categories/{ascId:int}/manufacturers")]
        public async Task<ActionResult<ApiResponse<List<AssetManufacturerDto>>>> GetManufacturers(int ascId)
        {
            try
            {
                var rows = await _repo.GetManufacturersAsync(ascId);
                return Ok(Ok(rows, $"Retrieved {rows.Count} manufacturers"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetGetManufacturers", ex, new { ascId });
                return Fail<List<AssetManufacturerDto>>(500, "Failed to retrieve manufacturers");
            }
        }

        [HttpPost("categories/{ascId:int}/manufacturers")]
        public async Task<ActionResult<ApiResponse<AssetManufacturerDto>>> CreateManufacturer(int ascId, [FromBody] SaveAssetManufacturerRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Manufacturer)) return Fail<AssetManufacturerDto>(400, "Manufacturer name is required");
            if (request.Manufacturer.Trim().Length > 50) return Fail<AssetManufacturerDto>(400, "Manufacturer name must be 50 characters or fewer");
            try
            {
                if (await _repo.GetCategoryAsync(ascId) == null) return Fail<AssetManufacturerDto>(404, "Equipment type not found");
                var existing = await _repo.GetManufacturersAsync(ascId);
                if (existing.Any(m => string.Equals(m.Manufacturer, request.Manufacturer.Trim(), StringComparison.OrdinalIgnoreCase)))
                    return Fail<AssetManufacturerDto>(409, $"'{request.Manufacturer.Trim()}' is already in this equipment type's manufacturer list");
                var id = await _repo.CreateManufacturerAsync(ascId, request.Manufacturer);
                await LogAuditAsync("AssetCreateManufacturer", new { ascId, id, request.Manufacturer });
                return Ok(Ok((await _repo.GetManufacturerAsync(id))!, "Manufacturer added"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetCreateManufacturer", ex, new { ascId, request });
                return Fail<AssetManufacturerDto>(500, "Failed to add the manufacturer");
            }
        }

        [HttpPut("manufacturers/{asmId:int}")]
        public async Task<ActionResult<ApiResponse<AssetManufacturerDto>>> UpdateManufacturer(int asmId, [FromBody] SaveAssetManufacturerRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Manufacturer)) return Fail<AssetManufacturerDto>(400, "Manufacturer name is required");
            if (request.Manufacturer.Trim().Length > 50) return Fail<AssetManufacturerDto>(400, "Manufacturer name must be 50 characters or fewer");
            try
            {
                var before = await _repo.GetManufacturerAsync(asmId);
                if (before == null) return Fail<AssetManufacturerDto>(404, "Manufacturer not found");
                await _repo.UpdateManufacturerAsync(asmId, request.Manufacturer);
                await LogAuditAsync("AssetUpdateManufacturer", new { asmId, from = before.Manufacturer, to = request.Manufacturer });
                return Ok(Ok((await _repo.GetManufacturerAsync(asmId))!, "Manufacturer updated"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetUpdateManufacturer", ex, new { asmId, request });
                return Fail<AssetManufacturerDto>(500, "Failed to update the manufacturer");
            }
        }

        [HttpDelete("manufacturers/{asmId:int}")]
        public async Task<ActionResult<ApiResponse<bool>>> DeleteManufacturer(int asmId)
        {
            try
            {
                var result = await _repo.DeleteManufacturerAsync(asmId);
                if (result == FormDeleteResult.NotFound) return Fail<bool>(404, "Manufacturer not found");
                if (result == FormDeleteResult.Referenced) return Fail<bool>(409, "Assets use this manufacturer; it cannot be removed");
                await LogAuditAsync("AssetDeleteManufacturer", new { asmId });
                return Ok(Ok(true, "Manufacturer removed"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetDeleteManufacturer", ex, new { asmId });
                return Fail<bool>(500, "Failed to remove the manufacturer");
            }
        }

        [HttpGet("trades/{tId:int}/component-types")]
        public async Task<ActionResult<ApiResponse<List<AssetComponentTypeDto>>>> GetComponentTypes(int tId)
        {
            try
            {
                var rows = await _repo.GetComponentTypesAsync(tId);
                return Ok(Ok(rows, $"Retrieved {rows.Count} component types"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetGetComponentTypes", ex, new { tId });
                return Fail<List<AssetComponentTypeDto>>(500, "Failed to retrieve component types");
            }
        }

        [HttpPost("trades/{tId:int}/component-types")]
        public async Task<ActionResult<ApiResponse<List<AssetComponentTypeDto>>>> CreateComponentType(int tId, [FromBody] SaveAssetComponentTypeRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Type)) return Fail<List<AssetComponentTypeDto>>(400, "Component type name is required");
            try
            {
                var existing = await _repo.GetComponentTypesAsync(tId);
                if (existing.Any(c => string.Equals(c.Type, request.Type.Trim(), StringComparison.OrdinalIgnoreCase)))
                    return Fail<List<AssetComponentTypeDto>>(409, $"Component type '{request.Type.Trim()}' already exists for this trade");
                var id = await _repo.CreateComponentTypeAsync(tId, request);
                await LogAuditAsync("AssetCreateComponentType", new { tId, id, request.Type });
                return Ok(Ok(await _repo.GetComponentTypesAsync(tId), "Component type added"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetCreateComponentType", ex, new { tId, request });
                return Fail<List<AssetComponentTypeDto>>(500, "Failed to add the component type");
            }
        }

        [HttpPut("component-types/{asctId:int}")]
        public async Task<ActionResult<ApiResponse<bool>>> UpdateComponentType(int asctId, [FromBody] SaveAssetComponentTypeRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Type)) return Fail<bool>(400, "Component type name is required");
            try
            {
                var ok = await _repo.UpdateComponentTypeAsync(asctId, request);
                if (!ok) return Fail<bool>(404, "Component type not found");
                await LogAuditAsync("AssetUpdateComponentType", new { asctId, request.Type });
                return Ok(Ok(true, "Component type updated"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetUpdateComponentType", ex, new { asctId, request });
                return Fail<bool>(500, "Failed to update the component type");
            }
        }

        [HttpGet("trades/{tId:int}/attribute-types")]
        public async Task<ActionResult<ApiResponse<List<AssetAttributeTypeDto>>>> GetAttributeTypes(int tId)
        {
            try
            {
                var rows = await _repo.GetAttributeTypesAsync(tId);
                return Ok(Ok(rows, $"Retrieved {rows.Count} attribute types"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetGetAttributeTypes", ex, new { tId });
                return Fail<List<AssetAttributeTypeDto>>(500, "Failed to retrieve attribute types");
            }
        }

        private static string? ValidateAttributeType(SaveAssetAttributeTypeRequest r)
        {
            if (string.IsNullOrWhiteSpace(r.Key)) return "Attribute key is required";
            var key = r.Key.Trim();
            if (key.Length > 40 || !key.All(ch => char.IsLetterOrDigit(ch) || ch == '_')) return "Attribute key must be letters, digits or underscore, up to 40 characters (it is used inside condition JSON)";
            if (string.IsNullOrWhiteSpace(r.Label)) return "Attribute label is required";
            if (!DataTypes.Contains(r.DataType)) return "Data type must be Bool, Number, Text or List";
            if (r.DataType == "List" && string.IsNullOrWhiteSpace(r.Values)) return "A List attribute needs its values (semicolon separated)";
            if (!string.IsNullOrWhiteSpace(r.RepeatKey) && !RepeatKeys.Contains(r.RepeatKey)) return "Repeat key must be one of " + string.Join(", ", RepeatKeys);
            if (!string.IsNullOrWhiteSpace(r.RepeatKey) && r.DataType != "Number") return "Only a Number attribute can count a repeat group";
            return null;
        }

        [HttpPost("trades/{tId:int}/attribute-types")]
        public async Task<ActionResult<ApiResponse<List<AssetAttributeTypeDto>>>> CreateAttributeType(int tId, [FromBody] SaveAssetAttributeTypeRequest request)
        {
            var err = ValidateAttributeType(request);
            if (err != null) return Fail<List<AssetAttributeTypeDto>>(400, err);
            try
            {
                var existing = await _repo.GetAttributeTypesAsync(tId);
                if (existing.Any(a => string.Equals(a.Key, request.Key.Trim(), StringComparison.OrdinalIgnoreCase)))
                    return Fail<List<AssetAttributeTypeDto>>(409, $"Attribute '{request.Key.Trim()}' already exists for this trade");
                if (!string.IsNullOrWhiteSpace(request.RepeatKey) && existing.Any(a => a.Active && string.Equals(a.RepeatKey, request.RepeatKey, StringComparison.OrdinalIgnoreCase)))
                    return Fail<List<AssetAttributeTypeDto>>(409, $"Another attribute already counts the {request.RepeatKey} repeat group");
                var id = await _repo.CreateAttributeTypeAsync(tId, request);
                await LogAuditAsync("AssetCreateAttributeType", new { tId, id, request.Key });
                return Ok(Ok(await _repo.GetAttributeTypesAsync(tId), "Attribute added"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetCreateAttributeType", ex, new { tId, request });
                return Fail<List<AssetAttributeTypeDto>>(500, "Failed to add the attribute");
            }
        }

        [HttpPut("attribute-types/{asatId:int}")]
        public async Task<ActionResult<ApiResponse<bool>>> UpdateAttributeType(int asatId, [FromBody] SaveAssetAttributeTypeRequest request)
        {
            var err = ValidateAttributeType(request);
            if (err != null) return Fail<bool>(400, err);
            try
            {
                var before = await _repo.GetAttributeTypeAsync(asatId);
                if (before == null) return Fail<bool>(404, "Attribute not found");
                if (before.UsageCount > 0 && !string.Equals(before.Key, request.Key.Trim(), StringComparison.Ordinal))
                    return Fail<bool>(409, $"'{before.Key}' is stored on {before.UsageCount} asset(s) and may be referenced by template conditions; the key cannot be renamed. Change the label instead.");
                var siblings = await _repo.GetAttributeTypesAsync(before.TId);
                if (siblings.Any(a => a.AsatId != asatId && string.Equals(a.Key, request.Key.Trim(), StringComparison.OrdinalIgnoreCase)))
                    return Fail<bool>(409, $"Attribute '{request.Key.Trim()}' already exists for this trade");
                if (!string.IsNullOrWhiteSpace(request.RepeatKey) && request.Active && siblings.Any(a => a.AsatId != asatId && a.Active && string.Equals(a.RepeatKey, request.RepeatKey, StringComparison.OrdinalIgnoreCase)))
                    return Fail<bool>(409, $"Another attribute already counts the {request.RepeatKey} repeat group");
                await _repo.UpdateAttributeTypeAsync(asatId, request);
                await LogAuditAsync("AssetUpdateAttributeType", new { asatId, request.Key });
                return Ok(Ok(true, "Attribute updated"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetUpdateAttributeType", ex, new { asatId, request });
                return Fail<bool>(500, "Failed to update the attribute");
            }
        }

        #endregion

        #region mount location and access requirement lookups

        [HttpGet("lookups/mount-locations")]
        public async Task<ActionResult<ApiResponse<List<AssetMountLocationDto>>>> GetMountLocations()
        {
            try { var rows = await _repo.GetMountLocationsAsync(); return Ok(Ok(rows, $"Retrieved {rows.Count} mount locations")); }
            catch (Exception ex) { await LogAuditErrorAsync("AssetGetMountLocations", ex); return Fail<List<AssetMountLocationDto>>(500, "Failed to retrieve mount locations"); }
        }

        [HttpPost("lookups/mount-locations")]
        public async Task<ActionResult<ApiResponse<List<AssetMountLocationDto>>>> CreateMountLocation([FromBody] SaveAssetMountLocationRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Location)) return Fail<List<AssetMountLocationDto>>(400, "Name is required");
            try
            {
                var existing = await _repo.GetMountLocationsAsync();
                if (existing.Any(u => string.Equals(u.Location, request.Location.Trim(), StringComparison.OrdinalIgnoreCase))) return Fail<List<AssetMountLocationDto>>(409, "That mount location already exists");
                await _repo.CreateMountLocationAsync(request);
                await LogAuditAsync("AssetCreateMountLocation", new { request.Location });
                return Ok(Ok(await _repo.GetMountLocationsAsync(), "Mount location added"));
            }
            catch (Exception ex) { await LogAuditErrorAsync("AssetCreateMountLocation", ex, request); return Fail<List<AssetMountLocationDto>>(500, "Failed to add the mount location"); }
        }

        [HttpPut("lookups/mount-locations/{pmulId:int}")]
        public async Task<ActionResult<ApiResponse<bool>>> UpdateMountLocation(int pmulId, [FromBody] SaveAssetMountLocationRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Location)) return Fail<bool>(400, "Name is required");
            try
            {
                var existing = await _repo.GetMountLocationsAsync();
                if (existing.Any(u => u.AmlId != pmulId && string.Equals(u.Location, request.Location.Trim(), StringComparison.OrdinalIgnoreCase))) return Fail<bool>(409, "That mount location already exists");
                if (!await _repo.UpdateMountLocationAsync(pmulId, request)) return Fail<bool>(404, "Mount location not found");
                await LogAuditAsync("AssetUpdateMountLocation", new { pmulId, request.Location });
                return Ok(Ok(true, "Mount location updated"));
            }
            catch (Exception ex) { await LogAuditErrorAsync("AssetUpdateMountLocation", ex, new { pmulId, request }); return Fail<bool>(500, "Failed to update the mount location"); }
        }

        [HttpGet("lookups/access-requirements")]
        public async Task<ActionResult<ApiResponse<List<LocationAccessRequirementDto>>>> GetAccessRequirements()
        {
            try { var rows = await _repo.GetAccessRequirementsAsync(); return Ok(Ok(rows, $"Retrieved {rows.Count} access requirements")); }
            catch (Exception ex) { await LogAuditErrorAsync("AssetGetAccessRequirements", ex); return Fail<List<LocationAccessRequirementDto>>(500, "Failed to retrieve access requirements"); }
        }

        [HttpPost("lookups/access-requirements")]
        public async Task<ActionResult<ApiResponse<List<LocationAccessRequirementDto>>>> CreateAccessRequirement([FromBody] SaveLocationAccessRequirementRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Requirement)) return Fail<List<LocationAccessRequirementDto>>(400, "Name is required");
            try
            {
                var existing = await _repo.GetAccessRequirementsAsync();
                if (existing.Any(u => string.Equals(u.Requirement, request.Requirement.Trim(), StringComparison.OrdinalIgnoreCase))) return Fail<List<LocationAccessRequirementDto>>(409, "That access requirement already exists");
                await _repo.CreateAccessRequirementAsync(request);
                await LogAuditAsync("AssetCreateAccessRequirement", new { request.Requirement });
                return Ok(Ok(await _repo.GetAccessRequirementsAsync(), "Access requirement added"));
            }
            catch (Exception ex) { await LogAuditErrorAsync("AssetCreateAccessRequirement", ex, request); return Fail<List<LocationAccessRequirementDto>>(500, "Failed to add the access requirement"); }
        }

        [HttpPut("lookups/access-requirements/{pmarId:int}")]
        public async Task<ActionResult<ApiResponse<bool>>> UpdateAccessRequirement(int pmarId, [FromBody] SaveLocationAccessRequirementRequest request)
        {
            if (string.IsNullOrWhiteSpace(request.Requirement)) return Fail<bool>(400, "Name is required");
            try
            {
                var existing = await _repo.GetAccessRequirementsAsync();
                if (existing.Any(u => u.LarId != pmarId && string.Equals(u.Requirement, request.Requirement.Trim(), StringComparison.OrdinalIgnoreCase))) return Fail<bool>(409, "That access requirement already exists");
                if (!await _repo.UpdateAccessRequirementAsync(pmarId, request)) return Fail<bool>(404, "Access requirement not found");
                await LogAuditAsync("AssetUpdateAccessRequirement", new { pmarId, request.Requirement });
                return Ok(Ok(true, "Access requirement updated"));
            }
            catch (Exception ex) { await LogAuditErrorAsync("AssetUpdateAccessRequirement", ex, new { pmarId, request }); return Fail<bool>(500, "Failed to update the access requirement"); }
        }

        #endregion

        #region location trade profile

        [HttpGet("/EvoApi/locations/{lId:int}/trade-profiles/{tId:int}")]
        public async Task<ActionResult<ApiResponse<LocationTradeProfileDto>>> GetProfile(int lId, int tId)
        {
            try
            {
                var profile = await _repo.GetProfileAsync(lId, tId);
                if (profile == null) return Fail<LocationTradeProfileDto>(404, "No PM profile for this location and trade yet");
                return Ok(Ok(profile, "PM profile retrieved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetGetProfile", ex, new { lId, tId });
                return Fail<LocationTradeProfileDto>(500, "Failed to retrieve the PM profile");
            }
        }

        [HttpPut("/EvoApi/locations/{lId:int}/trade-profiles/{tId:int}")]
        public async Task<ActionResult<ApiResponse<LocationTradeProfileDto>>> SaveProfile(int lId, int tId, [FromBody] SaveLocationTradeProfileRequest request)
        {
            if (request.UnitCount is < 0 or > 999) return Fail<LocationTradeProfileDto>(400, "Unit count must be between 0 and 999");
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (!await _repo.LocationExistsAsync(lId)) return Fail<LocationTradeProfileDto>(404, "Location not found");
                if (!string.IsNullOrWhiteSpace(request.AccessRequirements) && string.IsNullOrWhiteSpace(request.AccessNote))
                {
                    var needsNote = (await _repo.GetAccessRequirementsAsync()).Where(a => a.RequiresNote).Select(a => a.LarId.ToString()).ToHashSet();
                    if (request.AccessRequirements.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Any(needsNote.Contains))
                        return Fail<LocationTradeProfileDto>(400, "An access note is required when 'Other' is one of the access requirements");
                }
                var before = await _repo.GetProfileAsync(lId, tId);
                var after = await _repo.UpsertProfileAsync(lId, tId, request);
                var (oldValues, newValues) = ChangeDiff.Diff(before, request);
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync(
                    $"Location Trade Profile {(before == null ? "Created" : "Updated")} - ID: {after.LtpId} - location {lId} - trade {tId}",
                    oldValues, newValues, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                await LogAuditAsync("AssetSaveProfile", new { lId, tId, after.LtpId, changed = newValues.Keys });
                return Ok(Ok(after, "PM profile saved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetSaveProfile", ex, new { lId, tId, request });
                return Fail<LocationTradeProfileDto>(500, "Failed to save the PM profile");
            }
        }

        #endregion

        #region assets at a location

        [HttpGet("/EvoApi/locations/{lId:int}/assets")]
        public async Task<ActionResult<ApiResponse<List<AssetDto>>>> GetLocationAssets(int lId, [FromQuery] int? tId, [FromQuery] bool includeInactive = false)
        {
            try
            {
                var rows = await _repo.GetLocationAssetsAsync(lId, tId, includeInactive);
                return Ok(Ok(rows, $"Retrieved {rows.Count} assets"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetGetLocationAssets", ex, new { lId, tId });
                return Fail<List<AssetDto>>(500, "Failed to retrieve the location's assets");
            }
        }

        [HttpGet("{asId:int}")]
        public async Task<ActionResult<ApiResponse<AssetDto>>> GetAsset(int asId)
        {
            try
            {
                var asset = await _repo.GetAssetAsync(asId);
                if (asset == null) return Fail<AssetDto>(404, "Asset not found");
                return Ok(Ok(asset, "Asset retrieved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetGet", ex, new { asId });
                return Fail<AssetDto>(500, "Failed to retrieve the asset");
            }
        }

        private static string? ValidateAsset(SaveAssetRequest r)
        {
            if (r.AscId <= 0) return "Equipment type is required";
            if (r.CapacityTons is < 0 or > 9999) return "Tonnage must be between 0 and 9999";
            if (r.ManufactureYear is < 1900 or > 2100) return "Manufacture year must be a four-digit year";
            if (r.Components.Any(c => c.Quantity is < 0)) return "Component quantities cannot be negative";
            return null;
        }

        [HttpPost("/EvoApi/locations/{lId:int}/assets")]
        public async Task<ActionResult<ApiResponse<AssetDto>>> CreateAsset(int lId, [FromBody] SaveAssetRequest request)
        {
            var err = ValidateAsset(request);
            if (err != null) return Fail<AssetDto>(400, err);
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (!await _repo.LocationExistsAsync(lId)) return Fail<AssetDto>(404, "Location not found");
                if (await _repo.GetCategoryAsync(request.AscId) == null) return Fail<AssetDto>(400, "Equipment type not found");
                var asId = await _repo.CreateAssetAsync(lId, request, UserId > 0 ? UserId : null);
                var after = (await _repo.GetAssetAsync(asId))!;
                var (oldValues, newValues) = ChangeDiff.Diff(null, request);
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync($"Asset Created - ID: {asId} - {after.Category} {after.UnitTag} - location {lId}", oldValues, newValues, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                await LogAuditAsync("AssetCreate", new { lId, asId, request.AscId, request.UnitTag });
                return Ok(Ok(after, "Asset created"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetCreate", ex, new { lId, request });
                return Fail<AssetDto>(500, "Failed to create the asset");
            }
        }

        [HttpPut("{asId:int}")]
        public async Task<ActionResult<ApiResponse<AssetDto>>> UpdateAsset(int asId, [FromBody] SaveAssetRequest request)
        {
            var err = ValidateAsset(request);
            if (err != null) return Fail<AssetDto>(400, err);
            if (request.AsIdConnected == asId) return Fail<AssetDto>(400, "A unit cannot be connected to itself");
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var before = await _repo.GetAssetAsync(asId);
                if (before == null) return Fail<AssetDto>(404, "Asset not found");
                if (await _repo.GetCategoryAsync(request.AscId) == null) return Fail<AssetDto>(400, "Equipment type not found");
                await _repo.UpdateAssetAsync(asId, request, UserId > 0 ? UserId : null);
                var after = (await _repo.GetAssetAsync(asId))!;
                var (oldValues, newValues) = ChangeDiff.Diff(before, request);
                DiffChildren(before, after, oldValues, newValues);
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync($"Asset Updated - ID: {asId} - {after.Category} {after.UnitTag} - location {after.LId}", oldValues, newValues, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                await LogAuditAsync("AssetUpdate", new { asId, changed = newValues.Keys });
                return Ok(Ok(after, "Asset saved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("AssetUpdate", ex, new { asId, request });
                return Fail<AssetDto>(500, "Failed to save the asset");
            }
        }

        /// <summary>Components and attributes are compared as "Type: value" summaries so the audit row stays readable.</summary>
        private static void DiffChildren(AssetDto before, AssetDto after, Dictionary<string, object?> oldValues, Dictionary<string, object?> newValues)
        {
            string Comp(AssetDto a) => string.Join("; ", a.Components.Where(c => c.Active).Select(c => $"{c.Type}: {c.Quantity?.ToString() ?? "-"} x {c.Size ?? "-"}{(string.IsNullOrWhiteSpace(c.ComponentType) ? "" : " " + c.ComponentType)}"));
            string Attr(AssetDto a) => string.Join("; ", a.Attributes.Select(x => $"{x.Key}={x.Value}"));
            var (c1, c2) = (Comp(before), Comp(after));
            if (c1 != c2) { oldValues["Components"] = c1; newValues["Components"] = c2; }
            var (a1, a2) = (Attr(before), Attr(after));
            if (a1 != a2) { oldValues["Attributes"] = a1; newValues["Attributes"] = a2; }
        }

        #endregion
    }
}
