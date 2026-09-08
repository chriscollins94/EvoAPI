using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

/// <summary>
/// Asset and location layer (PM build slice 2): per-trade definitions (equipment types, manufacturers, component
/// types, attribute types), the mount-location and access-requirement lookups, a location's trade profile, the assets at a location, and the facts
/// the form resolver reads from one asset.
/// </summary>
public interface IAssetRepository
{
    // definitions per parent trade
    Task<List<AssetTradeDto>> GetTradesAsync();
    Task<List<AssetCategoryDto>> GetCategoriesAsync(int tId);
    Task<AssetCategoryDto?> GetCategoryAsync(int ascId);
    Task<int> CreateCategoryAsync(int tId, SaveAssetCategoryRequest request);
    Task<bool> UpdateCategoryAsync(int ascId, SaveAssetCategoryRequest request);
    Task<List<AssetManufacturerDto>> GetManufacturersAsync(int ascId);
    Task<AssetManufacturerDto?> GetManufacturerAsync(int asmId);
    Task<int> CreateManufacturerAsync(int ascId, string manufacturer);
    Task<bool> UpdateManufacturerAsync(int asmId, string manufacturer);
    Task<FormDeleteResult> DeleteManufacturerAsync(int asmId);
    Task<List<AssetComponentTypeDto>> GetComponentTypesAsync(int tId);
    Task<int> CreateComponentTypeAsync(int tId, SaveAssetComponentTypeRequest request);
    Task<bool> UpdateComponentTypeAsync(int asctId, SaveAssetComponentTypeRequest request);
    Task<List<AssetAttributeTypeDto>> GetAttributeTypesAsync(int tId);
    Task<AssetAttributeTypeDto?> GetAttributeTypeAsync(int asatId);
    Task<int> CreateAttributeTypeAsync(int tId, SaveAssetAttributeTypeRequest request);
    Task<bool> UpdateAttributeTypeAsync(int asatId, SaveAssetAttributeTypeRequest request);

    // mount location and access requirement lookups (org-wide)
    Task<List<AssetMountLocationDto>> GetMountLocationsAsync();
    Task<int> CreateMountLocationAsync(SaveAssetMountLocationRequest request);
    Task<bool> UpdateMountLocationAsync(int pmulId, SaveAssetMountLocationRequest request);
    Task<List<LocationAccessRequirementDto>> GetAccessRequirementsAsync();
    Task<int> CreateAccessRequirementAsync(SaveLocationAccessRequirementRequest request);
    Task<bool> UpdateAccessRequirementAsync(int pmarId, SaveLocationAccessRequirementRequest request);

    // location trade profile (the PM profile on the LOCATIONS tab)
    Task<LocationTradeProfileDto?> GetProfileAsync(int lId, int tId);
    Task<LocationTradeProfileDto> UpsertProfileAsync(int lId, int tId, SaveLocationTradeProfileRequest request);

    // assets at a location
    Task<bool> LocationExistsAsync(int lId);
    Task<List<AssetDto>> GetLocationAssetsAsync(int lId, int? tId, bool includeInactive);
    Task<AssetDto?> GetAssetAsync(int asId);
    Task<int> CreateAssetAsync(int lId, SaveAssetRequest request, int? userId);
    Task<bool> UpdateAssetAsync(int asId, SaveAssetRequest request, int? userId);

    // facts for the resolver
    Task<AssetFactsDto?> GetAssetFactsAsync(int asId);
}
