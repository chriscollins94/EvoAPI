namespace EvoAPI.Shared.DTOs;

// Asset and location layer (PM build slice 2). Tables from create_asset_tables.sql.
// Settings > Assets manages the per-trade definitions (equipment types = AssetCategory, manufacturers, component
// types, attribute types) and the two location/asset lookups (mount locations, access requirements); Company Admin > LOCATIONS manages a location's PM profile and
// its assets. The FORM RULES preview reads an asset's facts through AssetFactsDto.

#region definitions per parent trade

public class AssetTradeDto
{
    public int TId { get; set; }
    public string Trade { get; set; } = string.Empty;
    public int CategoryCount { get; set; }
    public int ComponentTypeCount { get; set; }
    public int AttributeTypeCount { get; set; }
    public int AssetCount { get; set; }
}

public class AssetCategoryDto
{
    public int AscId { get; set; }
    public int TId { get; set; }
    public string Category { get; set; } = string.Empty;
    public bool RequireManufacturer { get; set; }
    public bool RequireModelNumber { get; set; }
    public bool RequireSerialNumber { get; set; }
    public bool RequireDescription { get; set; }
    public string? SpecialInstructions { get; set; }
    public int ManufacturerCount { get; set; }
    public int AssetCount { get; set; }
}

public class SaveAssetCategoryRequest
{
    public string Category { get; set; } = string.Empty;
    public bool RequireManufacturer { get; set; } = true;
    public bool RequireModelNumber { get; set; } = true;
    public bool RequireSerialNumber { get; set; } = true;
    public bool RequireDescription { get; set; } = true;
    public string? SpecialInstructions { get; set; }
}

public class AssetManufacturerDto
{
    public int AsmId { get; set; }
    public int AscId { get; set; }
    public string Manufacturer { get; set; } = string.Empty;
    public int AssetCount { get; set; }
}

public class SaveAssetManufacturerRequest
{
    public string Manufacturer { get; set; } = string.Empty;
}

public class AssetComponentTypeDto
{
    public int AsctId { get; set; }
    public int TId { get; set; }
    public string Type { get; set; } = string.Empty;
    public bool HasSize { get; set; }
    public bool HasQuantity { get; set; }
    public bool HasType { get; set; }
    public int Order { get; set; }
    public bool Active { get; set; }
    public int UsageCount { get; set; }
}

public class SaveAssetComponentTypeRequest
{
    public string Type { get; set; } = string.Empty;
    public bool HasSize { get; set; } = true;
    public bool HasQuantity { get; set; } = true;
    public bool HasType { get; set; }
    public int Order { get; set; }
    public bool Active { get; set; } = true;
}

public class AssetAttributeTypeDto
{
    public int AsatId { get; set; }
    public int TId { get; set; }
    public string Key { get; set; } = string.Empty;        // name used in condition JSON, e.g. BeltDriven
    public string Label { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;   // Bool | Number | Text | List
    public string? Values { get; set; }                    // List: semicolon separated
    public string? FqCode { get; set; }                    // template question that writes it
    public string? RepeatKey { get; set; }                 // Number attribute that counts a repeat group
    public int Order { get; set; }
    public bool Active { get; set; }
    public int UsageCount { get; set; }
}

public class SaveAssetAttributeTypeRequest
{
    public string Key { get; set; } = string.Empty;
    public string Label { get; set; } = string.Empty;
    public string DataType { get; set; } = "Bool";
    public string? Values { get; set; }
    public string? FqCode { get; set; }
    public string? RepeatKey { get; set; }
    public int Order { get; set; }
    public bool Active { get; set; } = true;
}

public class AssetMountLocationDto
{
    public int AmlId { get; set; }
    public string Location { get; set; } = string.Empty;
    public int Order { get; set; }
    public bool Active { get; set; }
    public int UsageCount { get; set; }
}

public class SaveAssetMountLocationRequest
{
    public string Location { get; set; } = string.Empty;
    public int Order { get; set; }
    public bool Active { get; set; } = true;
}

public class LocationAccessRequirementDto
{
    public int LarId { get; set; }
    public string Requirement { get; set; } = string.Empty;
    public bool RequiresNote { get; set; }
    public int Order { get; set; }
    public bool Active { get; set; }
}

public class SaveLocationAccessRequirementRequest
{
    public string Requirement { get; set; } = string.Empty;
    public bool RequiresNote { get; set; }
    public int Order { get; set; }
    public bool Active { get; set; } = true;
}

#endregion

#region location trade profile (the PM profile on the LOCATIONS tab)

public class LocationTradeProfileDto
{
    public int LtpId { get; set; }
    public int LId { get; set; }
    public int TId { get; set; }
    public int? UnitCount { get; set; }
    public string? AccessRequirements { get; set; }   // CSV of lar_id
    public string? AccessNote { get; set; }
    public string? MountLocations { get; set; }        // CSV of aml_id
    public int? AttIdAerial { get; set; }
    public string? ManagerName { get; set; }
    public string? ManagerPhone { get; set; }
    public string? Note { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
}

public class SaveLocationTradeProfileRequest
{
    public int? UnitCount { get; set; }
    public string? AccessRequirements { get; set; }
    public string? AccessNote { get; set; }
    public string? MountLocations { get; set; }
    public int? AttIdAerial { get; set; }
    public string? ManagerName { get; set; }
    public string? ManagerPhone { get; set; }
    public string? Note { get; set; }
}

#endregion

#region assets at a location

public class AssetDto
{
    public int AsId { get; set; }
    public int LId { get; set; }
    public int AscId { get; set; }
    public string Category { get; set; } = string.Empty;   // equipment type
    public int? AsmId { get; set; }
    public string? ManufacturerName { get; set; }          // from the manufacturer list
    public string? Manufacturer { get; set; }              // free text (as_manufacturer)
    public string? ModelNumber { get; set; }
    public string? SerialNumber { get; set; }
    public string? Description { get; set; }
    public string? SpecialInstructions { get; set; }
    public string? UnitTag { get; set; }
    public string? AssetTag { get; set; }
    public int? AsIdConnected { get; set; }
    public string? ConnectedUnitTag { get; set; }
    public decimal? CapacityTons { get; set; }
    public string? HeatingType { get; set; }
    public string? RefrigerantType { get; set; }
    public short? ManufactureYear { get; set; }
    public DateTime? InstallDate { get; set; }
    public string? ServedArea { get; set; }
    public int? AmlId { get; set; }
    public string? MountLocation { get; set; }              // mounting name
    public decimal? MapX { get; set; }
    public decimal? MapY { get; set; }
    public bool Active { get; set; }
    public DateTime? LastVerifiedDateTime { get; set; }
    public int? UIdLastVerified { get; set; }
    public int? AttIdNameplate { get; set; }
    public DateTime InsertDateTime { get; set; }
    public DateTime? ModifiedDateTime { get; set; }
    public List<AssetComponentDto> Components { get; set; } = new();
    public List<AssetAttributeDto> Attributes { get; set; } = new();
}

public class AssetComponentDto
{
    public int AscpId { get; set; }
    public int AsId { get; set; }
    public int AsctId { get; set; }
    public string Type { get; set; } = string.Empty;
    public int? Quantity { get; set; }
    public string? Size { get; set; }
    public string? ComponentType { get; set; }             // ascp_type (Pleated / MERV 8)
    public string? PartNumber { get; set; }
    public string? Note { get; set; }
    public bool Active { get; set; }
}

public class AssetAttributeDto
{
    public int AsaId { get; set; }
    public int AsId { get; set; }
    public int AsatId { get; set; }
    public string Key { get; set; } = string.Empty;
    public string Label { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public string? Value { get; set; }
    public decimal? Numeric { get; set; }
}

public class SaveAssetRequest
{
    public int AscId { get; set; }
    public int? AsmId { get; set; }
    public string? Manufacturer { get; set; }
    public string? ModelNumber { get; set; }
    public string? SerialNumber { get; set; }
    public string? Description { get; set; }
    public string? SpecialInstructions { get; set; }
    public string? UnitTag { get; set; }
    public string? AssetTag { get; set; }
    public int? AsIdConnected { get; set; }
    public decimal? CapacityTons { get; set; }
    public string? HeatingType { get; set; }
    public string? RefrigerantType { get; set; }
    public short? ManufactureYear { get; set; }
    public DateTime? InstallDate { get; set; }
    public string? ServedArea { get; set; }
    public int? AmlId { get; set; }
    public bool Active { get; set; } = true;
    public bool MarkVerified { get; set; }                 // stamp as_lastverifieddatetime / u_id_lastverified with the caller
    public List<SaveAssetComponentRequest> Components { get; set; } = new();
    public List<SaveAssetAttributeRequest> Attributes { get; set; } = new();
}

public class SaveAssetComponentRequest
{
    public int AsctId { get; set; }
    public int? Quantity { get; set; }
    public string? Size { get; set; }
    public string? ComponentType { get; set; }
    public string? PartNumber { get; set; }
    public string? Note { get; set; }
    public bool Active { get; set; } = true;
}

public class SaveAssetAttributeRequest
{
    public int AsatId { get; set; }
    public string? Value { get; set; }                     // blank = remove the attribute row
}

#endregion

#region facts for the resolver

/// <summary>What the form resolver needs to know about one unit. Built from Asset + AssetAttribute + lookups.</summary>
public class AssetFactsDto
{
    public int AsId { get; set; }
    public string Label { get; set; } = string.Empty;      // "Unit 3 · RTU · Carrier 48TC"
    public string? EquipmentType { get; set; }
    public string? HeatingType { get; set; }
    public string? Mount { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new(StringComparer.OrdinalIgnoreCase);   // key -> value ("true"/"false", number, text)
    public Dictionary<string, int> RepeatCounts { get; set; } = new(StringComparer.OrdinalIgnoreCase);    // repeat key -> count
}

#endregion
