using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using Dapper;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;

namespace EvoAPI.Infrastructure.Repositories;

/// <summary>
/// Data access for the asset and location layer (PM build slice 2): AssetCategory / AssetManufacturer (existing),
/// AssetComponentType / AssetAttributeType / AssetComponent / AssetAttribute / LocationTradeProfile / AssetMountLocation /
/// LocationAccessRequirement (create_asset_tables.sql) and the new identity columns on Asset.
/// </summary>
public class AssetRepository : IAssetRepository
{
    private readonly string _connectionString;

    private const string CategorySelect = @"
        SELECT
            c.asc_id                   AS AscId,
            c.t_id                     AS TId,
            LTRIM(RTRIM(c.asc_category)) AS Category,
            c.asc_manufacturer         AS RequireManufacturer,
            c.asc_modelnumber          AS RequireModelNumber,
            c.asc_serialnumber         AS RequireSerialNumber,
            c.asc_description          AS RequireDescription,
            c.asc_specialinstructions  AS SpecialInstructions,
            (SELECT COUNT(*) FROM dbo.AssetManufacturer m WHERE m.asc_id = c.asc_id) AS ManufacturerCount,
            (SELECT COUNT(*) FROM dbo.Asset a WHERE a.asc_id = c.asc_id) AS AssetCount
        FROM dbo.AssetCategory c";

    private const string ManufacturerSelect = @"
        SELECT
            m.asm_id                        AS AsmId,
            m.asc_id                        AS AscId,
            LTRIM(RTRIM(m.asm_manufacturer)) AS Manufacturer,
            (SELECT COUNT(*) FROM dbo.Asset a WHERE a.asm_id = m.asm_id) AS AssetCount
        FROM dbo.AssetManufacturer m";

    private const string ComponentTypeSelect = @"
        SELECT
            ct.asct_id          AS AsctId,
            ct.t_id             AS TId,
            ct.asct_type        AS [Type],
            ct.asct_hassize     AS HasSize,
            ct.asct_hasquantity AS HasQuantity,
            ct.asct_hastype     AS HasType,
            ct.asct_order       AS [Order],
            ct.asct_active      AS Active,
            (SELECT COUNT(*) FROM dbo.AssetComponent cp WHERE cp.asct_id = ct.asct_id) AS UsageCount
        FROM dbo.AssetComponentType ct";

    private const string AttributeTypeSelect = @"
        SELECT
            ty.asat_id        AS AsatId,
            ty.t_id           AS TId,
            ty.asat_key       AS [Key],
            ty.asat_label     AS Label,
            ty.asat_datatype  AS DataType,
            ty.asat_values    AS [Values],
            ty.fq_code        AS FqCode,
            ty.asat_repeatkey AS RepeatKey,
            ty.asat_order     AS [Order],
            ty.asat_active    AS Active,
            (SELECT COUNT(*) FROM dbo.AssetAttribute at WHERE at.asat_id = ty.asat_id) AS UsageCount
        FROM dbo.AssetAttributeType ty";

    private const string ProfileSelect = @"
        SELECT
            p.ltp_id                 AS LtpId,
            p.l_id                   AS LId,
            p.t_id                   AS TId,
            p.ltp_unitcount          AS UnitCount,
            p.ltp_accessrequirements AS AccessRequirements,
            p.ltp_accessnote         AS AccessNote,
            p.ltp_mountlocations      AS MountLocations,
            p.att_id_aerial          AS AttIdAerial,
            p.ltp_managername        AS ManagerName,
            p.ltp_managerphone       AS ManagerPhone,
            p.ltp_note               AS Note,
            p.ltp_insertdatetime     AS InsertDateTime,
            p.ltp_modifieddatetime   AS ModifiedDateTime
        FROM dbo.LocationTradeProfile p";

    private const string AssetSelect = @"
        SELECT
            a.as_id                     AS AsId,
            a.l_id                      AS LId,
            a.asc_id                    AS AscId,
            LTRIM(RTRIM(c.asc_category)) AS Category,
            a.asm_id                    AS AsmId,
            LTRIM(RTRIM(m.asm_manufacturer)) AS ManufacturerName,
            a.as_manufacturer           AS Manufacturer,
            a.as_modelnumber            AS ModelNumber,
            a.as_serialnumber           AS SerialNumber,
            a.as_description            AS Description,
            a.as_specialinstructions    AS SpecialInstructions,
            a.as_unittag                AS UnitTag,
            a.as_assettag               AS AssetTag,
            a.as_id_connected           AS AsIdConnected,
            con.as_unittag              AS ConnectedUnitTag,
            a.as_capacitytons           AS CapacityTons,
            a.as_heatingtype            AS HeatingType,
            a.as_refrigeranttype        AS RefrigerantType,
            a.as_manufactureyear        AS ManufactureYear,
            a.as_installdate            AS InstallDate,
            a.as_servedarea             AS ServedArea,
            a.aml_id                   AS AmlId,
            ul.aml_location            AS MountLocation,
            a.as_mapx                   AS MapX,
            a.as_mapy                   AS MapY,
            a.as_active                 AS Active,
            a.as_lastverifieddatetime   AS LastVerifiedDateTime,
            a.u_id_lastverified         AS UIdLastVerified,
            a.att_id_nameplate          AS AttIdNameplate,
            a.as_insertdatetime         AS InsertDateTime,
            a.as_modifieddatetime       AS ModifiedDateTime
        FROM dbo.Asset a
        JOIN dbo.AssetCategory c ON c.asc_id = a.asc_id
        LEFT JOIN dbo.AssetManufacturer m ON m.asm_id = a.asm_id
        LEFT JOIN dbo.Asset con ON con.as_id = a.as_id_connected
        LEFT JOIN dbo.AssetMountLocation ul ON ul.aml_id = a.aml_id";

    private const string ComponentSelect = @"
        SELECT
            cp.ascp_id         AS AscpId,
            cp.as_id           AS AsId,
            cp.asct_id         AS AsctId,
            ct.asct_type       AS [Type],
            cp.ascp_quantity   AS Quantity,
            cp.ascp_size       AS Size,
            cp.ascp_type       AS ComponentType,
            cp.ascp_partnumber AS PartNumber,
            cp.ascp_note       AS Note,
            cp.ascp_active     AS Active
        FROM dbo.AssetComponent cp
        JOIN dbo.AssetComponentType ct ON ct.asct_id = cp.asct_id";

    private const string AttributeSelect = @"
        SELECT
            at.asa_id        AS AsaId,
            at.as_id         AS AsId,
            at.asat_id       AS AsatId,
            ty.asat_key      AS [Key],
            ty.asat_label    AS Label,
            ty.asat_datatype AS DataType,
            at.asa_value     AS Value,
            at.asa_numeric   AS Numeric
        FROM dbo.AssetAttribute at
        JOIN dbo.AssetAttributeType ty ON ty.asat_id = at.asat_id";

    public AssetRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
    }

    #region definitions per parent trade

    public async Task<List<AssetTradeDto>> GetTradesAsync()
    {
        using var conn = new SqlConnection(_connectionString);
        var rows = await conn.QueryAsync<AssetTradeDto>(@"
            SELECT
                t.t_id    AS TId,
                LTRIM(RTRIM(t.t_trade)) AS Trade,
                (SELECT COUNT(*) FROM dbo.AssetCategory c WHERE c.t_id = t.t_id) AS CategoryCount,
                (SELECT COUNT(*) FROM dbo.AssetComponentType x WHERE x.t_id = t.t_id) AS ComponentTypeCount,
                (SELECT COUNT(*) FROM dbo.AssetAttributeType x WHERE x.t_id = t.t_id) AS AttributeTypeCount,
                (SELECT COUNT(*) FROM dbo.Asset a JOIN dbo.AssetCategory c ON c.asc_id = a.asc_id WHERE c.t_id = t.t_id) AS AssetCount
            FROM dbo.Trade t
            WHERE t.t_parentonly = 1 AND t.t_active = 1
            ORDER BY t.t_trade");
        return rows.ToList();
    }

    public async Task<List<AssetCategoryDto>> GetCategoriesAsync(int tId)
    {
        using var conn = new SqlConnection(_connectionString);
        return (await conn.QueryAsync<AssetCategoryDto>(CategorySelect + " WHERE c.t_id = @TId ORDER BY c.asc_category", new { TId = tId })).ToList();
    }

    public async Task<AssetCategoryDto?> GetCategoryAsync(int ascId)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.QueryFirstOrDefaultAsync<AssetCategoryDto>(CategorySelect + " WHERE c.asc_id = @AscId", new { AscId = ascId });
    }

    public async Task<int> CreateCategoryAsync(int tId, SaveAssetCategoryRequest r)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.AssetCategory (t_id, asc_category, asc_manufacturer, asc_modelnumber, asc_serialnumber, asc_description, asc_specialinstructions)
            VALUES (@TId, @Category, @RequireManufacturer, @RequireModelNumber, @RequireSerialNumber, @RequireDescription, @SpecialInstructions);
            SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { TId = tId, Category = r.Category.Trim(), r.RequireManufacturer, r.RequireModelNumber, r.RequireSerialNumber, r.RequireDescription, r.SpecialInstructions });
    }

    public async Task<bool> UpdateCategoryAsync(int ascId, SaveAssetCategoryRequest r)
    {
        using var conn = new SqlConnection(_connectionString);
        var n = await conn.ExecuteAsync(@"
            UPDATE dbo.AssetCategory SET asc_category = @Category, asc_manufacturer = @RequireManufacturer, asc_modelnumber = @RequireModelNumber,
                asc_serialnumber = @RequireSerialNumber, asc_description = @RequireDescription, asc_specialinstructions = @SpecialInstructions,
                asc_modifieddatetime = GETDATE()
            WHERE asc_id = @AscId",
            new { AscId = ascId, Category = r.Category.Trim(), r.RequireManufacturer, r.RequireModelNumber, r.RequireSerialNumber, r.RequireDescription, r.SpecialInstructions });
        return n > 0;
    }

    public async Task<List<AssetManufacturerDto>> GetManufacturersAsync(int ascId)
    {
        using var conn = new SqlConnection(_connectionString);
        return (await conn.QueryAsync<AssetManufacturerDto>(ManufacturerSelect + " WHERE m.asc_id = @AscId ORDER BY m.asm_manufacturer", new { AscId = ascId })).ToList();
    }

    public async Task<AssetManufacturerDto?> GetManufacturerAsync(int asmId)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.QueryFirstOrDefaultAsync<AssetManufacturerDto>(ManufacturerSelect + " WHERE m.asm_id = @AsmId", new { AsmId = asmId });
    }

    public async Task<int> CreateManufacturerAsync(int ascId, string manufacturer)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteScalarAsync<int>(
            "INSERT INTO dbo.AssetManufacturer (asc_id, asm_manufacturer) VALUES (@AscId, @Manufacturer); SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { AscId = ascId, Manufacturer = manufacturer.Trim() });
    }

    public async Task<bool> UpdateManufacturerAsync(int asmId, string manufacturer)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteAsync("UPDATE dbo.AssetManufacturer SET asm_manufacturer = @Manufacturer, asm_modifieddatetime = GETDATE() WHERE asm_id = @AsmId",
            new { AsmId = asmId, Manufacturer = manufacturer.Trim() }) > 0;
    }

    public async Task<FormDeleteResult> DeleteManufacturerAsync(int asmId)
    {
        using var conn = new SqlConnection(_connectionString);
        var used = await conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM dbo.Asset WHERE asm_id = @AsmId", new { AsmId = asmId });
        if (used > 0) return FormDeleteResult.Referenced;
        var n = await conn.ExecuteAsync("DELETE FROM dbo.AssetManufacturer WHERE asm_id = @AsmId", new { AsmId = asmId });
        return n > 0 ? FormDeleteResult.Deleted : FormDeleteResult.NotFound;
    }

    public async Task<List<AssetComponentTypeDto>> GetComponentTypesAsync(int tId)
    {
        using var conn = new SqlConnection(_connectionString);
        return (await conn.QueryAsync<AssetComponentTypeDto>(ComponentTypeSelect + " WHERE ct.t_id = @TId ORDER BY ct.asct_order, ct.asct_type", new { TId = tId })).ToList();
    }

    public async Task<int> CreateComponentTypeAsync(int tId, SaveAssetComponentTypeRequest r)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.AssetComponentType (t_id, asct_type, asct_hassize, asct_hasquantity, asct_hastype, asct_order, asct_active)
            VALUES (@TId, @Type, @HasSize, @HasQuantity, @HasType, @Order, @Active);
            SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { TId = tId, Type = r.Type.Trim(), r.HasSize, r.HasQuantity, r.HasType, r.Order, r.Active });
    }

    public async Task<bool> UpdateComponentTypeAsync(int asctId, SaveAssetComponentTypeRequest r)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteAsync(@"
            UPDATE dbo.AssetComponentType SET asct_type = @Type, asct_hassize = @HasSize, asct_hasquantity = @HasQuantity, asct_hastype = @HasType,
                asct_order = @Order, asct_active = @Active, asct_modifieddatetime = GETDATE()
            WHERE asct_id = @AsctId",
            new { AsctId = asctId, Type = r.Type.Trim(), r.HasSize, r.HasQuantity, r.HasType, r.Order, r.Active }) > 0;
    }

    public async Task<List<AssetAttributeTypeDto>> GetAttributeTypesAsync(int tId)
    {
        using var conn = new SqlConnection(_connectionString);
        return (await conn.QueryAsync<AssetAttributeTypeDto>(AttributeTypeSelect + " WHERE ty.t_id = @TId ORDER BY ty.asat_order, ty.asat_key", new { TId = tId })).ToList();
    }

    public async Task<AssetAttributeTypeDto?> GetAttributeTypeAsync(int asatId)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.QueryFirstOrDefaultAsync<AssetAttributeTypeDto>(AttributeTypeSelect + " WHERE ty.asat_id = @AsatId", new { AsatId = asatId });
    }

    public async Task<int> CreateAttributeTypeAsync(int tId, SaveAssetAttributeTypeRequest r)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.AssetAttributeType (t_id, asat_key, asat_label, asat_datatype, asat_values, fq_code, asat_repeatkey, asat_order, asat_active)
            VALUES (@TId, @Key, @Label, @DataType, @Values, @FqCode, @RepeatKey, @Order, @Active);
            SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { TId = tId, Key = r.Key.Trim(), Label = r.Label.Trim(), r.DataType, r.Values, r.FqCode, r.RepeatKey, r.Order, r.Active });
    }

    public async Task<bool> UpdateAttributeTypeAsync(int asatId, SaveAssetAttributeTypeRequest r)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteAsync(@"
            UPDATE dbo.AssetAttributeType SET asat_key = @Key, asat_label = @Label, asat_datatype = @DataType, asat_values = @Values, fq_code = @FqCode,
                asat_repeatkey = @RepeatKey, asat_order = @Order, asat_active = @Active, asat_modifieddatetime = GETDATE()
            WHERE asat_id = @AsatId",
            new { AsatId = asatId, Key = r.Key.Trim(), Label = r.Label.Trim(), r.DataType, r.Values, r.FqCode, r.RepeatKey, r.Order, r.Active }) > 0;
    }

    #endregion

    #region mount location and access requirement lookups

    public async Task<List<AssetMountLocationDto>> GetMountLocationsAsync()
    {
        using var conn = new SqlConnection(_connectionString);
        return (await conn.QueryAsync<AssetMountLocationDto>(@"
            SELECT u.aml_id AS AmlId, u.aml_location AS Location, u.aml_order AS [Order], u.aml_active AS Active,
                   (SELECT COUNT(*) FROM dbo.Asset a WHERE a.aml_id = u.aml_id) AS UsageCount
            FROM dbo.AssetMountLocation u ORDER BY u.aml_order, u.aml_location")).ToList();
    }

    public async Task<int> CreateMountLocationAsync(SaveAssetMountLocationRequest r)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteScalarAsync<int>("INSERT INTO dbo.AssetMountLocation (aml_location, aml_order, aml_active) VALUES (@Location, @Order, @Active); SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { Location = r.Location.Trim(), r.Order, r.Active });
    }

    public async Task<bool> UpdateMountLocationAsync(int pmulId, SaveAssetMountLocationRequest r)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteAsync("UPDATE dbo.AssetMountLocation SET aml_location = @Location, aml_order = @Order, aml_active = @Active, aml_modifieddatetime = GETDATE() WHERE aml_id = @AmlId",
            new { AmlId = pmulId, Location = r.Location.Trim(), r.Order, r.Active }) > 0;
    }

    public async Task<List<LocationAccessRequirementDto>> GetAccessRequirementsAsync()
    {
        using var conn = new SqlConnection(_connectionString);
        return (await conn.QueryAsync<LocationAccessRequirementDto>(@"
            SELECT lar_id AS LarId, lar_requirement AS Requirement, lar_requiresnote AS RequiresNote, lar_order AS [Order], lar_active AS Active
            FROM dbo.LocationAccessRequirement ORDER BY lar_order, lar_requirement")).ToList();
    }

    public async Task<int> CreateAccessRequirementAsync(SaveLocationAccessRequirementRequest r)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteScalarAsync<int>("INSERT INTO dbo.LocationAccessRequirement (lar_requirement, lar_requiresnote, lar_order, lar_active) VALUES (@Requirement, @RequiresNote, @Order, @Active); SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { Requirement = r.Requirement.Trim(), r.RequiresNote, r.Order, r.Active });
    }

    public async Task<bool> UpdateAccessRequirementAsync(int pmarId, SaveLocationAccessRequirementRequest r)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteAsync("UPDATE dbo.LocationAccessRequirement SET lar_requirement = @Requirement, lar_requiresnote = @RequiresNote, lar_order = @Order, lar_active = @Active, lar_modifieddatetime = GETDATE() WHERE lar_id = @LarId",
            new { LarId = pmarId, Requirement = r.Requirement.Trim(), r.RequiresNote, r.Order, r.Active }) > 0;
    }

    #endregion

    #region location trade profile

    public async Task<LocationTradeProfileDto?> GetProfileAsync(int lId, int tId)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.QueryFirstOrDefaultAsync<LocationTradeProfileDto>(ProfileSelect + " WHERE p.l_id = @LId AND p.t_id = @TId", new { LId = lId, TId = tId });
    }

    public async Task<LocationTradeProfileDto> UpsertProfileAsync(int lId, int tId, SaveLocationTradeProfileRequest r)
    {
        using var conn = new SqlConnection(_connectionString);
        var args = new
        {
            LId = lId, TId = tId, r.UnitCount, r.AccessRequirements, r.AccessNote, r.MountLocations, r.AttIdAerial,
            r.ManagerName, r.ManagerPhone, r.Note
        };
        var updated = await conn.ExecuteAsync(@"
            UPDATE dbo.LocationTradeProfile SET ltp_unitcount = @UnitCount, ltp_accessrequirements = @AccessRequirements, ltp_accessnote = @AccessNote,
                ltp_mountlocations = @MountLocations, att_id_aerial = @AttIdAerial, ltp_managername = @ManagerName, ltp_managerphone = @ManagerPhone,
                ltp_note = @Note, ltp_modifieddatetime = GETDATE()
            WHERE l_id = @LId AND t_id = @TId", args);
        if (updated == 0)
        {
            await conn.ExecuteAsync(@"
                INSERT INTO dbo.LocationTradeProfile (l_id, t_id, ltp_unitcount, ltp_accessrequirements, ltp_accessnote, ltp_mountlocations, att_id_aerial, ltp_managername, ltp_managerphone, ltp_note)
                VALUES (@LId, @TId, @UnitCount, @AccessRequirements, @AccessNote, @MountLocations, @AttIdAerial, @ManagerName, @ManagerPhone, @Note)", args);
        }
        return (await GetProfileAsync(lId, tId))!;
    }

    #endregion

    #region assets at a location

    public async Task<bool> LocationExistsAsync(int lId)
    {
        using var conn = new SqlConnection(_connectionString);
        return await conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM dbo.Location WHERE l_id = @LId", new { LId = lId }) > 0;
    }

    public async Task<List<AssetDto>> GetLocationAssetsAsync(int lId, int? tId, bool includeInactive)
    {
        using var conn = new SqlConnection(_connectionString);
        var assets = (await conn.QueryAsync<AssetDto>(AssetSelect + @"
            WHERE a.l_id = @LId AND (@TId IS NULL OR c.t_id = @TId) AND (@IncludeInactive = 1 OR a.as_active = 1)
            ORDER BY a.as_unittag, a.as_id", new { LId = lId, TId = tId, IncludeInactive = includeInactive ? 1 : 0 })).ToList();
        await AttachChildrenAsync(conn, assets);
        return assets;
    }

    public async Task<AssetDto?> GetAssetAsync(int asId)
    {
        using var conn = new SqlConnection(_connectionString);
        var asset = await conn.QueryFirstOrDefaultAsync<AssetDto>(AssetSelect + " WHERE a.as_id = @AsId", new { AsId = asId });
        if (asset == null) return null;
        await AttachChildrenAsync(conn, new List<AssetDto> { asset });
        return asset;
    }

    private static async Task AttachChildrenAsync(SqlConnection conn, List<AssetDto> assets)
    {
        if (assets.Count == 0) return;
        var ids = assets.Select(a => a.AsId).ToArray();
        var components = await conn.QueryAsync<AssetComponentDto>(ComponentSelect + " WHERE cp.as_id IN @Ids ORDER BY ct.asct_order, cp.ascp_id", new { Ids = ids });
        var attributes = await conn.QueryAsync<AssetAttributeDto>(AttributeSelect + " WHERE at.as_id IN @Ids ORDER BY ty.asat_order, ty.asat_key", new { Ids = ids });
        var byId = assets.ToDictionary(a => a.AsId);
        foreach (var c in components) if (byId.TryGetValue(c.AsId, out var a)) a.Components.Add(c);
        foreach (var at in attributes) if (byId.TryGetValue(at.AsId, out var a)) a.Attributes.Add(at);
    }

    public async Task<int> CreateAssetAsync(int lId, SaveAssetRequest r, int? userId)
    {
        using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        using var tx = conn.BeginTransaction();
        var asId = await conn.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.Asset (l_id, asc_id, asm_id, as_manufacturer, as_modelnumber, as_serialnumber, as_description, as_specialinstructions,
                as_unittag, as_assettag, as_id_connected, as_capacitytons, as_heatingtype, as_refrigeranttype, as_manufactureyear, as_installdate,
                as_servedarea, aml_id, as_active, as_lastverifieddatetime, u_id_lastverified, as_insertdatetime)
            VALUES (@LId, @AscId, @AsmId, @Manufacturer, @ModelNumber, @SerialNumber, @Description, @SpecialInstructions,
                @UnitTag, @AssetTag, @AsIdConnected, @CapacityTons, @HeatingType, @RefrigerantType, @ManufactureYear, @InstallDate,
                @ServedArea, @AmlId, @Active, CASE WHEN @MarkVerified = 1 THEN GETDATE() ELSE NULL END, CASE WHEN @MarkVerified = 1 THEN @UserId ELSE NULL END, GETDATE());
            SELECT CAST(SCOPE_IDENTITY() AS INT);", AssetArgs(lId, r, userId), tx);
        await ReplaceChildrenAsync(conn, tx, asId, r, userId);
        tx.Commit();
        return asId;
    }

    public async Task<bool> UpdateAssetAsync(int asId, SaveAssetRequest r, int? userId)
    {
        using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        using var tx = conn.BeginTransaction();
        var n = await conn.ExecuteAsync(@"
            UPDATE dbo.Asset SET asc_id = @AscId, asm_id = @AsmId, as_manufacturer = @Manufacturer, as_modelnumber = @ModelNumber, as_serialnumber = @SerialNumber,
                as_description = @Description, as_specialinstructions = @SpecialInstructions, as_unittag = @UnitTag, as_assettag = @AssetTag,
                as_id_connected = @AsIdConnected, as_capacitytons = @CapacityTons, as_heatingtype = @HeatingType, as_refrigeranttype = @RefrigerantType,
                as_manufactureyear = @ManufactureYear, as_installdate = @InstallDate, as_servedarea = @ServedArea, aml_id = @AmlId, as_active = @Active,
                as_lastverifieddatetime = CASE WHEN @MarkVerified = 1 THEN GETDATE() ELSE as_lastverifieddatetime END,
                u_id_lastverified = CASE WHEN @MarkVerified = 1 THEN @UserId ELSE u_id_lastverified END,
                as_modifieddatetime = GETDATE()
            WHERE as_id = @AsId", AssetArgs(asId, r, userId, forUpdate: true), tx);
        if (n == 0) { tx.Rollback(); return false; }
        await ReplaceChildrenAsync(conn, tx, asId, r, userId);
        tx.Commit();
        return true;
    }

    private static object AssetArgs(int id, SaveAssetRequest r, int? userId, bool forUpdate = false) => new
    {
        LId = forUpdate ? 0 : id, AsId = forUpdate ? id : 0,
        r.AscId, r.AsmId, Manufacturer = Trim(r.Manufacturer, 50), ModelNumber = Trim(r.ModelNumber, 100), SerialNumber = Trim(r.SerialNumber, 100),
        r.Description, r.SpecialInstructions, UnitTag = Trim(r.UnitTag, 50), AssetTag = Trim(r.AssetTag, 50), AsIdConnected = r.AsIdConnected == 0 ? null : r.AsIdConnected,
        r.CapacityTons, HeatingType = Trim(r.HeatingType, 30), RefrigerantType = Trim(r.RefrigerantType, 20), r.ManufactureYear, r.InstallDate,
        ServedArea = Trim(r.ServedArea, 100), AmlId = r.AmlId == 0 ? null : r.AmlId, r.Active, MarkVerified = r.MarkVerified ? 1 : 0, UserId = userId
    };

    private static string? Trim(string? s, int max)
    {
        if (string.IsNullOrWhiteSpace(s)) return null;
        s = s.Trim();
        return s.Length > max ? s[..max] : s;
    }

    /// <summary>Components and attributes are replaced as a set: what the caller sends is what the asset has.</summary>
    private static async Task ReplaceChildrenAsync(SqlConnection conn, IDbTransaction tx, int asId, SaveAssetRequest r, int? userId)
    {
        await conn.ExecuteAsync("DELETE FROM dbo.AssetComponent WHERE as_id = @AsId", new { AsId = asId }, tx);
        foreach (var c in r.Components.Where(c => c.AsctId > 0 && (c.Quantity != null || !string.IsNullOrWhiteSpace(c.Size) || !string.IsNullOrWhiteSpace(c.ComponentType) || !string.IsNullOrWhiteSpace(c.PartNumber) || !string.IsNullOrWhiteSpace(c.Note))))
        {
            await conn.ExecuteAsync(@"
                INSERT INTO dbo.AssetComponent (as_id, asct_id, ascp_quantity, ascp_size, ascp_type, ascp_partnumber, ascp_note, ascp_active)
                VALUES (@AsId, @AsctId, @Quantity, @Size, @ComponentType, @PartNumber, @Note, @Active)",
                new { AsId = asId, c.AsctId, c.Quantity, Size = Trim(c.Size, 30), ComponentType = Trim(c.ComponentType, 50), PartNumber = Trim(c.PartNumber, 50), Note = Trim(c.Note, 400), c.Active }, tx);
        }

        // attribute values are normalised by their type: Bool -> true/false, Number -> numeric copy, blank -> no row
        var types = (await conn.QueryAsync<(int AsatId, string DataType)>("SELECT asat_id, asat_datatype FROM dbo.AssetAttributeType WHERE asat_id IN @Ids",
            new { Ids = r.Attributes.Select(a => a.AsatId).Distinct().ToArray() }, tx)).ToDictionary(t => t.AsatId, t => t.DataType);
        await conn.ExecuteAsync("DELETE FROM dbo.AssetAttribute WHERE as_id = @AsId", new { AsId = asId }, tx);
        foreach (var a in r.Attributes)
        {
            if (!types.TryGetValue(a.AsatId, out var dataType)) continue;
            var raw = a.Value?.Trim();
            if (string.IsNullOrEmpty(raw)) continue;
            string? value = raw;
            decimal? numeric = null;
            switch (dataType)
            {
                case "Bool":
                    if (IsTrue(raw)) value = "true";
                    else if (IsFalse(raw)) value = "false";
                    else continue;   // Unknown / not set: no row
                    break;
                case "Number":
                    if (!decimal.TryParse(raw, NumberStyles.Any, CultureInfo.InvariantCulture, out var d)) continue;
                    numeric = d;
                    value = d.ToString(CultureInfo.InvariantCulture);
                    break;
            }
            await conn.ExecuteAsync(@"
                INSERT INTO dbo.AssetAttribute (as_id, asat_id, asa_value, asa_numeric, u_id)
                VALUES (@AsId, @AsatId, @Value, @Numeric, @UserId)",
                new { AsId = asId, a.AsatId, Value = Trim(value, 400), Numeric = numeric, UserId = userId }, tx);
        }
    }

    private static bool IsTrue(string s) => s.Equals("true", StringComparison.OrdinalIgnoreCase) || s.Equals("yes", StringComparison.OrdinalIgnoreCase) || s == "1" || s.Equals("y", StringComparison.OrdinalIgnoreCase);
    private static bool IsFalse(string s) => s.Equals("false", StringComparison.OrdinalIgnoreCase) || s.Equals("no", StringComparison.OrdinalIgnoreCase) || s == "0" || s.Equals("n", StringComparison.OrdinalIgnoreCase);

    #endregion

    #region facts for the resolver

    public async Task<AssetFactsDto?> GetAssetFactsAsync(int asId)
    {
        using var conn = new SqlConnection(_connectionString);
        var row = await conn.QueryFirstOrDefaultAsync<(int AsId, string? UnitTag, string Category, string? Mfr, string? Model, string? HeatingType, string? Mount)>(@"
            SELECT a.as_id, a.as_unittag, LTRIM(RTRIM(c.asc_category)), COALESCE(LTRIM(RTRIM(m.asm_manufacturer)), a.as_manufacturer), a.as_modelnumber, a.as_heatingtype, ul.aml_location
            FROM dbo.Asset a
            JOIN dbo.AssetCategory c ON c.asc_id = a.asc_id
            LEFT JOIN dbo.AssetManufacturer m ON m.asm_id = a.asm_id
            LEFT JOIN dbo.AssetMountLocation ul ON ul.aml_id = a.aml_id
            WHERE a.as_id = @AsId", new { AsId = asId });
        if (row.AsId == 0) return null;

        var facts = new AssetFactsDto
        {
            AsId = row.AsId,
            EquipmentType = row.Category,
            HeatingType = string.IsNullOrWhiteSpace(row.HeatingType) ? null : row.HeatingType.Trim(),
            Mount = row.Mount,
            Label = string.Join(" · ", new[] { string.IsNullOrWhiteSpace(row.UnitTag) ? $"Asset {row.AsId}" : row.UnitTag.Trim(), row.Category, string.Join(" ", new[] { row.Mfr, row.Model }.Where(s => !string.IsNullOrWhiteSpace(s))) }.Where(s => !string.IsNullOrWhiteSpace(s)))
        };
        var attrs = await conn.QueryAsync<(string Key, string DataType, string? RepeatKey, string? Value, decimal? Numeric)>(@"
            SELECT ty.asat_key, ty.asat_datatype, ty.asat_repeatkey, at.asa_value, at.asa_numeric
            FROM dbo.AssetAttribute at JOIN dbo.AssetAttributeType ty ON ty.asat_id = at.asat_id
            WHERE at.as_id = @AsId AND ty.asat_active = 1", new { AsId = asId });
        foreach (var a in attrs)
        {
            if (a.Value != null) facts.Attributes[a.Key] = a.Value;
            if (!string.IsNullOrWhiteSpace(a.RepeatKey) && a.Numeric != null) facts.RepeatCounts[a.RepeatKey] = (int)Math.Round(a.Numeric.Value);
        }
        return facts;
    }

    #endregion
}
