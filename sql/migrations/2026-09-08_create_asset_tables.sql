-- =====================================================================================================
-- Preventative Maintenance build slice 2: asset and location layer
-- Plan: Temp\PM\PM Workflow - POC Plan.md section 8b (2026-09-07); design: technical-design\data-model-assets-locations.html
--
-- Generic names on purpose (no PM prefix): mounting, access and the per-trade location profile matter for any visit once
-- the legacy checklists move onto the form engine. Only the billing terms (PMRule, PMRateTier, PMSeason, PMBillingMode) stay PM.
-- Adds, all guarded so the script can be re-run:
--   1. Nullable identity columns on the existing Asset table (unit tag, tonnage, heating type, refrigerant,
--      year, install date, area served, mounting, connected unit, map pin, active, verification stamp, nameplate photo).
--   2. Lookups: AssetMountLocation (where a unit is mounted), LocationAccessRequirement (what a tech needs to reach it),
--      AssetComponentType (filters, belts ... per parent trade), AssetAttributeType (the yes/no features, counts and
--      free-form facts per parent trade that template conditions and repeat groups read).
--   3. AssetComponent and AssetAttribute rows hanging off an asset.
--   4. LocationTradeProfile: one row per location per parent trade (unit count, access, unit locations, aerial, manager).
--   5. Seed rows for HVAC: unit locations, access requirements, component types, attribute types, and the
--      manufacturer lists from the seeded HVAC template (F051 questions) into AssetManufacturer per equipment type.
--
-- Nothing here is dropped or altered destructively. Existing Asset rows keep working: every new column is
-- nullable except as_active, which defaults to 1. Legacy EvoWS/EvoUI ignore the new columns.
-- Deploy together with EvoAPI (AssetController) and evotech (Settings > Assets, LOCATIONS > PM profile), behind the
-- PreventativeMaintenance feature flag.
-- =====================================================================================================

SET NOCOUNT ON;
GO

------------------------------------------------------------------------------------------------------
-- 1. Lookups (created first so Asset can reference AssetMountLocation)
------------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'AssetMountLocation')
BEGIN
    CREATE TABLE dbo.AssetMountLocation (
        aml_id                INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        aml_insertdatetime    DATETIME      NOT NULL CONSTRAINT DF_AssetMountLocation_insertdatetime DEFAULT (GETDATE()),
        aml_modifieddatetime  DATETIME      NULL,
        aml_location          NVARCHAR(60)  NOT NULL,   -- Ground | Rooftop | Stilts | Fenced area on the roof ... (matches "mount" condition values)
        aml_order             INT           NOT NULL CONSTRAINT DF_AssetMountLocation_order DEFAULT (0),
        aml_active            BIT           NOT NULL CONSTRAINT DF_AssetMountLocation_active DEFAULT (1),
        CONSTRAINT UQ_AssetMountLocation_Location UNIQUE (aml_location)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'LocationAccessRequirement')
BEGIN
    CREATE TABLE dbo.LocationAccessRequirement (
        lar_id                INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        lar_insertdatetime    DATETIME      NOT NULL CONSTRAINT DF_LocationAccessRequirement_insertdatetime DEFAULT (GETDATE()),
        lar_modifieddatetime  DATETIME      NULL,
        lar_requirement       NVARCHAR(60)  NOT NULL,   -- Roof hatch | Ladder | Lift | Key | Locked area | Escort | Ceiling access | Other
        lar_requiresnote      BIT           NOT NULL CONSTRAINT DF_LocationAccessRequirement_requiresnote DEFAULT (0),  -- "Other" needs the access note
        lar_order             INT           NOT NULL CONSTRAINT DF_LocationAccessRequirement_order DEFAULT (0),
        lar_active            BIT           NOT NULL CONSTRAINT DF_LocationAccessRequirement_active DEFAULT (1),
        CONSTRAINT UQ_LocationAccessRequirement_Requirement UNIQUE (lar_requirement)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'AssetComponentType')
BEGIN
    CREATE TABLE dbo.AssetComponentType (
        asct_id                INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        asct_insertdatetime    DATETIME      NOT NULL CONSTRAINT DF_AssetComponentType_insertdatetime DEFAULT (GETDATE()),
        asct_modifieddatetime  DATETIME      NULL,
        t_id                   INT           NOT NULL REFERENCES dbo.Trade (t_id),   -- parent trade
        asct_type              NVARCHAR(50)  NOT NULL,   -- Filter | Belt | Fresh-air Filter ...
        asct_hassize           BIT           NOT NULL CONSTRAINT DF_AssetComponentType_hassize DEFAULT (1),      -- 20x25x2, A42
        asct_hasquantity       BIT           NOT NULL CONSTRAINT DF_AssetComponentType_hasquantity DEFAULT (1),
        asct_hastype           BIT           NOT NULL CONSTRAINT DF_AssetComponentType_hastype DEFAULT (0),      -- Pleated / MERV 8 ...
        asct_order             INT           NOT NULL CONSTRAINT DF_AssetComponentType_order DEFAULT (0),
        asct_active            BIT           NOT NULL CONSTRAINT DF_AssetComponentType_active DEFAULT (1),
        CONSTRAINT UQ_AssetComponentType_Trade_Type UNIQUE (t_id, asct_type)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'AssetAttributeType')
BEGIN
    CREATE TABLE dbo.AssetAttributeType (
        asat_id                INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        asat_insertdatetime    DATETIME      NOT NULL CONSTRAINT DF_AssetAttributeType_insertdatetime DEFAULT (GETDATE()),
        asat_modifieddatetime  DATETIME      NULL,
        t_id                   INT           NOT NULL REFERENCES dbo.Trade (t_id),   -- parent trade
        asat_key               NVARCHAR(40)  NOT NULL,   -- the name used in condition JSON: {"attr":{"BeltDriven":true}}; permanent once used
        asat_label             NVARCHAR(100) NOT NULL,   -- what the office / tech sees
        asat_datatype          NVARCHAR(10)  NOT NULL,   -- Bool | Number | Text | List
        asat_values            NVARCHAR(400) NULL,       -- List: semicolon separated choices
        fq_code                NVARCHAR(12)  NULL,       -- template question that writes this attribute (fq_writesto = Asset), if any
        asat_repeatkey         NVARCHAR(15)  NULL,       -- Number attributes that count repeat groups: Circuit | Compressor | HeatStage | Phase | FilterBank | BeltDrive
        asat_order             INT           NOT NULL CONSTRAINT DF_AssetAttributeType_order DEFAULT (0),
        asat_active            BIT           NOT NULL CONSTRAINT DF_AssetAttributeType_active DEFAULT (1),
        CONSTRAINT UQ_AssetAttributeType_Trade_Key UNIQUE (t_id, asat_key)
    );
END;
GO

------------------------------------------------------------------------------------------------------
-- 2. Asset: new nullable identity columns (safe for the legacy EF model, which ignores unmapped columns)
------------------------------------------------------------------------------------------------------
IF COL_LENGTH('dbo.Asset', 'as_unittag') IS NULL              ALTER TABLE dbo.Asset ADD as_unittag NVARCHAR(50) NULL;                 -- F046 unit number / ID
IF COL_LENGTH('dbo.Asset', 'as_assettag') IS NULL             ALTER TABLE dbo.Asset ADD as_assettag NVARCHAR(50) NULL;                -- F047 customer asset tag
IF COL_LENGTH('dbo.Asset', 'as_id_connected') IS NULL         ALTER TABLE dbo.Asset ADD as_id_connected INT NULL CONSTRAINT FK_Asset_Connected REFERENCES dbo.Asset (as_id);  -- F048 connected AHU / condenser
IF COL_LENGTH('dbo.Asset', 'as_capacitytons') IS NULL         ALTER TABLE dbo.Asset ADD as_capacitytons DECIMAL(6,2) NULL;           -- F057 tonnage
IF COL_LENGTH('dbo.Asset', 'as_heatingtype') IS NULL          ALTER TABLE dbo.Asset ADD as_heatingtype NVARCHAR(30) NULL;             -- F059 (Heating Type list); read by heatingType conditions
IF COL_LENGTH('dbo.Asset', 'as_refrigeranttype') IS NULL      ALTER TABLE dbo.Asset ADD as_refrigeranttype NVARCHAR(20) NULL;         -- F060
IF COL_LENGTH('dbo.Asset', 'as_manufactureyear') IS NULL      ALTER TABLE dbo.Asset ADD as_manufactureyear SMALLINT NULL;             -- F054
IF COL_LENGTH('dbo.Asset', 'as_installdate') IS NULL          ALTER TABLE dbo.Asset ADD as_installdate DATE NULL;                     -- F055
IF COL_LENGTH('dbo.Asset', 'as_servedarea') IS NULL           ALTER TABLE dbo.Asset ADD as_servedarea NVARCHAR(100) NULL;             -- F061 area served
IF COL_LENGTH('dbo.Asset', 'aml_id') IS NULL                 ALTER TABLE dbo.Asset ADD aml_id INT NULL CONSTRAINT FK_Asset_AssetMountLocation REFERENCES dbo.AssetMountLocation (aml_id);  -- mounting; read by "mount" conditions
IF COL_LENGTH('dbo.Asset', 'as_mapx') IS NULL                 ALTER TABLE dbo.Asset ADD as_mapx DECIMAL(7,4) NULL;                    -- aerial pin, fraction of image width (later phase)
IF COL_LENGTH('dbo.Asset', 'as_mapy') IS NULL                 ALTER TABLE dbo.Asset ADD as_mapy DECIMAL(7,4) NULL;
IF COL_LENGTH('dbo.Asset', 'as_active') IS NULL               ALTER TABLE dbo.Asset ADD as_active BIT NOT NULL CONSTRAINT DF_Asset_active DEFAULT (1);  -- retire without delete
IF COL_LENGTH('dbo.Asset', 'as_lastverifieddatetime') IS NULL ALTER TABLE dbo.Asset ADD as_lastverifieddatetime DATETIME NULL;       -- stamped when a tech (later) or admin confirms the record
IF COL_LENGTH('dbo.Asset', 'u_id_lastverified') IS NULL       ALTER TABLE dbo.Asset ADD u_id_lastverified INT NULL;
IF COL_LENGTH('dbo.Asset', 'att_id_nameplate') IS NULL        ALTER TABLE dbo.Asset ADD att_id_nameplate INT NULL;                    -- P05 nameplate photo
GO

------------------------------------------------------------------------------------------------------
-- 3. Components and attributes on an asset
------------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'AssetComponent')
BEGIN
    CREATE TABLE dbo.AssetComponent (
        ascp_id                INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ascp_insertdatetime    DATETIME      NOT NULL CONSTRAINT DF_AssetComponent_insertdatetime DEFAULT (GETDATE()),
        ascp_modifieddatetime  DATETIME      NULL,
        as_id                  INT           NOT NULL REFERENCES dbo.Asset (as_id),
        asct_id                INT           NOT NULL REFERENCES dbo.AssetComponentType (asct_id),
        ascp_quantity          INT           NULL,       -- F072 / F074 / F083
        ascp_size              NVARCHAR(30)  NULL,       -- F071 / F073 / F082, e.g. 20x25x2, A42
        ascp_type              NVARCHAR(50)  NULL,       -- F075 Pleated / Panel / MERV 8 ...
        ascp_partnumber        NVARCHAR(50)  NULL,
        ascp_note              NVARCHAR(400) NULL,
        ascp_active            BIT           NOT NULL CONSTRAINT DF_AssetComponent_active DEFAULT (1)
    );
    CREATE INDEX IX_AssetComponent_Asset ON dbo.AssetComponent (as_id);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'AssetAttribute')
BEGIN
    CREATE TABLE dbo.AssetAttribute (
        asa_id                 INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        asa_insertdatetime     DATETIME      NOT NULL CONSTRAINT DF_AssetAttribute_insertdatetime DEFAULT (GETDATE()),
        asa_modifieddatetime   DATETIME      NULL,
        as_id                  INT           NOT NULL REFERENCES dbo.Asset (as_id),
        asat_id                INT           NOT NULL REFERENCES dbo.AssetAttributeType (asat_id),
        asa_value              NVARCHAR(400) NULL,       -- Bool: true/false; List/Text: the text; Number: the text form
        asa_numeric            DECIMAL(18,4) NULL,       -- Number attributes (repeat counts read this)
        att_id                 INT           NULL,       -- evidence photo, later
        u_id                   INT           NULL,       -- who last set it
        CONSTRAINT UQ_AssetAttribute_Asset_Type UNIQUE (as_id, asat_id)
    );
END;
GO

------------------------------------------------------------------------------------------------------
-- 4. Location trade profile (one per location per parent trade): the PM profile on the LOCATIONS tab
------------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'LocationTradeProfile')
BEGIN
    CREATE TABLE dbo.LocationTradeProfile (
        ltp_id                 INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ltp_insertdatetime     DATETIME      NOT NULL CONSTRAINT DF_LocationTradeProfile_insertdatetime DEFAULT (GETDATE()),
        ltp_modifieddatetime   DATETIME      NULL,
        l_id                   INT           NOT NULL REFERENCES dbo.Location (l_id),
        t_id                   INT           NOT NULL REFERENCES dbo.Trade (t_id),   -- parent trade
        ltp_unitcount          INT           NULL,       -- expected units; ticket entry compares against it
        ltp_accessrequirements NVARCHAR(200) NULL,       -- CSV of lar_id
        ltp_accessnote         NVARCHAR(1000) NULL,      -- required when an access requirement needs a note (Other)
        ltp_mountlocations      NVARCHAR(200) NULL,       -- CSV of aml_id
        att_id_aerial          INT           NULL,       -- aerial image attachment
        ltp_managername        NVARCHAR(100) NULL,
        ltp_managerphone       NVARCHAR(30)  NULL,
        ltp_note               NVARCHAR(2000) NULL,
        CONSTRAINT UQ_LocationTradeProfile_Location_Trade UNIQUE (l_id, t_id)
    );
END;
GO

------------------------------------------------------------------------------------------------------
-- 5. Seeds (insert-if-missing). HVAC parent trade is found by name; nothing is seeded if it is missing.
------------------------------------------------------------------------------------------------------
-- Unit locations: the names are the values "mount" conditions use ({"mount":["Rooftop","Stilts","Fenced area on the roof"]})
MERGE dbo.AssetMountLocation AS tgt
USING (VALUES (N'Ground', 1), (N'Rooftop', 2), (N'Stilts', 3), (N'Fenced area on the ground', 4), (N'Fenced area on the roof', 5), (N'Interior / mechanical room', 6), (N'Other', 7)) AS src (loc, ord)
ON tgt.aml_location = src.loc
WHEN NOT MATCHED THEN INSERT (aml_location, aml_order) VALUES (src.loc, src.ord);

MERGE dbo.LocationAccessRequirement AS tgt
USING (VALUES (N'Roof hatch', 0, 1), (N'Ladder', 0, 2), (N'Lift', 0, 3), (N'Key', 0, 4), (N'Locked area', 0, 5), (N'Escort', 0, 6), (N'Ceiling access', 0, 7), (N'Other', 1, 8)) AS src (req, needsnote, ord)
ON tgt.lar_requirement = src.req
WHEN NOT MATCHED THEN INSERT (lar_requirement, lar_requiresnote, lar_order) VALUES (src.req, src.needsnote, src.ord);
GO

DECLARE @hvac INT = (SELECT TOP 1 t_id FROM dbo.Trade WHERE t_trade = 'HVAC' AND t_parentonly = 1);
IF @hvac IS NULL
    PRINT 'HVAC parent trade not found: component types, attribute types and manufacturers not seeded.';
ELSE
BEGIN
    -- Component types (things that stay with the unit and are confirmed each visit)
    MERGE dbo.AssetComponentType AS tgt
    USING (VALUES (N'Filter', 1, 1, 1, 1), (N'Fresh-air Filter', 1, 1, 1, 2), (N'Belt', 1, 1, 0, 3)) AS src (typ, hassize, hasqty, hastype, ord)
    ON tgt.t_id = @hvac AND tgt.asct_type = src.typ
    WHEN NOT MATCHED THEN INSERT (t_id, asct_type, asct_hassize, asct_hasquantity, asct_hastype, asct_order) VALUES (@hvac, src.typ, src.hassize, src.hasqty, src.hastype, src.ord);

    -- Attribute types: yes/no features used by "attr" conditions, counts that drive repeat groups, and the
    -- asset-basics questions (fq_writesto = Asset) that are not core Asset columns.
    MERGE dbo.AssetAttributeType AS tgt
    USING (VALUES
        (N'BeltDriven',        N'Belt-driven blower',                N'Bool',   NULL, NULL,    NULL,         1),
        (N'Economizer',        N'Economizer / outside-air damper',   N'Bool',   NULL, N'F148', NULL,         2),
        (N'EMS',               N'EMS / BAS / DDC present',           N'Bool',   NULL, N'F146', NULL,         3),
        (N'RemoteSensors',     N'Remote temperature sensors',        N'Bool',   NULL, N'F144', NULL,         4),
        (N'BatteryThermostat', N'Battery-powered thermostat',        N'Bool',   NULL, NULL,    NULL,         5),
        (N'Ducted',            N'Ducted supply',                     N'Bool',   NULL, NULL,    NULL,         6),
        (N'FreshAirFilter',    N'Fresh-air filter fitted',           N'Bool',   NULL, NULL,    NULL,         7),
        (N'ThreePhase',        N'Three-phase power',                 N'Bool',   NULL, NULL,    NULL,         8),
        (N'CrankcaseHeater',   N'Crankcase heater',                  N'Bool',   NULL, NULL,    NULL,         9),
        (N'Glycol',            N'Glycol loop',                       N'Bool',   NULL, NULL,    NULL,         10),
        (N'CurbAdapter',       N'Curb adapter present',              N'Bool',   NULL, N'F064', NULL,         11),
        (N'Circuits',          N'Refrigerant circuits',              N'Number', NULL, NULL,    N'Circuit',   20),
        (N'Compressors',       N'Compressors',                       N'Number', NULL, NULL,    N'Compressor', 21),
        (N'HeatStages',        N'Heat stages',                       N'Number', NULL, NULL,    N'HeatStage', 22),
        (N'Phases',            N'Electrical phases (1 or 3)',        N'Number', NULL, NULL,    N'Phase',     23),
        (N'FilterBanks',       N'Filter banks',                      N'Number', NULL, NULL,    N'FilterBank', 24),
        (N'BeltDrives',        N'Belt drives',                       N'Number', NULL, NULL,    N'BeltDrive', 25),
        (N'SupplyVoltage',     N'Supply voltage',                    N'Text',   NULL, NULL,    NULL,         30),
        (N'RatedEfficiency',   N'Rated efficiency (SEER / EER)',     N'Text',   NULL, N'F058', NULL,         31),
        (N'SharedUnit',        N'Shared unit',                       N'List',   N'Yes;No;Unknown', N'F062', NULL, 32),
        (N'AirSuppliedBy',     N'Air / water supplied by',           N'List',   N'Customer;Mall;Landlord;VSS;Other', N'F063', NULL, 33),
        (N'Warranty',          N'Warranty period / status',          N'Text',   NULL, N'F065', NULL,         34),
        (N'LifeExpectancy',    N'Unit life expectancy (years)',      N'Number', NULL, N'F066', NULL,         35),
        (N'ThermostatModel',   N'Thermostat brand / model',          N'Text',   NULL, N'F133', NULL,         36)
    ) AS src (k, lbl, dt, vals, fq, rk, ord)
    ON tgt.t_id = @hvac AND tgt.asat_key = src.k
    WHEN NOT MATCHED THEN INSERT (t_id, asat_key, asat_label, asat_datatype, asat_values, fq_code, asat_repeatkey, asat_order)
        VALUES (@hvac, src.k, src.lbl, src.dt, src.vals, src.fq, src.rk, src.ord);

    -- Manufacturer lists from the seeded HVAC template (question F051 = general list, F051-1 = VRF, F051-2 = Chiller).
    -- F051-3 (Custom) and F051-4 (PTAC) have no equipment type to attach to and are left for the team (SEED_DECISIONS H).
    IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FormQuestion')
    BEGIN
        DECLARE @general NVARCHAR(MAX) = (SELECT TOP 1 fq_answervalues FROM dbo.FormQuestion q JOIN dbo.FormSection s ON s.fs_id = q.fs_id JOIN dbo.FormTemplate t ON t.ft_id = s.ft_id WHERE t.t_id = @hvac AND t.ft_active = 1 AND q.fq_code = 'F051');
        DECLARE @vrf     NVARCHAR(MAX) = (SELECT TOP 1 fq_answervalues FROM dbo.FormQuestion q JOIN dbo.FormSection s ON s.fs_id = q.fs_id JOIN dbo.FormTemplate t ON t.ft_id = s.ft_id WHERE t.t_id = @hvac AND t.ft_active = 1 AND q.fq_code = 'F051-1');
        DECLARE @chiller NVARCHAR(MAX) = (SELECT TOP 1 fq_answervalues FROM dbo.FormQuestion q JOIN dbo.FormSection s ON s.fs_id = q.fs_id JOIN dbo.FormTemplate t ON t.ft_id = s.ft_id WHERE t.t_id = @hvac AND t.ft_active = 1 AND q.fq_code = 'F051-2');

        -- general list -> the dictionary's equipment types except VRF (which has its own list)
        INSERT INTO dbo.AssetManufacturer (asc_id, asm_manufacturer)
        SELECT c.asc_id, v.name
        FROM dbo.AssetCategory c
        CROSS JOIN (SELECT DISTINCT LTRIM(RTRIM(value)) AS name FROM STRING_SPLIT(@general, ';') WHERE LTRIM(RTRIM(value)) <> '' AND LTRIM(RTRIM(value)) <> 'Other') v
        WHERE c.t_id = @hvac
          AND LTRIM(RTRIM(c.asc_category)) IN (N'RTU', N'Split System', N'AHU', N'Heat Pump', N'Water-Source Heat Pump', N'VAV', N'FPVAV', N'Fan Coil', N'Exhaust / Booster Fan', N'Refrigeration', N'Other')
          AND NOT EXISTS (SELECT 1 FROM dbo.AssetManufacturer m WHERE m.asc_id = c.asc_id AND LTRIM(RTRIM(m.asm_manufacturer)) = v.name);

        INSERT INTO dbo.AssetManufacturer (asc_id, asm_manufacturer)
        SELECT c.asc_id, v.name
        FROM dbo.AssetCategory c
        CROSS JOIN (SELECT DISTINCT LTRIM(RTRIM(value)) AS name FROM STRING_SPLIT(@vrf, ';') WHERE LTRIM(RTRIM(value)) <> '' AND LTRIM(RTRIM(value)) <> 'Other') v
        WHERE c.t_id = @hvac AND LTRIM(RTRIM(c.asc_category)) = N'VRF'
          AND NOT EXISTS (SELECT 1 FROM dbo.AssetManufacturer m WHERE m.asc_id = c.asc_id AND LTRIM(RTRIM(m.asm_manufacturer)) = v.name);

        INSERT INTO dbo.AssetManufacturer (asc_id, asm_manufacturer)
        SELECT c.asc_id, v.name
        FROM dbo.AssetCategory c
        CROSS JOIN (SELECT DISTINCT LEFT(LTRIM(RTRIM(value)), 50) AS name FROM STRING_SPLIT(@chiller, ';') WHERE LTRIM(RTRIM(value)) <> '' AND LTRIM(RTRIM(value)) <> 'Other') v   -- AssetManufacturer is NVARCHAR(50); one chiller name is longer and is cut to 50
        WHERE c.t_id = @hvac AND LTRIM(RTRIM(c.asc_category)) = N'Chiller'
          AND NOT EXISTS (SELECT 1 FROM dbo.AssetManufacturer m WHERE m.asc_id = c.asc_id AND LTRIM(RTRIM(m.asm_manufacturer)) = v.name);
    END;
END;
GO

PRINT 'Asset / location layer: tables and seeds in place.';
SELECT 'AssetMountLocation' AS [table], COUNT(*) AS [rows] FROM dbo.AssetMountLocation
UNION ALL SELECT 'LocationAccessRequirement', COUNT(*) FROM dbo.LocationAccessRequirement
UNION ALL SELECT 'AssetComponentType', COUNT(*) FROM dbo.AssetComponentType
UNION ALL SELECT 'AssetAttributeType', COUNT(*) FROM dbo.AssetAttributeType
UNION ALL SELECT 'AssetComponent', COUNT(*) FROM dbo.AssetComponent
UNION ALL SELECT 'AssetAttribute', COUNT(*) FROM dbo.AssetAttribute
UNION ALL SELECT 'LocationTradeProfile', COUNT(*) FROM dbo.LocationTradeProfile
UNION ALL SELECT 'AssetManufacturer (HVAC)', COUNT(*) FROM dbo.AssetManufacturer m JOIN dbo.AssetCategory c ON c.asc_id = m.asc_id WHERE c.t_id = (SELECT TOP 1 t_id FROM dbo.Trade WHERE t_trade = 'HVAC' AND t_parentonly = 1);
GO
