-- Performance upload tables (replaces legacy dbo.Performance once fully live).
--
-- Each Excel upload creates one PerformanceUpload batch row plus one detail row
-- per employee (PerformanceEmployee) or per zone (PerformanceZone). Every batch
-- is kept as history; "current" data = rows from the latest batch of each type
-- (by pfu_reportdate, then pfu_insertdatetime).
--
-- Metric targets live in ConfigSetting (cs_type = 'PerformanceTarget'), seeded
-- below; edited from the admin Performance page, never auto-updated by uploads.
-- Metric display names / formats / higher-vs-lower-is-better live in code.
--
-- This script only runs on a database that has never had these tables. For an
-- existing database, add_performance_zone_columns.sql adds the two zone columns
-- introduced with the trimmed zone workbook format -- keep the two in sync.

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PerformanceUpload')
BEGIN
    CREATE TABLE dbo.PerformanceUpload (
        pfu_id             INT IDENTITY(1,1) PRIMARY KEY,
        pfu_type           VARCHAR(20)   NOT NULL,        -- 'Employee' | 'Zone'
        pfu_reportdate     DATE          NOT NULL,        -- date the report data represents (from filename/UI)
        pfu_filename       NVARCHAR(255) NULL,
        u_id               INT           NOT NULL,        -- uploader
        pfu_rowcount       INT           NOT NULL DEFAULT 0,
        pfu_skippedcount   INT           NOT NULL DEFAULT 0,
        pfu_insertdatetime DATETIME      NOT NULL DEFAULT GETDATE()
    );

    CREATE INDEX IX_PerformanceUpload_Type_Date ON dbo.PerformanceUpload(pfu_type, pfu_reportdate DESC, pfu_insertdatetime DESC);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PerformanceEmployee')
BEGIN
    CREATE TABLE dbo.PerformanceEmployee (
        pe_id                  INT IDENTITY(1,1) PRIMARY KEY,
        pfu_id                 INT NOT NULL REFERENCES dbo.PerformanceUpload(pfu_id) ON DELETE CASCADE,
        u_id                   INT NOT NULL,
        pe_utilization         DECIMAL(18,6) NULL,        -- fraction, e.g. 0.538 = 53.8%
        pe_achlabortrip        DECIMAL(18,6) NULL,        -- $/hr
        pe_callouts            DECIMAL(18,6) NULL,
        pe_serviceitemspayback DECIMAL(18,6) NULL,
        pe_truckfuelefficiency DECIMAL(18,6) NULL,        -- $/day
        pe_gallonsperday       DECIMAL(18,6) NULL,
        pe_grossmargin         DECIMAL(18,6) NULL,        -- fraction
        pe_receiptsviolations  DECIMAL(18,6) NULL,
        pe_callbacks           DECIMAL(18,6) NULL,
        pe_pendingtechinfo     DECIMAL(18,6) NULL,
        pe_positiveqtrpct      DECIMAL(18,6) NULL,        -- fraction
        pe_profitgrade         VARCHAR(4)    NULL,        -- letter grade from file, stored as-is
        pe_healthscore         DECIMAL(18,6) NULL,        -- from file; NULL when file says N/A
        pe_insertdatetime      DATETIME      NOT NULL DEFAULT GETDATE(),
        CONSTRAINT UQ_PerformanceEmployee_Batch_User UNIQUE (pfu_id, u_id)
    );

    CREATE INDEX IX_PerformanceEmployee_User ON dbo.PerformanceEmployee(u_id, pfu_id);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PerformanceZone')
BEGIN
    CREATE TABLE dbo.PerformanceZone (
        pz_id                  INT IDENTITY(1,1) PRIMARY KEY,
        pfu_id                 INT NOT NULL REFERENCES dbo.PerformanceUpload(pfu_id) ON DELETE CASCADE,
        z_id                   INT NOT NULL,
        pz_utilization         DECIMAL(18,6) NULL,
        pz_achlabortrip        DECIMAL(18,6) NULL,
        pz_callouts            DECIMAL(18,6) NULL,
        pz_serviceitemspayback DECIMAL(18,6) NULL,
        pz_truckfuelefficiency DECIMAL(18,6) NULL,
        pz_gallonsperday       DECIMAL(18,6) NULL,
        pz_grossmargin         DECIMAL(18,6) NULL,
        pz_receiptsviolations  DECIMAL(18,6) NULL,
        pz_callbacks           DECIMAL(18,6) NULL,
        pz_pendingtechinfo     DECIMAL(18,6) NULL,
        pz_positiveqtrpct      DECIMAL(18,6) NULL,
        pz_revpertechperday    DECIMAL(18,6) NULL,        -- $/tech/day; zone file only, untargeted
        pz_ytdcontribution     DECIMAL(18,6) NULL,        -- cumulative $, often negative; zone file only, untargeted
        pz_profitgrade         VARCHAR(4)    NULL,        -- retired: the zone file no longer has a grade column
        pz_insertdatetime      DATETIME      NOT NULL DEFAULT GETDATE(),
        CONSTRAINT UQ_PerformanceZone_Batch_Zone UNIQUE (pfu_id, z_id)
    );

    CREATE INDEX IX_PerformanceZone_Zone ON dbo.PerformanceZone(z_id, pfu_id);
END;
GO

-- Seed metric targets (values from the Zone workbook target row). Manual edits
-- only from here on — uploads never touch these.
IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'PerformanceTarget')
BEGIN
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_description)
    VALUES
        (1, 'PerformanceTarget', 'Perf.Utilization',          '0.596',    'Performance target: Utilization (fraction; higher is better)'),
        (1, 'PerformanceTarget', 'Perf.AchLaborTrip',         '60',       'Performance target: Ach Labor + Trip $/hr (higher is better)'),
        (1, 'PerformanceTarget', 'Perf.CallOuts',             '2.8',      'Performance target: Call Outs (lower is better)'),
        (1, 'PerformanceTarget', 'Perf.ServiceItemsPayback',  '2.5',      'Performance target: Service Items Payback (higher is better)'),
        (1, 'PerformanceTarget', 'Perf.TruckFuelEfficiency',  '25',       'Performance target: Truck/Fuel Efficiency $/day (lower is better)'),
        (1, 'PerformanceTarget', 'Perf.GallonsPerDay',        '10',       'Performance target: Gallons Per Day (lower is better)'),
        (1, 'PerformanceTarget', 'Perf.GrossMargin',          '0.4',      'Performance target: Gross Margin (fraction; higher is better)'),
        (1, 'PerformanceTarget', 'Perf.ReceiptsViolations',   '5.6',      'Performance target: Receipts Violations (lower is better)'),
        (1, 'PerformanceTarget', 'Perf.Callbacks',            '1.0769',   'Performance target: Callbacks (lower is better)'),
        (1, 'PerformanceTarget', 'Perf.PendingTechInfo',      '4.3077',   'Performance target: Pending Tech Info - Basic (lower is better)'),
        (1, 'PerformanceTarget', 'Perf.PositiveQtrPct',       '0.6',      'Performance target: Positive Qtr % (fraction; higher is better)'),
        (1, 'PerformanceTarget', 'Perf.JobMixLookbackDays',   '365',      'Job mix pie charts: include service requests created in the last N days');
END;
GO
