-- PROD deploy preflight for the 2608-changes EvoAPI release. Read-only; safe on TEST or PROD.
-- Run BEFORE the deploy to see which pending migrations (sql/README.md "Pending for PROD") are
-- still missing, and AFTER running them to confirm every line says OK.
--
-- Also sanity-checks the earlier releases the code already depends on (markup decimal, status-change
-- user tracking, attachment geolocation, customer inquiry). Those should already be OK on PROD.

SET NOCOUNT ON;

SELECT * FROM (VALUES
    -- Earlier releases the code already assumes (expected OK on PROD already)
    ('earlier', '2026-07-18 markup decimal',          CASE WHEN EXISTS (SELECT 1 FROM sys.columns c JOIN sys.types t ON t.user_type_id = c.user_type_id WHERE c.object_id = OBJECT_ID('dbo.LaborRate') AND c.name = 'lr_markup' AND t.name = 'decimal') THEN 'OK' ELSE 'MISSING - lr_markup is not decimal; see 2026-07-18_alter_markup_decimal_and_markuptype.sql' END),
    ('earlier', '2026-08-16 CustomerInquiry',         CASE WHEN OBJECT_ID('dbo.CustomerInquiry') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),
    ('earlier', '2026-08-18 AttackPointCustomerInquiry', CASE WHEN OBJECT_ID('dbo.AttackPointCustomerInquiry') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),
    ('earlier', '2026-08-22 StatusChange.u_id',       CASE WHEN COL_LENGTH('dbo.StatusChange', 'u_id') IS NOT NULL AND COL_LENGTH('dbo.StatusSecondaryChange', 'u_id') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),
    ('earlier', '2026-08-30 Attachment geolocation',  CASE WHEN COL_LENGTH('dbo.Attachment', 'att_latitude') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),
    ('earlier', '2026-07-19 Performance tables',      CASE WHEN OBJECT_ID('dbo.PerformanceUpload') IS NOT NULL AND OBJECT_ID('dbo.PerformanceZone') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),

    -- Pending for this release, in run order
    ('pending', '2026-09-08 form tables',             CASE WHEN OBJECT_ID('dbo.FormTemplate') IS NOT NULL AND OBJECT_ID('dbo.PMRule') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),
    ('pending', '2026-09-08 asset tables',            CASE WHEN OBJECT_ID('dbo.AssetComponent') IS NOT NULL AND OBJECT_ID('dbo.LocationTradeProfile') IS NOT NULL AND COL_LENGTH('dbo.Asset', 'as_unittag') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),
    ('pending', '2026-09-10 XRF tables',              CASE WHEN OBJECT_ID('dbo.XrfBatch') IS NOT NULL AND COL_LENGTH('dbo.XrfBatchDetail', 'xrfbd_premisenumber') IS NOT NULL AND COL_LENGTH('dbo.XrfBatchDetail', 'xrfbd_att_id') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),
    ('pending', '2026-09-15 XRF Not Used result',     CASE WHEN OBJECT_ID('dbo.XrfBatchDetail') IS NULL THEN 'MISSING (tables first)' WHEN EXISTS (SELECT 1 FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID('dbo.XrfBatchDetail') AND definition LIKE '%XRF Not Used%') THEN 'OK' ELSE 'MISSING' END),
    ('pending', '2026-09-16 LaborRate unique index',  CASE WHEN EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.LaborRate') AND name = 'UX_LaborRate_xccc_id_t_id') THEN 'OK' ELSE 'MISSING (ships with EvoWS/EvoUI; clean duplicates first)' END),
    ('pending', '2026-09-20 Attachment.l_id',         CASE WHEN COL_LENGTH('dbo.Attachment', 'l_id') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),
    ('pending', '2026-09-19 Metro Pipe public key',   CASE WHEN EXISTS (SELECT 1 FROM dbo.ConfigSetting WHERE cs_identifier = 'PublicReportKey' AND ISNULL(cs_value, '') <> '') THEN 'OK' ELSE 'MISSING' END),
    ('pending', '2026-09-24 PM ticket tables',        CASE WHEN OBJECT_ID('dbo.ServiceType') IS NOT NULL AND OBJECT_ID('dbo.PMVisit') IS NOT NULL AND COL_LENGTH('dbo.ServiceRequest', 'svt_id') IS NOT NULL AND COL_LENGTH('dbo.Priority', 'p_allowpreventative') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),
    ('pending', '2026-09-25 PMVisitAnswer',           CASE WHEN OBJECT_ID('dbo.PMVisitAnswer') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),
    ('pending', '2026-09-25 PMVisitFinding',          CASE WHEN OBJECT_ID('dbo.PMVisitFinding') IS NOT NULL THEN 'OK' ELSE 'MISSING' END),
    ('pending', '2026-04-19 AuditNTE + NteConfig',    CASE WHEN OBJECT_ID('dbo.AuditNTE') IS NOT NULL AND EXISTS (SELECT 1 FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'Enabled') THEN 'OK' ELSE 'MISSING' END),
    ('pending', '2026-11-01 NTE config updates',      CASE WHEN EXISTS (SELECT 1 FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'EnabledTechIds') THEN 'OK' ELSE 'MISSING' END),

    -- Hand-set values the NTE service needs before Enabled goes to true
    ('nte', 'NteConfig/Enabled',                      ISNULL((SELECT cs_value FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'Enabled'), '(row missing)')),
    ('nte', 'NteConfig/AcsConnectionString',          CASE WHEN EXISTS (SELECT 1 FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'AcsConnectionString' AND ISNULL(cs_value, '') <> '') THEN 'set' ELSE 'EMPTY - copy from TEST before enabling' END),
    ('nte', 'NteConfig/AcsSmsFrom',                   ISNULL((SELECT cs_value FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'AcsSmsFrom'), '(row missing)')),
    ('nte', 'NteConfig/SmokeTestOverrideMobile',      ISNULL((SELECT cs_value FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'SmokeTestOverrideMobile'), '(row missing)')),
    ('nte', 'NteConfig/SmokeTestOverrideEmail',       ISNULL((SELECT cs_value FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'SmokeTestOverrideEmail'), '(row missing)')),
    ('nte', 'EmailServer/Port (optional)',            ISNULL((SELECT cs_value FROM dbo.ConfigSetting WHERE cs_type = 'EmailServer' AND cs_identifier = 'Port'), '(not set; code defaults to 587)'))
) AS v([group], item, status);
