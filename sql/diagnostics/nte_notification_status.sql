-- NTE notification service: one-shot status check. Read-only, safe on TEST or PROD.
-- Answers: did the migrations run here, what are the NteConfig / NteTemplate values, and what
-- has the service logged so far. The ACS connection string is masked; only its length is shown.
-- For the full audit view use nte_audit_recent.sql.

SET NOCOUNT ON;

-- 1) Migrations applied?
SELECT 'AuditNTE table' AS item,
       CASE WHEN OBJECT_ID('dbo.AuditNTE') IS NULL THEN 'MISSING - run sql/migrations/2026-04-19_create_AuditNTE_table.sql' ELSE 'EXISTS' END AS status
UNION ALL
SELECT 'EnabledTechIds row',
       CASE WHEN EXISTS (SELECT 1 FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'EnabledTechIds')
            THEN 'EXISTS' ELSE 'MISSING - run sql/migrations/2026-11-01_nte_notification_config_updates.sql' END
UNION ALL
SELECT 'Old NotificationLog table',
       CASE WHEN OBJECT_ID('dbo.NotificationLog') IS NULL THEN 'gone (expected)' ELSE 'STILL PRESENT - rerun the 2026-04-19 script' END;

-- 2) Config + templates (10 NteConfig rows and 4 NteTemplate rows once both scripts have run)
SELECT cs_type, cs_identifier,
       CASE WHEN cs_identifier = 'AcsConnectionString'
            THEN CASE WHEN ISNULL(cs_value, '') = '' THEN '(empty)' ELSE '(set, ' + CAST(LEN(cs_value) AS varchar(10)) + ' chars)' END
            ELSE cs_value END AS cs_value
FROM dbo.ConfigSetting
WHERE cs_type IN ('NteConfig', 'NteTemplate')
ORDER BY cs_type, cs_identifier;

-- 3) Anything logged yet? (empty is expected until the first scan with Enabled = 'true')
IF OBJECT_ID('dbo.AuditNTE') IS NOT NULL
    SELECT TOP 50 an_id, an_insertdatetime, an_status, an_channel, an_threshold, an_srnumber, an_techname,
           an_intendedrecipient, an_recipient, an_overridden, an_errormessage
    FROM dbo.AuditNTE
    ORDER BY an_id DESC;
