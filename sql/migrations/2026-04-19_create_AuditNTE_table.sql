-- NTE notification audit + control table. One row per (service request, channel, dedup key).
-- Ships with: EvoAPI. Idempotent. Run on TEST first, on PROD with the EvoAPI deploy, before
-- sql/migrations/2026-11-01_nte_notification_config_updates.sql.
--
-- Shaped like dbo.Audit (an_ prefix, identity key, GETDATE() insert time, description / detail /
-- error / servername) plus what the office needs to see for each text or email: who it was about,
-- where it was meant to go, where it actually went, the message, the spend numbers, and the outcome.
--
-- Dedup: unique (sr_id, an_channel, an_key). The scan inserts the row as 'Sending' BEFORE the send,
-- then updates it to 'Sent' / 'Failed', so a crash between send and log cannot double-send.
-- A retry reuses the row: an_attempts goes up, the earlier error text is kept behind the new one.
--
-- History: this table was first created on TEST as dbo.NotificationLog (nl_ columns) during the
-- 2026-11-01 smoke tests. PROD never had that table. The DROP below removes the TEST copy (six smoke
-- rows, nothing to keep) so the same script yields the same table in both environments.

IF OBJECT_ID('dbo.NotificationLog') IS NOT NULL
    DROP TABLE dbo.NotificationLog;
GO

IF OBJECT_ID('dbo.AuditNTE') IS NULL
BEGIN
    CREATE TABLE dbo.AuditNTE (
        an_id                 INT IDENTITY(1,1) NOT NULL,
        an_insertdatetime     DATETIME       NOT NULL CONSTRAINT DF_AuditNTE_insertdatetime DEFAULT (GETDATE()),
        an_modifieddatetime   DATETIME       NULL,            -- last status change / retry
        an_description        VARCHAR(8000)  NULL,            -- one readable line, e.g. 'SMS 100% to Ayman Abdulshaheed for SR 20261020-6783'
        an_detail             VARCHAR(MAX)   NULL,            -- JSON spend snapshot (labor, items, trip charge, check-ins)
        an_error              BIT            NOT NULL CONSTRAINT DF_AuditNTE_error DEFAULT (0),
        an_errormessage       VARCHAR(MAX)   NULL,            -- ACS / SMTP error, or the skip reason; earlier attempts kept after ' || earlier: '
        an_servername         VARCHAR(50)    NULL,
        an_channel            VARCHAR(10)    NOT NULL,        -- 'Sms' | 'Email'
        an_threshold          INT            NOT NULL,        -- 75 | 100
        an_key                VARCHAR(100)   NOT NULL,        -- dedup key: '{threshold}@{nte}|u{u_id}' (SMS) or '{threshold}@{nte}|z{z_id}' (email)
        an_status             VARCHAR(20)    NOT NULL,        -- 'Sending' | 'Sent' | 'Failed' | 'Skipped' | 'Reviewed'
        an_attempts           INT            NOT NULL CONSTRAINT DF_AuditNTE_attempts DEFAULT (1),
        an_sentdatetime       DATETIME       NULL,
        sr_id                 INT            NOT NULL,
        an_srnumber           VARCHAR(50)    NULL,
        u_id                  INT            NULL,            -- the tech it was about
        an_techname           VARCHAR(100)   NULL,
        z_id                  INT            NULL,            -- the tech's zone (email leg)
        an_zonename           VARCHAR(100)   NULL,
        an_intendedrecipient  VARCHAR(500)   NULL,            -- the tech's phone / the zone email it was meant for
        an_recipient          VARCHAR(500)   NULL,            -- where it actually went (override rows reroute it); NULL when skipped
        an_overridden         BIT            NOT NULL CONSTRAINT DF_AuditNTE_overridden DEFAULT (0),
        an_subject            VARCHAR(500)   NULL,            -- email only
        an_body               VARCHAR(MAX)   NULL,            -- the rendered message exactly as sent
        an_nte                DECIMAL(18,2)  NULL,
        an_spent              DECIMAL(18,2)  NULL,
        an_percent            DECIMAL(18,1)  NULL,
        CONSTRAINT PK_AuditNTE PRIMARY KEY CLUSTERED (an_id),
        CONSTRAINT UQ_AuditNTE_sr_channel_key UNIQUE (sr_id, an_channel, an_key)
    );

    CREATE INDEX IX_AuditNTE_insertdatetime ON dbo.AuditNTE (an_insertdatetime DESC);
    CREATE INDEX IX_AuditNTE_u_id           ON dbo.AuditNTE (u_id) INCLUDE (an_insertdatetime, an_status);
END;
GO

-- Seed config rows. Fill AcsConnectionString / AcsSmsFrom after this runs; EmailFrom, MaxOpenCheckInHours,
-- EnabledTechIds and the final template wording come from the 2026-11-01 script. Leave Enabled = false.
IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'Enabled')
BEGIN
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_insertdatetime, cs_description) VALUES
      (1, 'NteConfig', 'Enabled',                 'false',     GETDATE(), 'Master kill-switch for the NTE notification service'),
      (1, 'NteConfig', 'ScanIntervalMinutes',     '5',         GETDATE(), 'How often the background loop scans active service requests'),
      (1, 'NteConfig', 'Thresholds',              '75,100',    GETDATE(), 'CSV of percent thresholds that trigger a notification'),
      (1, 'NteConfig', 'AcsConnectionString',     '',          GETDATE(), 'Azure Communication Services connection string'),
      (1, 'NteConfig', 'AcsSmsFrom',              '',          GETDATE(), 'E.164 sender number for SMS, e.g. +15551234567'),
      (1, 'NteConfig', 'SmokeTestOverrideMobile', '',          GETDATE(), 'When non-empty, every SMS goes to these numbers (semicolon-separated) instead of the tech'),
      (1, 'NteConfig', 'SmokeTestOverrideEmail',  '',          GETDATE(), 'When non-empty, every zone email goes to these addresses (semicolon-separated) instead of the zone');
END;
GO

IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'NteTemplate' AND cs_identifier = 'Sms75Body')
BEGIN
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_insertdatetime, cs_description) VALUES
      (1, 'NteTemplate', 'Sms75Body',
       'SR {sr_number} is at {percent}% of NTE (${spent} / ${nte}). Contact the zone before exceeding.',
       GETDATE(), 'SMS body sent to tech when NTE utilization crosses 75%'),
      (1, 'NteTemplate', 'Sms100Body',
       'SR {sr_number} has reached 100% of NTE (${spent} / ${nte}). Stop work and get customer approval.',
       GETDATE(), 'SMS body sent to tech when NTE utilization reaches 100%'),
      (1, 'NteTemplate', 'Email100Subject',
       'NTE Reached: SR {sr_number} ({percent}%)',
       GETDATE(), 'Email subject sent to zone when NTE utilization reaches 100%'),
      (1, 'NteTemplate', 'Email100Body',
       '<p>Service Request <strong>{sr_number}</strong> assigned to {tech_name} has reached <strong>{percent}%</strong> of its Not-To-Exceed amount.</p><p>NTE: ${nte}<br/>Current spend: ${spent}<br/>Zone: {zone}</p><p>Customer approval is required before further work proceeds.</p>',
       GETDATE(), 'Email HTML body sent to zone when NTE utilization reaches 100%');
END;
GO
