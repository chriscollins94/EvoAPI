-- Generic notification log. NTE is the first consumer; future notification types
-- (escalations, password expiry warnings, schedule reminders, etc.) reuse the same table.
--
-- Dedup is enforced by the unique constraint on
-- (nl_type, nl_key, nl_entity_type, nl_entity_id, nl_channel).
-- Insert with status 'Sending' BEFORE the send, then update to 'Sent' / 'Failed'
-- so a crash between send and log cannot double-send.

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'NotificationLog')
BEGIN
    CREATE TABLE dbo.NotificationLog (
        nl_id              INT IDENTITY(1,1) PRIMARY KEY,
        nl_type            NVARCHAR(100) NOT NULL,        -- e.g. 'NteThresholdCrossed'
        nl_key             NVARCHAR(100) NULL,            -- type-specific dedup key, e.g. '75' / '100'
        nl_entity_type     NVARCHAR(50)  NULL,            -- e.g. 'ServiceRequest', 'WorkOrder', 'User'
        nl_entity_id       INT           NULL,            -- the related entity id
        nl_channel         NVARCHAR(20)  NOT NULL,        -- 'Sms' | 'Email'
        nl_recipient       NVARCHAR(500) NOT NULL,        -- phone number or email actually sent to
        nl_subject         NVARCHAR(500) NULL,            -- email subject (null for SMS)
        nl_body            NVARCHAR(MAX) NULL,            -- rendered message body
        nl_send_status     NVARCHAR(20)  NOT NULL,        -- 'Sending' | 'Sent' | 'Failed' | 'Skipped'
        nl_send_error      NVARCHAR(MAX) NULL,
        nl_metadata        NVARCHAR(MAX) NULL,            -- JSON for type-specific snapshot data
        nl_insertdatetime  DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_NotificationLog_Type_Key_Entity_Channel
            UNIQUE (nl_type, nl_key, nl_entity_type, nl_entity_id, nl_channel)
    );

    CREATE INDEX IX_NotificationLog_Entity ON dbo.NotificationLog(nl_entity_type, nl_entity_id);
    CREATE INDEX IX_NotificationLog_Type   ON dbo.NotificationLog(nl_type, nl_insertdatetime);
END;
GO

-- Seed default config rows. Update AcsConnectionString / AcsSmsFrom / AcsEmailFrom in
-- ConfigSetting after this runs. Leave Enabled = false until smoke-tested.
IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'Enabled')
BEGIN
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_insertdatetime, cs_description) VALUES
      (1, 'NteConfig', 'Enabled',                 'false',     GETDATE(), 'Master kill-switch for the NTE notification service'),
      (1, 'NteConfig', 'ScanIntervalMinutes',     '15',        GETDATE(), 'How often the background loop scans active service requests'),
      (1, 'NteConfig', 'Thresholds',              '75,100',    GETDATE(), 'CSV of percent thresholds that trigger a notification'),
      (1, 'NteConfig', 'AcsConnectionString',     '',          GETDATE(), 'Azure Communication Services connection string'),
      (1, 'NteConfig', 'AcsSmsFrom',              '',          GETDATE(), 'E.164 sender number for SMS, e.g. +15551234567'),
      (1, 'NteConfig', 'AcsEmailFrom',            '',          GETDATE(), 'Verified ACS sender email address'),
      (1, 'NteConfig', 'SmokeTestOverrideMobile', '',          GETDATE(), 'When non-empty, all SMS routes here instead of the tech mobile'),
      (1, 'NteConfig', 'SmokeTestOverrideEmail',  '',          GETDATE(), 'When non-empty, all zone emails route here instead of the zone email');
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
