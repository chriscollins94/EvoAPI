-- Customer Inquiry log. Written from the office Work Order list "CI" button.
--
-- Each row is a point-in-time snapshot: the secondary status the primary work order
-- was in when the inquiry was logged, and how long it had been in that status
-- (derived from the latest StatusSecondaryChange row for that work order).
-- A matching WorkOrderNote is written in the same transaction by the API.

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'CustomerInquiry')
BEGIN
    CREATE TABLE dbo.CustomerInquiry (
        ci_id                  INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        sr_id                  INT           NOT NULL,   -- service request the inquiry is about
        wo_id                  INT           NULL,       -- primary work order the status/note hang off
        u_id                   INT           NOT NULL,   -- user who submitted the inquiry
        ss_id                  INT           NULL,       -- secondary status at time of inquiry (workorder.ss_id)
        ci_statusstartdatetime DATETIME      NULL,       -- when the current status began (max ssc_insertdatetime)
        ci_minutesinstatus     INT           NULL,       -- snapshot of time in that status, in minutes
        ci_note                NVARCHAR(MAX) NULL,       -- optional note typed by the user
        ci_active              BIT           NOT NULL CONSTRAINT DF_CustomerInquiry_active DEFAULT ((1)),
        ci_insertdatetime      DATETIME      NOT NULL CONSTRAINT DF_CustomerInquiry_insertdatetime DEFAULT (GETUTCDATE()),
        ci_modifieddatetime    DATETIME      NULL
    );

    CREATE INDEX IX_CustomerInquiry_sr ON dbo.CustomerInquiry (sr_id, ci_insertdatetime DESC);
    CREATE INDEX IX_CustomerInquiry_user ON dbo.CustomerInquiry (u_id, ci_insertdatetime DESC);
END;
GO

-- Minimum hours between inquiries on the same service request, so repeat calls or two
-- people acting on the same email do not produce duplicate rows. The API falls back to
-- 24 if this row is missing or unparseable. ConfigSetting is read per request, but
-- restart EvoAPI after changing it if the value appears not to take effect.
IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'CustomerInquiry' AND cs_identifier = 'CooldownHours')
BEGIN
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_insertdatetime, cs_description)
    VALUES (1, 'CustomerInquiry', 'CooldownHours', '24', GETDATE(),
            'Minimum hours between customer inquiry entries on the same service request');
END;
GO
