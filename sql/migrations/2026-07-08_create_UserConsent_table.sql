-- Generic user consent table. SMS NTE budget alerts (CTIA/TFV opt-in proof) is the
-- first consumer; future consent types reuse the same table with a different
-- uc_consenttype string. No lookup table — types are code constants.
--
-- Append-only: never UPDATE rows. The latest row per (u_id, uc_consenttype,
-- uc_disclosureversion) is the current decision; older rows are the audit trail.

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'UserConsent')
BEGIN
    CREATE TABLE dbo.UserConsent (
        uc_id                INT IDENTITY(1,1) PRIMARY KEY,
        u_id                 INT           NOT NULL,
        uc_consenttype       NVARCHAR(50)  NOT NULL,        -- e.g. 'NteSmsAlerts'
        uc_contactvalue      NVARCHAR(100) NOT NULL,        -- number/address consented to; E.164 for SMS, e.g. +16155550123
        uc_status            NVARCHAR(20)  NOT NULL,        -- 'OptedIn' | 'Declined' | 'OptedOut' (only OptedIn written in v1)
        uc_disclosureversion NVARCHAR(10)  NOT NULL,        -- version of the disclosure copy shown, e.g. 'v1'
        uc_source            NVARCHAR(50)  NOT NULL,        -- e.g. 'connect_app_optin_screen'
        uc_ipaddress         NVARCHAR(50)  NULL,
        uc_useragent         NVARCHAR(500) NULL,
        uc_insertdatetime    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_UserConsent_User_Type ON dbo.UserConsent(u_id, uc_consenttype, uc_disclosureversion);
END;
GO
