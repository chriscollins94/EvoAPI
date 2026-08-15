/*
  NTE Guidance feature — schema + settings.
  Run once against the Evo database.

  1) Per-company opt-in flag (default OFF) so existing companies are unaffected
     until someone turns it on under Office -> Settings -> Companies -> General Info.
  2) Tunable settings (percentile / look-back window / minimum jobs) stored in
     ConfigSetting so they can be changed without a redeploy.
*/

-- 1) Opt-in flag on the company / call-center pairing
IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE Name = N'xccc_nteguidance' AND Object_ID = OBJECT_ID(N'dbo.xrefCompanyCallCenter')
)
BEGIN
    ALTER TABLE dbo.xrefCompanyCallCenter
        ADD xccc_nteguidance BIT NOT NULL
        CONSTRAINT DF_xrefCompanyCallCenter_nteguidance DEFAULT (0);
END
GO

-- 2) Guidance settings (o_id = 1 is required on ConfigSetting)
IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'Config' AND cs_identifier = 'NteGuidancePercentile')
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_description, cs_insertdatetime)
    VALUES (1, 'Config', 'NteGuidancePercentile', '75', 'Percentile used for the New Service Request NTE guidance estimate (e.g. 75 = covers ~3 of 4 past jobs)', GETDATE());

IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'Config' AND cs_identifier = 'NteGuidanceWindowMonths')
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_description, cs_insertdatetime)
    VALUES (1, 'Config', 'NteGuidanceWindowMonths', '12', 'Look-back window in months for NTE guidance history', GETDATE());

IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'Config' AND cs_identifier = 'NteGuidanceMinJobs')
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_description, cs_insertdatetime)
    VALUES (1, 'Config', 'NteGuidanceMinJobs', '5', 'Minimum number of historical jobs before an NTE estimate is shown', GETDATE());
GO
