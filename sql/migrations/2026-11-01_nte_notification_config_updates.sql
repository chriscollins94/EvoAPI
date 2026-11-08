-- NTE notification service: config rows for the completed implementation (NTE_NOTIFICATION_PLAN.md).
-- Ships with: EvoAPI. Run on TEST before the smoke test, on PROD with the EvoAPI deploy.
-- Idempotent. Requires sql/migrations/2026-04-19_create_AuditNTE_table.sql to have run first.
--
-- 1) D8 - open check-ins older than this many hours are ignored by the spend calculator.
-- 2) D5 - scan every 5 minutes.
-- 3) D1 - zone email goes through the shared SMTP sender (same mailbox as the time-off emails);
--         NteConfig/EmailFrom replaces NteConfig/AcsEmailFrom.
-- 5) Rollout valve (2026-11-07): NteConfig/EnabledTechIds - comma-separated user ids that may be
--    notified. Empty = every tech. Techs not listed are skipped without writing an AuditNTE row,
--    so they are notified normally once the list is cleared. Override rows now take semicolon lists.

IF NOT EXISTS (SELECT 1 FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'EmailFrom')
    INSERT INTO dbo.ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_insertdatetime, cs_description)
    SELECT 1, 'NteConfig', 'EmailFrom', ISNULL((SELECT TOP 1 cs_value FROM dbo.ConfigSetting WHERE cs_type = 'EmailTimeOff' AND cs_identifier = 'From'), ''),
           GETDATE(), 'From address for the NTE zone email; must be the mailbox whose password is in EmailServer/FromPassword';

DELETE FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'AcsEmailFrom';

IF NOT EXISTS (SELECT 1 FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'MaxOpenCheckInHours')
    INSERT INTO dbo.ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_insertdatetime, cs_description)
    VALUES (1, 'NteConfig', 'MaxOpenCheckInHours', '24', GETDATE(),
            'Open check-ins older than this many hours are treated as a forgotten check-out and ignored by the NTE spend calculation');

UPDATE dbo.ConfigSetting SET cs_value = '5'
 WHERE cs_type = 'NteConfig' AND cs_identifier = 'ScanIntervalMinutes' AND cs_value = '15';

IF NOT EXISTS (SELECT 1 FROM dbo.ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'EnabledTechIds')
    INSERT INTO dbo.ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_insertdatetime, cs_description)
    VALUES (1, 'NteConfig', 'EnabledTechIds', '', GETDATE(),
            'Rollout valve: comma-separated user ids (Employee Directory) that receive NTE texts/emails. Empty = all techs.');

UPDATE dbo.ConfigSetting
   SET cs_description = 'When non-empty, every SMS goes to these numbers (semicolon-separated) instead of the tech'
 WHERE cs_type = 'NteConfig' AND cs_identifier = 'SmokeTestOverrideMobile';

UPDATE dbo.ConfigSetting
   SET cs_description = 'When non-empty, every zone email goes to these addresses (semicolon-separated) instead of the zone'
 WHERE cs_type = 'NteConfig' AND cs_identifier = 'SmokeTestOverrideEmail';

-- 4) D6 - message wording approved 2026-11-01. Uses the {customer} / {location} tokens added in the same
--    EvoAPI release, so run with that deploy. The office can edit these rows later without a redeploy.
UPDATE dbo.ConfigSetting
   SET cs_value = '{tech_firstname}, SR {sr_number} is at {percent}% of NTE (${spent} of ${nte}). Stop and call the office before continuing.'
 WHERE cs_type = 'NteTemplate' AND cs_identifier = 'Sms75Body';

UPDATE dbo.ConfigSetting
   SET cs_value = '{tech_firstname}, SR {sr_number} has hit 100% of NTE (${spent} of ${nte}). Stop work now and call the office.'
 WHERE cs_type = 'NteTemplate' AND cs_identifier = 'Sms100Body';

UPDATE dbo.ConfigSetting
   SET cs_value = 'NTE reached: SR {sr_number} ({tech_name})'
 WHERE cs_type = 'NteTemplate' AND cs_identifier = 'Email100Subject';

UPDATE dbo.ConfigSetting
   SET cs_value = '<p>Service Request <strong>{sr_number}</strong> for {customer} at {location}, worked by {tech_name}, has reached <strong>{percent}%</strong> of its Not-To-Exceed amount.</p><p>NTE: ${nte}<br/>Current spend: ${spent} (labor ${labor}, parts ${service_items}, trip charge ${trip_charge})<br/>Zone: {zone}</p><p>Customer approval is required before further work proceeds.</p>'
 WHERE cs_type = 'NteTemplate' AND cs_identifier = 'Email100Body';
GO
