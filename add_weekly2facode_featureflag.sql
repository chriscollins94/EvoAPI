-- Feature flag for the weekly rotating 3-digit login code (u_2fa users).
-- '0' = disabled: u_2fa users log in with just their password, no code appended.
-- '1' = enabled: legacy behavior — u_2fa users must append the weekly code to their password.
-- Missing row also counts as enabled (fail-safe), so this row existing with '0' is what turns it off.
-- To re-enable later: UPDATE ConfigSetting SET cs_value = '1' WHERE cs_type = 'featureflag' AND cs_identifier = 'Weekly2faCode'

IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'featureflag' AND cs_identifier = 'Weekly2faCode')
BEGIN
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_description)
    VALUES (1, 'featureflag', 'Weekly2faCode', '0', 'Require weekly rotating 3-digit code appended to password for users with u_2fa = 1 (0 = off, 1 = on)');
END;
GO
