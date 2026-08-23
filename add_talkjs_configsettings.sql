-- TalkJS credentials for the Chat Admin page (EvoApi/chat endpoints).
-- Run in EACH environment's database with that environment's values:
--   TEST DB:  AppId = test app id (tqOaGSeo),  SecretKey = sk_test_... key
--   PROD DB:  AppId = live app id (8DpaXjD0),  SecretKey = sk_live_... key
-- Values are in the TalkJS dashboard under Settings, and also in evotech's
-- .env files (NEXT_PUBLIC_APP_ID / TALKJS_SECRET_KEY).
-- REPLACE THE PLACEHOLDERS BELOW BEFORE RUNNING.

IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'talkjs' AND cs_identifier = 'AppId')
BEGIN
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_description)
    VALUES (1, 'talkjs', 'AppId', 'REPLACE_WITH_APP_ID', 'TalkJS application id for this environment');
END;
GO

IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'talkjs' AND cs_identifier = 'SecretKey')
BEGIN
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_description)
    VALUES (1, 'talkjs', 'SecretKey', 'REPLACE_WITH_SECRET_KEY', 'TalkJS REST API secret key for this environment');
END;
GO
