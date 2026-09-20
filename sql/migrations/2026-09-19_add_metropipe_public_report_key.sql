-- Shared key for the public (no-login) copy of the Metro Pipe Program Report.
--
-- Outside viewers open  https://<evotech host>/evotech/reports/metro-pipe/?k=<key>
-- and the page passes the key to EvoAPI (PublicReportsController) in a request header; the API
-- serves the report only when it matches this value, compared case-sensitively and in constant
-- time. A wrong key gets a 404. The API caches the value for one minute,
-- so rotating the key is an UPDATE and old links die within a minute.
--
-- The key is GENERATED HERE, at run time, so TEST and PROD end up with different values and no
-- secret is committed to the repository. The script PRINTs the key it inserted: copy it from the
-- Messages tab and hand it to whoever needs the link. Safe to rerun; an existing key is kept.
--
-- Ships with: EvoAPI + evotech. Run on TEST first, then PROD with the deploy.

IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'MetroPipe' AND cs_identifier = 'PublicReportKey')
BEGIN
    -- 8 characters from an alphabet without look-alikes (no 0/O, 1/l/I). 55^8 possibilities.
    DECLARE @alphabet varchar(60) = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789';
    DECLARE @key varchar(8) = '';
    DECLARE @i int = 0;
    WHILE @i < 8
    BEGIN
        SET @key = @key + SUBSTRING(@alphabet, (ABS(CHECKSUM(CRYPT_GEN_RANDOM(4))) % LEN(@alphabet)) + 1, 1);
        SET @i = @i + 1;
    END;

    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_insertdatetime, cs_description) VALUES
      (1, 'MetroPipe', 'PublicReportKey', @key, GETDATE(),
       'Key required in ?k= to open the public Metro Pipe Program Report without a login. Case-sensitive. Rotate by updating this value.');

    PRINT 'Public Metro Pipe report key created: ' + @key;
END
ELSE
BEGIN
    PRINT 'Public Metro Pipe report key already exists: ' + (SELECT cs_value FROM ConfigSetting WHERE cs_type = 'MetroPipe' AND cs_identifier = 'PublicReportKey');
END;
GO

-- To rotate later (pick a new 8-character value or rerun the generator above):
-- UPDATE ConfigSetting SET cs_value = 'NewKey12', cs_modifieddatetime = GETDATE() WHERE cs_type = 'MetroPipe' AND cs_identifier = 'PublicReportKey';
-- To disable the public page entirely, set cs_value = ''.
