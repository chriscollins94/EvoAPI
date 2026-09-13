-- Adds photo capture location columns to the Attachment table.
-- Populated by tech photo uploads (EvoUI tech schedule/highvolume -> EvoWS ProcessAttachments).
-- att_geosource: 'exif'   = parsed from the photo's EXIF metadata (true capture location),
--                'device' = device GPS position at the moment the photo was selected.
-- att_geoaccuracy: accuracy radius in meters (device source only; EXIF has no accuracy).
-- Run on TEST first; run on PROD together with the EvoWS + EvoUI deploy
-- (the Evo.Models.Attachment entity references these columns once deployed).

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.Attachment') AND name = 'att_latitude')
BEGIN
    ALTER TABLE dbo.Attachment ADD
        att_latitude    DECIMAL(9, 6) NULL,
        att_longitude   DECIMAL(9, 6) NULL,
        att_geoaccuracy INT NULL,
        att_geosource   VARCHAR(10) NULL;
END
