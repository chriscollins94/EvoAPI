-- =====================================================================================================
-- 2026-09-20  Attachment: link files to a Location (PM slice 2 follow-up)
--
-- Aerial images (and later site / access photos) belong to a location, not to a call center / company
-- pairing. The legacy company-attachment list also only returns the five newest files per pairing, so
-- pooling every store's aerial there was never going to scale. This adds a nullable location key to
-- Attachment, mirroring the existing sr_id / as_id / xccc_id keys. Legacy EvoWS / EvoUI ignore the column.
--
-- Uploads go through EvoAPI (/EvoApi/locations/{lId}/attachments), which forwards the file to the legacy
-- file service (same Azure Files storage, resize and EXIF handling) and then stamps l_id on the new row.
-- Ships with: EvoAPI + evotech. Safe to rerun.
-- =====================================================================================================
SET NOCOUNT ON;
GO

IF COL_LENGTH('dbo.Attachment', 'l_id') IS NULL
BEGIN
    ALTER TABLE dbo.Attachment ADD l_id INT NULL CONSTRAINT FK_Attachment_Location REFERENCES dbo.Location (l_id);
    PRINT 'Attachment.l_id added';
END
ELSE
    PRINT 'Attachment.l_id already present';
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Attachment_Location' AND object_id = OBJECT_ID('dbo.Attachment'))
BEGIN
    -- deliberately NOT a filtered index: a filtered index makes every INSERT/UPDATE on Attachment fail on a connection
    -- with QUOTED_IDENTIFIER OFF (sqlcmd default), which would break uploads the first time a maintenance script ran that way
    CREATE NONCLUSTERED INDEX IX_Attachment_Location ON dbo.Attachment (l_id) INCLUDE (att_active, att_filename);
    PRINT 'IX_Attachment_Location created';
END
GO
