-- Adds the optional XRF photo to XrfBatchDetail. The tech may attach one picture when
-- submitting a stop; it goes through the normal attachment service (resized, GPS-stamped,
-- stored with the other attachments) and only its id is kept here. Not linked to a service
-- request. Optional for every result; only settable at submit time.
--
-- Run on TEST and PROD with the EvoAPI + evotech deploy. Safe to rerun.

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.XrfBatchDetail') AND name = 'xrfbd_att_id')
BEGIN
    ALTER TABLE dbo.XrfBatchDetail ADD xrfbd_att_id INT NULL;   -- Attachment.att_id of the optional XRF photo
END;
GO
