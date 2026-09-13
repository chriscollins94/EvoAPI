-- Adds the required XRF result to XrfBatchDetail. The tech picks one of
-- Complete | Error | Inaccessible | Dirty/Wet when submitting a stop; the API rejects a
-- submit without one. Any result finalizes the stop (completed datetime, tech, position);
-- a location that needs another try is re-queued by loading it into a new wave.
--
-- Only needed where sql/migrations/2026-09-10_create_xrf_tables.sql was run before 2026-09-11 (the create script
-- now includes the column). Run on TEST first; run on PROD with the EvoAPI + evotech deploy.

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.XrfBatchDetail') AND name = 'xrfbd_result')
BEGIN
    ALTER TABLE dbo.XrfBatchDetail ADD
        xrfbd_result NVARCHAR(20) NULL
            CONSTRAINT CK_XrfBatchDetail_result
            CHECK (xrfbd_result IS NULL OR xrfbd_result IN ('Complete', 'Error', 'Inaccessible', 'Dirty/Wet'));
END;
GO
