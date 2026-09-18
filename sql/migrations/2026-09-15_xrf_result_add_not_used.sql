-- Adds 'XRF Not Used' as a fifth allowed XRF result. The CHECK constraint on
-- XrfBatchDetail.xrfbd_result is the only thing that knows the list on the database side, so
-- it is dropped and recreated with the new value. Existing rows are untouched.
--
-- Run on EVERY database whose XRF tables were created before 2026-09-15 (TEST and PROD alike);
-- create_xrf_tables.sql only gained 'XRF Not Used' that day. Until this runs, every submit with
-- that result is rejected by the constraint and the tech sees a save failure. Safe to rerun.

IF EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_XrfBatchDetail_result')
    ALTER TABLE dbo.XrfBatchDetail DROP CONSTRAINT CK_XrfBatchDetail_result;

ALTER TABLE dbo.XrfBatchDetail ADD CONSTRAINT CK_XrfBatchDetail_result
    CHECK (xrfbd_result IS NULL OR xrfbd_result IN ('Complete', 'Error', 'Inaccessible', 'Dirty/Wet', 'XRF Not Used'));
GO
