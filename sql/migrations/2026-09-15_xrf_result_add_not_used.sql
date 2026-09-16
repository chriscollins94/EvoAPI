-- Adds 'XRF Not Used' as a fifth allowed XRF result. The CHECK constraint on
-- XrfBatchDetail.xrfbd_result is the only thing that knows the list on the database side, so
-- it is dropped and recreated with the new value. Existing rows are untouched.
--
-- TEST only when the tables were created before 2026-09-15; create_xrf_tables.sql already
-- carries the full list, so PROD gets it from that script. Ships with EvoAPI + evotech.

IF EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_XrfBatchDetail_result')
    ALTER TABLE dbo.XrfBatchDetail DROP CONSTRAINT CK_XrfBatchDetail_result;

ALTER TABLE dbo.XrfBatchDetail ADD CONSTRAINT CK_XrfBatchDetail_result
    CHECK (xrfbd_result IS NULL OR xrfbd_result IN ('Complete', 'Error', 'Inaccessible', 'Dirty/Wet', 'XRF Not Used'));
GO
