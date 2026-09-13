-- Re-keys XRF -> High Volume on premise number instead of meter number (meter numbers turned
-- out to be unreliable in the source lists). xrfbd_premisenumber becomes the required key;
-- xrfbd_meternumber stays as an optional, stored-as-given value that nothing joins on or
-- displays, kept only so a later report can measure how often it disagrees with High Volume.
--
-- TEST ONLY as written: the existing XRF waves are seed data and are wiped first so the new
-- NOT NULL column can be added cleanly. Re-seed afterwards with sql/seed/seed_xrf_test_wave.sql.
-- A fresh database gets the same shape from sql/migrations/2026-09-10_create_xrf_tables.sql and does not need this.

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.XrfBatchDetail') AND name = 'xrfbd_premisenumber')
BEGIN
    DELETE FROM dbo.XrfBatchDetail;
    DELETE FROM dbo.XrfBatch;

    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.XrfBatchDetail') AND name = 'IX_XrfBatchDetail_meternumber')
        DROP INDEX IX_XrfBatchDetail_meternumber ON dbo.XrfBatchDetail;

    ALTER TABLE dbo.XrfBatchDetail ADD xrfbd_premisenumber NVARCHAR(50) NOT NULL;   -- table is empty, so no default needed
    ALTER TABLE dbo.XrfBatchDetail ALTER COLUMN xrfbd_meternumber NVARCHAR(50) NULL;

    CREATE INDEX IX_XrfBatchDetail_premisenumber ON dbo.XrfBatchDetail (xrfbd_premisenumber);
END;
GO
