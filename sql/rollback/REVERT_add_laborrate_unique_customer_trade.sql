-- Undoes migrations/2026-09-16_add_laborrate_unique_customer_trade.sql.
-- Drops the unique index only; no data is touched.
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_LaborRate_xccc_id_t_id' AND object_id = OBJECT_ID('dbo.LaborRate'))
    DROP INDEX UX_LaborRate_xccc_id_t_id ON dbo.LaborRate;
GO
