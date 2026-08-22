-- =============================================================================
-- Indexes for StatusChange / StatusSecondaryChange
-- StatusSecondaryChange previously had ONLY its clustered PK, so the status
-- change history report, its export, the "previous change" lookup, and the
-- status triggers' own TOP 1 prior-row lookups all scanned the table.
--
-- Run on TEST first, then PROD with the same deploy as the export fast path.
-- Safe to re-run (guarded by IF NOT EXISTS).
-- =============================================================================

-- Prev-row lookup (report Prior By) + trigger prior-status lookup for WOs.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_StatusSecondaryChange_wo_id' AND object_id = OBJECT_ID('dbo.StatusSecondaryChange'))
    CREATE NONCLUSTERED INDEX IX_StatusSecondaryChange_wo_id
        ON dbo.StatusSecondaryChange (wo_id, ssc_id)
        INCLUDE (ss_id_new, ss_id_prior, u_id, ssc_insertdatetime);
GO

-- Date-window filtering for the report/summary/export; covering to avoid key lookups.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_StatusSecondaryChange_insertdatetime' AND object_id = OBJECT_ID('dbo.StatusSecondaryChange'))
    CREATE NONCLUSTERED INDEX IX_StatusSecondaryChange_insertdatetime
        ON dbo.StatusSecondaryChange (ssc_insertdatetime)
        INCLUDE (wo_id, ss_id_prior, ss_id_new, ssc_minutesinpriorstatus, u_id);
GO

-- Prev-row lookup (report Prior By) for SRs. The trigger's own lookup is already
-- served by the existing auto-created index on (s_id_new, sr_id).
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_StatusChange_sr_id' AND object_id = OBJECT_ID('dbo.StatusChange'))
    CREATE NONCLUSTERED INDEX IX_StatusChange_sr_id
        ON dbo.StatusChange (sr_id, sc_id)
        INCLUDE (s_id_new, s_id_prior, u_id, sc_insertdatetime);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_StatusChange_insertdatetime' AND object_id = OBJECT_ID('dbo.StatusChange'))
    CREATE NONCLUSTERED INDEX IX_StatusChange_insertdatetime
        ON dbo.StatusChange (sc_insertdatetime)
        INCLUDE (sr_id, s_id_prior, s_id_new, sc_minutesinpriorstatus, u_id);
GO

-- Quote column on the status change history report: per-SR quoted-labor sum.
-- TimeQuoted previously had only its PK, so each per-row lookup scanned it.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_TimeQuoted_sr_id' AND object_id = OBJECT_ID('dbo.TimeQuoted'))
    CREATE NONCLUSTERED INDEX IX_TimeQuoted_sr_id
        ON dbo.TimeQuoted (sr_id)
        INCLUDE (tq_rate, tq_hours);
GO

SELECT OBJECT_NAME(object_id) AS tbl, name FROM sys.indexes
WHERE object_id IN (OBJECT_ID('dbo.StatusChange'), OBJECT_ID('dbo.StatusSecondaryChange'), OBJECT_ID('dbo.TimeQuoted'))
ORDER BY tbl, name;
GO
