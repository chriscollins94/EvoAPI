-- Zone performance: columns for the trimmed zone workbook format.
--
-- The zone file is now a flat sheet -- one header row, one row per zone, 14
-- columns: Zone, Rev Per Tech Per Day, YTD Contribution, then the 11 metrics
-- that both the zone and technician files share. The first two have no home in
-- PerformanceZone yet; this script adds them.
--
-- Both are intentionally untargeted: no ConfigSetting rows, no good/bad
-- coloring on the dashboard. YTD Contribution in particular is a cumulative
-- dollar total that is negative for most zones, so a fixed target would be
-- noise rather than signal. Rev Per Tech Per Day simply has no stated target
-- now that the file no longer carries a TARGET LEVELS row.
--
-- pz_profitgrade is deliberately left in place. The new file has no YTD Profit
-- Grade column for zones, so it stops being written and read, but every
-- existing row in it is already NULL and dropping it is the only irreversible
-- step in this change.
--
-- Safe to re-run. Must be applied BEFORE deploying the API -- the zone queries
-- select these columns by name and will fail against an un-migrated database.

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.PerformanceZone') AND name = 'pz_revpertechperday')
BEGIN
    ALTER TABLE dbo.PerformanceZone ADD pz_revpertechperday DECIMAL(18,6) NULL;
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dbo.PerformanceZone') AND name = 'pz_ytdcontribution')
BEGIN
    ALTER TABLE dbo.PerformanceZone ADD pz_ytdcontribution DECIMAL(18,6) NULL;
END;
GO
