-- One-time clean-up for a TEST database that ran the first cut of create_asset_tables.sql (2026-09-07),
-- which created PMUnitLocation, PMAccessRequirement and LocationPMProfile. Those three were renamed the same day to
-- AssetMountLocation, LocationAccessRequirement and LocationTradeProfile (generic names: nothing in them is PM-only).
-- Run this once, then re-run create_asset_tables.sql. Never needed in production, which only ever sees the renamed script.
-- Safe to re-run: every step is guarded. Only seed rows lived in these tables; profile rows, if any, are dropped.

SET NOCOUNT ON;

IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_Asset_PMUnitLocation')
    ALTER TABLE dbo.Asset DROP CONSTRAINT FK_Asset_PMUnitLocation;
IF COL_LENGTH('dbo.Asset', 'pmul_id') IS NOT NULL
    ALTER TABLE dbo.Asset DROP COLUMN pmul_id;
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'LocationPMProfile')
    DROP TABLE dbo.LocationPMProfile;
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PMAccessRequirement')
    DROP TABLE dbo.PMAccessRequirement;
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PMUnitLocation')
    DROP TABLE dbo.PMUnitLocation;

PRINT 'PM-named slice 2 lookups removed; run create_asset_tables.sql next.';
