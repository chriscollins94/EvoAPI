-- =============================================================================
-- Materials Markup overhaul (2026-07-15)
-- Doc: evotech /office/docs/materials-markup  (decisions D1-D11, rules T1-T6)
--
--   1) Markup percentage columns: int -> decimal(6,2)  (D7)
--   2) New xrefWorkOrderServiceItem.xwosi_markuptype   (D8)
--      Existing rows backfill to 'quoted' via the default (D6)
--
-- Run AFTER deploying no code, BEFORE deploying the updated evo / EvoAPI code
-- is also safe: int-based code keeps working against decimal columns because
-- all existing values are whole numbers.
-- =============================================================================

-- ---- 0) Drop default constraints on the columns being altered ---------------
-- ALTER COLUMN fails (Msg 5074) while a DEFAULT constraint references the
-- column. This drops any DF on each affected column (prints what it dropped);
-- known defaults are re-created in step 5 below.
DECLARE @sql nvarchar(max) = N'';
SELECT @sql += N'PRINT ''Dropping ' + dc.name + N''';' +
               N'ALTER TABLE ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name) +
               N' DROP CONSTRAINT ' + QUOTENAME(dc.name) + N';' + CHAR(10)
FROM sys.default_constraints dc
JOIN sys.tables  t ON t.object_id = dc.parent_object_id
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE (t.name = 'MaterialsMarkup'          AND c.name IN ('mm_markup','mm_markuphighquantity','mm_markupfoundational'))
   OR (t.name = 'xrefWorkOrderServiceItem' AND c.name IN ('xwosi_percentagemarkup','xwosi_percentagemarkupsupplier','xwosi_percentagetax'))
   OR (t.name = 'xrefCompanyCallCenter'    AND c.name IN ('xccc_markuppercentage','xccc_markuppercentagesupplier'))
   OR (t.name = 'LaborRate'                AND c.name = 'lr_markup');
EXEC sp_executesql @sql;
GO

-- ---- 0b) Drop nonclustered indexes referencing the columns ------------------
-- ALTER COLUMN also fails (Msg 5074) while an index keys or INCLUDEs the
-- column (e.g. auto-created nci_wi_* missing-index indexes). This captures
-- each dependent index's full definition into a session temp table, drops it,
-- and step 3b re-creates them identically after the type changes.
IF OBJECT_ID('tempdb..#RecreateIndexes') IS NOT NULL DROP TABLE #RecreateIndexes;
CREATE TABLE #RecreateIndexes (ix_name sysname, drop_sql nvarchar(max), create_sql nvarchar(max));

;WITH affected AS (
    SELECT c.object_id, c.column_id
    FROM sys.columns c
    JOIN sys.tables t ON t.object_id = c.object_id
    WHERE (t.name = 'MaterialsMarkup'          AND c.name IN ('mm_markup','mm_markuphighquantity','mm_markupfoundational'))
       OR (t.name = 'xrefWorkOrderServiceItem' AND c.name IN ('xwosi_percentagemarkup','xwosi_percentagemarkupsupplier','xwosi_percentagetax'))
       OR (t.name = 'xrefCompanyCallCenter'    AND c.name IN ('xccc_markuppercentage','xccc_markuppercentagesupplier'))
       OR (t.name = 'LaborRate'                AND c.name = 'lr_markup')
)
INSERT INTO #RecreateIndexes (ix_name, drop_sql, create_sql)
SELECT i.name,
       N'DROP INDEX ' + QUOTENAME(i.name) + N' ON ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name),
       N'CREATE ' + CASE WHEN i.is_unique = 1 THEN N'UNIQUE ' ELSE N'' END + N'NONCLUSTERED INDEX ' + QUOTENAME(i.name) +
       N' ON ' + QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name) +
       N' (' + k.keycols + N')' +
       ISNULL(N' INCLUDE (' + inc.inclcols + N')', N'') +
       ISNULL(N' WHERE ' + i.filter_definition, N'')
FROM sys.indexes i
JOIN sys.tables t ON t.object_id = i.object_id
CROSS APPLY (
    SELECT STUFF((
        SELECT N', ' + QUOTENAME(col.name) + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N'' END
        FROM sys.index_columns ic
        JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
        ORDER BY ic.key_ordinal
        FOR XML PATH(''), TYPE).value('.','nvarchar(max)'), 1, 2, N'')
) AS k(keycols)
OUTER APPLY (
    SELECT STUFF((
        SELECT N', ' + QUOTENAME(col.name)
        FROM sys.index_columns ic
        JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id
        WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 1
        ORDER BY ic.index_column_id
        FOR XML PATH(''), TYPE).value('.','nvarchar(max)'), 1, 2, N'')
) AS inc(inclcols)
WHERE i.type = 2  -- nonclustered only; clustered/PK on these columns would need manual handling
  AND EXISTS (SELECT 1
              FROM sys.index_columns ic
              JOIN affected a ON a.object_id = ic.object_id AND a.column_id = ic.column_id
              WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id);

DECLARE @dropIx nvarchar(max) = N'';
SELECT @dropIx += N'PRINT ''Dropping index ' + ix_name + N''';' + drop_sql + N';' + CHAR(10) FROM #RecreateIndexes;
EXEC sp_executesql @dropIx;
GO

-- ---- 1) MaterialsMarkup: int -> decimal(6,2) --------------------------------
ALTER TABLE dbo.MaterialsMarkup ALTER COLUMN mm_markup             decimal(6,2) NOT NULL;
ALTER TABLE dbo.MaterialsMarkup ALTER COLUMN mm_markuphighquantity decimal(6,2) NULL;
ALTER TABLE dbo.MaterialsMarkup ALTER COLUMN mm_markupfoundational decimal(6,2) NULL;

-- ---- 2) xrefWorkOrderServiceItem: persisted percentages -> decimal(9,2) -----
-- decimal(9,2) rather than (6,2): legacy rows contain out-of-range percentage
-- values (> 9,999) that must be preserved as-is; new values are still written
-- as sane percents by the recalc engine.
ALTER TABLE dbo.xrefWorkOrderServiceItem ALTER COLUMN xwosi_percentagemarkup         decimal(9,2) NOT NULL;
ALTER TABLE dbo.xrefWorkOrderServiceItem ALTER COLUMN xwosi_percentagemarkupsupplier decimal(9,2) NOT NULL;
ALTER TABLE dbo.xrefWorkOrderServiceItem ALTER COLUMN xwosi_percentagetax            decimal(9,2) NOT NULL;

-- ---- 3) Company-level markups: int -> decimal(6,2)  (D7) --------------------
ALTER TABLE dbo.xrefCompanyCallCenter ALTER COLUMN xccc_markuppercentage         decimal(6,2) NULL;
ALTER TABLE dbo.xrefCompanyCallCenter ALTER COLUMN xccc_markuppercentagesupplier decimal(6,2) NULL;
ALTER TABLE dbo.LaborRate             ALTER COLUMN lr_markup                     decimal(6,2) NULL;
GO

-- ---- 3b) Re-create the indexes dropped in step 0b ---------------------------
IF OBJECT_ID('tempdb..#RecreateIndexes') IS NOT NULL
BEGIN
    DECLARE @createIx nvarchar(max) = N'';
    SELECT @createIx += N'PRINT ''Re-creating index ' + ix_name + N''';' + create_sql + N';' + CHAR(10) FROM #RecreateIndexes;
    EXEC sp_executesql @createIx;
    DROP TABLE #RecreateIndexes;
END
GO

-- ---- 4) Re-create the known default(s) dropped in step 0 --------------------
-- (Check the PRINT output of step 0: if it dropped other DF_ constraints,
--  re-create those the same way with their original default values.)
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE name = 'DF_MaterialsMarkup_mm_markuphighquantity')
BEGIN
    ALTER TABLE dbo.MaterialsMarkup
        ADD CONSTRAINT DF_MaterialsMarkup_mm_markuphighquantity DEFAULT ((0)) FOR mm_markuphighquantity;
END

-- ---- 5) New markup type flag  (D8; backfill per D6) -------------------------
IF NOT EXISTS (SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID('dbo.xrefWorkOrderServiceItem')
                 AND name = 'xwosi_markuptype')
BEGIN
    ALTER TABLE dbo.xrefWorkOrderServiceItem
        ADD xwosi_markuptype varchar(12) NOT NULL
        CONSTRAINT DF_xwosi_markuptype DEFAULT ('quoted')
        CONSTRAINT CK_xwosi_markuptype CHECK (xwosi_markuptype IN ('quoted','foundational'));
END
GO

-- ---- Verification -----------------------------------------------------------
-- Expect decimal(6,2) on the MaterialsMarkup / xccc / lr columns, decimal(9,2)
-- on the three xwosi percentage columns, and xwosi_markuptype present:
SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE (c.TABLE_NAME = 'MaterialsMarkup'          AND c.COLUMN_NAME IN ('mm_markup','mm_markuphighquantity','mm_markupfoundational'))
   OR (c.TABLE_NAME = 'xrefWorkOrderServiceItem' AND c.COLUMN_NAME IN ('xwosi_percentagemarkup','xwosi_percentagemarkupsupplier','xwosi_percentagetax','xwosi_markuptype'))
   OR (c.TABLE_NAME = 'xrefCompanyCallCenter'    AND c.COLUMN_NAME IN ('xccc_markuppercentage','xccc_markuppercentagesupplier'))
   OR (c.TABLE_NAME = 'LaborRate'                AND c.COLUMN_NAME = 'lr_markup')
ORDER BY c.TABLE_NAME, c.COLUMN_NAME;

-- Expect every pre-existing row = 'quoted':
SELECT xwosi_markuptype, COUNT(*) FROM dbo.xrefWorkOrderServiceItem GROUP BY xwosi_markuptype;

-- =============================================================================
-- REVERT (restores prior schema; decimal values with fractions will be
-- truncated back to int, so only revert before fractional markups are entered)
-- NOTE: default constraints block ALTER COLUMN here too - re-run the step 0
-- drop block first, then re-add DF_MaterialsMarkup_mm_markuphighquantity after.
-- =============================================================================
-- ALTER TABLE dbo.xrefWorkOrderServiceItem DROP CONSTRAINT CK_xwosi_markuptype;
-- ALTER TABLE dbo.xrefWorkOrderServiceItem DROP CONSTRAINT DF_xwosi_markuptype;
-- ALTER TABLE dbo.xrefWorkOrderServiceItem DROP COLUMN xwosi_markuptype;
-- ALTER TABLE dbo.MaterialsMarkup ALTER COLUMN mm_markup             int NOT NULL;
-- ALTER TABLE dbo.MaterialsMarkup ALTER COLUMN mm_markuphighquantity int NULL;
-- ALTER TABLE dbo.MaterialsMarkup ALTER COLUMN mm_markupfoundational int NULL;
-- ALTER TABLE dbo.xrefWorkOrderServiceItem ALTER COLUMN xwosi_percentagemarkup         int NOT NULL;
-- ALTER TABLE dbo.xrefWorkOrderServiceItem ALTER COLUMN xwosi_percentagemarkupsupplier int NOT NULL;
-- ALTER TABLE dbo.xrefWorkOrderServiceItem ALTER COLUMN xwosi_percentagetax            int NOT NULL;
-- ALTER TABLE dbo.xrefCompanyCallCenter ALTER COLUMN xccc_markuppercentage         int NULL;
-- ALTER TABLE dbo.xrefCompanyCallCenter ALTER COLUMN xccc_markuppercentagesupplier int NULL;
-- ALTER TABLE dbo.LaborRate             ALTER COLUMN lr_markup                     int NULL;
