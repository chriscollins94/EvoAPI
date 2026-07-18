-- =============================================================================
-- PRODUCTION REVERT for alter_markup_decimal_and_markuptype.sql (2026-07-18)
--
-- Purpose: the decimal migration was accidentally run in production while the
-- OLD application code is still deployed there. Old code breaks against the
-- decimal columns (EF6 int materialization errors; int.TryParse("25.00")
-- silently zeroing lr_markup in invoice math). This restores the int schema.
--
-- Safe because only whole-number values exist in these columns (old code can
-- only write ints), so int conversion loses nothing.
--
-- xwosi_markuptype is intentionally LEFT IN PLACE: old code ignores it, the
-- default keeps stamping 'quoted' on new rows, and it saves re-adding it later.
--
-- Run the WHOLE script in ONE SSMS window (temp table is session-scoped).
-- =============================================================================

-- ---- 0) Drop default constraints on the columns being altered ---------------
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

-- ---- 0b) Capture + drop nonclustered indexes referencing the columns --------
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
WHERE i.type = 2
  AND EXISTS (SELECT 1
              FROM sys.index_columns ic
              JOIN affected a ON a.object_id = ic.object_id AND a.column_id = ic.column_id
              WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id);

DECLARE @dropIx nvarchar(max) = N'';
SELECT @dropIx += N'PRINT ''Dropping index ' + ix_name + N''';' + drop_sql + N';' + CHAR(10) FROM #RecreateIndexes;
EXEC sp_executesql @dropIx;
GO

-- ---- 1) Restore int types ---------------------------------------------------
-- (Altering a column that is already int is a harmless no-op, so this works
--  regardless of which xwosi statements succeeded in the accidental run.)
ALTER TABLE dbo.MaterialsMarkup ALTER COLUMN mm_markup             int NOT NULL;
ALTER TABLE dbo.MaterialsMarkup ALTER COLUMN mm_markuphighquantity int NULL;
ALTER TABLE dbo.MaterialsMarkup ALTER COLUMN mm_markupfoundational int NULL;

ALTER TABLE dbo.xrefWorkOrderServiceItem ALTER COLUMN xwosi_percentagemarkup         int NOT NULL;
ALTER TABLE dbo.xrefWorkOrderServiceItem ALTER COLUMN xwosi_percentagemarkupsupplier int NOT NULL;
ALTER TABLE dbo.xrefWorkOrderServiceItem ALTER COLUMN xwosi_percentagetax            int NOT NULL;

ALTER TABLE dbo.xrefCompanyCallCenter ALTER COLUMN xccc_markuppercentage         int NULL;
ALTER TABLE dbo.xrefCompanyCallCenter ALTER COLUMN xccc_markuppercentagesupplier int NULL;
ALTER TABLE dbo.LaborRate             ALTER COLUMN lr_markup                     int NULL;
GO

-- ---- 2) Re-create the indexes dropped in step 0b ----------------------------
IF OBJECT_ID('tempdb..#RecreateIndexes') IS NOT NULL
BEGIN
    DECLARE @createIx nvarchar(max) = N'';
    SELECT @createIx += N'PRINT ''Re-creating index ' + ix_name + N''';' + create_sql + N';' + CHAR(10) FROM #RecreateIndexes;
    EXEC sp_executesql @createIx;
    DROP TABLE #RecreateIndexes;
END
GO

-- ---- 3) Re-create the known default constraints -----------------------------
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints WHERE name = 'DF_MaterialsMarkup_mm_markuphighquantity')
BEGIN
    ALTER TABLE dbo.MaterialsMarkup
        ADD CONSTRAINT DF_MaterialsMarkup_mm_markuphighquantity DEFAULT ((0)) FOR mm_markuphighquantity;
END

-- The three xwosi percentage columns are NOT NULL and the service-item INSERT
-- omits them, so their DEFAULT ((0)) constraints are load-bearing — without
-- them every service-item insert fails (Msg 515).
IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
               JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
               WHERE dc.parent_object_id = OBJECT_ID('dbo.xrefWorkOrderServiceItem') AND c.name = 'xwosi_percentagemarkup')
    ALTER TABLE dbo.xrefWorkOrderServiceItem
        ADD CONSTRAINT DF_xrefWorkOrderServiceItem_xwosi_percentagemarkup DEFAULT ((0)) FOR xwosi_percentagemarkup;

IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
               JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
               WHERE dc.parent_object_id = OBJECT_ID('dbo.xrefWorkOrderServiceItem') AND c.name = 'xwosi_percentagemarkupsupplier')
    ALTER TABLE dbo.xrefWorkOrderServiceItem
        ADD CONSTRAINT DF_xrefWorkOrderServiceItem_xwosi_percentagemarkupsupplier DEFAULT ((0)) FOR xwosi_percentagemarkupsupplier;

IF NOT EXISTS (SELECT 1 FROM sys.default_constraints dc
               JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
               WHERE dc.parent_object_id = OBJECT_ID('dbo.xrefWorkOrderServiceItem') AND c.name = 'xwosi_percentagetax')
    ALTER TABLE dbo.xrefWorkOrderServiceItem
        ADD CONSTRAINT DF_xrefWorkOrderServiceItem_xwosi_percentagetax DEFAULT ((0)) FOR xwosi_percentagetax;
GO

-- ---- Verification -----------------------------------------------------------
-- Expect int on all nine columns; xwosi_markuptype (varchar) intentionally remains.
SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE (c.TABLE_NAME = 'MaterialsMarkup'          AND c.COLUMN_NAME IN ('mm_markup','mm_markuphighquantity','mm_markupfoundational'))
   OR (c.TABLE_NAME = 'xrefWorkOrderServiceItem' AND c.COLUMN_NAME IN ('xwosi_percentagemarkup','xwosi_percentagemarkupsupplier','xwosi_percentagetax','xwosi_markuptype'))
   OR (c.TABLE_NAME = 'xrefCompanyCallCenter'    AND c.COLUMN_NAME IN ('xccc_markuppercentage','xccc_markuppercentagesupplier'))
   OR (c.TABLE_NAME = 'LaborRate'                AND c.COLUMN_NAME = 'lr_markup')
ORDER BY c.TABLE_NAME, c.COLUMN_NAME;

-- ---- Post-revert data check -------------------------------------------------
-- Invoices/recalcs that ran while the schema was decimal treated trade-level
-- lr_markup as 0 (int.TryParse("25.00") fails in the old code). Review SRs
-- whose markup was re-persisted during that window for trades with lr_markup > 0:
-- SELECT sr.sr_requestnumber, xwosi.xwosi_id, xwosi.xwosi_percentagemarkup, xwosi.xwosi_modifieddatetime
-- FROM xrefWorkOrderServiceItem xwosi
-- JOIN WorkOrder wo ON wo.wo_id = xwosi.wo_id
-- JOIN ServiceRequest sr ON sr.sr_id = wo.sr_id
-- JOIN LaborRate lr ON lr.xccc_id = sr.xccc_id AND lr.t_id = sr.t_id AND ISNULL(lr.lr_markup,0) > 0
-- WHERE xwosi.xwosi_modifieddatetime >= '<time you ran the accidental script>'
-- ORDER BY xwosi.xwosi_modifieddatetime DESC;
