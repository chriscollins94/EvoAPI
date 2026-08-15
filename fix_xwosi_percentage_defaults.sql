-- =============================================================================
-- Restore DEFAULT constraints on xrefWorkOrderServiceItem percentage columns
-- (2026-07-18) — RUN IN BOTH TEST AND PRODUCTION.
--
-- The markup migration (and the production revert) drop every DEFAULT
-- constraint on the columns they alter, but only re-created the MaterialsMarkup
-- one. The service-item INSERT (old and new code alike) omits the three
-- NOT NULL percentage columns and relies on their defaults, so inserts fail
-- with "Cannot insert the value NULL into column 'xwosi_percentagemarkup'".
-- =============================================================================

-- Diagnostic: what defaults currently exist on the four markup tables
SELECT t.name AS table_name, c.name AS column_name, dc.name AS constraint_name, dc.definition
FROM sys.default_constraints dc
JOIN sys.tables  t ON t.object_id = dc.parent_object_id
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE t.name IN ('MaterialsMarkup','xrefWorkOrderServiceItem','xrefCompanyCallCenter','LaborRate')
ORDER BY t.name, c.name;
GO

-- Re-create the three required defaults (guarded per column, safe to re-run)
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

-- Verify: expect a default on all three percentage columns (plus markuptype + MaterialsMarkup highquantity)
SELECT t.name AS table_name, c.name AS column_name, dc.name AS constraint_name, dc.definition
FROM sys.default_constraints dc
JOIN sys.tables  t ON t.object_id = dc.parent_object_id
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE t.name = 'xrefWorkOrderServiceItem'
ORDER BY c.name;
