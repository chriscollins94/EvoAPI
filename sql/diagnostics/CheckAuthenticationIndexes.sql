-- =============================================
-- Check Authentication Performance Indexes
-- =============================================

SET NOCOUNT ON;

PRINT '========================================'
PRINT 'Checking Authentication Performance Indexes'
PRINT '========================================'
PRINT ''

-- Check User table indexes
PRINT '1. User Table Indexes:'
PRINT '----------------------------------------'
IF EXISTS (
    SELECT 1 FROM sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE i.object_id = OBJECT_ID('[User]')
    AND c.name = 'u_username'
    AND i.type IN (1, 2) -- Clustered or Non-clustered
)
    PRINT '   ✓ Index on u_username EXISTS'
ELSE
    PRINT '   ✗ MISSING: Index on u_username'

-- Recommended index for User table
PRINT ''
PRINT '   Recommended User Index:'
PRINT '   CREATE INDEX IX_User_Username ON [User](u_username) INCLUDE (u_password, u_active, u_2fa, u_firstname, u_lastname, u_picture, u_passwordchanged);'
PRINT ''

-- Check xrefUserRole table indexes
PRINT '2. xrefUserRole Table Indexes:'
PRINT '----------------------------------------'
IF EXISTS (
    SELECT 1 FROM sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE i.object_id = OBJECT_ID('xrefUserRole')
    AND c.name = 'u_id'
    AND i.type IN (1, 2)
)
    PRINT '   ✓ Index on u_id EXISTS'
ELSE
    PRINT '   ✗ MISSING: Index on u_id'

PRINT ''
PRINT '   Recommended xrefUserRole Index:'
PRINT '   CREATE INDEX IX_xrefUserRole_UserId ON xrefUserRole(u_id);'
PRINT ''

-- Check xrefRoleFunction table indexes
PRINT '3. xrefRoleFunction Table Indexes:'
PRINT '----------------------------------------'
IF EXISTS (
    SELECT 1 FROM sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE i.object_id = OBJECT_ID('xrefRoleFunction')
    AND c.name = 'r_id'
    AND i.type IN (1, 2)
)
    PRINT '   ✓ Index on r_id EXISTS'
ELSE
    PRINT '   ✗ MISSING: Index on r_id'

PRINT ''
PRINT '   Recommended xrefRoleFunction Index:'
PRINT '   CREATE INDEX IX_xrefRoleFunction_RoleId ON xrefRoleFunction(r_id);'
PRINT ''

-- Check TimeTracking table indexes
PRINT '4. TimeTracking Table Indexes:'
PRINT '----------------------------------------'
IF EXISTS (
    SELECT 1 FROM sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE i.object_id = OBJECT_ID('TimeTracking')
    AND c.name = 'u_id'
    AND i.type IN (1, 2)
)
BEGIN
    -- Check if composite index exists
    IF EXISTS (
        SELECT 1 FROM sys.indexes i
        INNER JOIN sys.index_columns ic1 ON i.object_id = ic1.object_id AND i.index_id = ic1.index_id AND ic1.key_ordinal = 1
        INNER JOIN sys.columns c1 ON ic1.object_id = c1.object_id AND ic1.column_id = c1.column_id
        INNER JOIN sys.index_columns ic2 ON i.object_id = ic2.object_id AND i.index_id = ic2.index_id AND ic2.key_ordinal = 2
        INNER JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
        WHERE i.object_id = OBJECT_ID('TimeTracking')
        AND c1.name = 'u_id'
        AND c2.name = 'tt_begin'
    )
        PRINT '   ✓ Composite index on (u_id, tt_begin) EXISTS'
    ELSE
        PRINT '   ✓ Index on u_id EXISTS (but composite index recommended)'
END
ELSE
    PRINT '   ✗ MISSING: Index on u_id and tt_begin'

PRINT ''
PRINT '   Recommended TimeTracking Index:'
PRINT '   CREATE INDEX IX_TimeTracking_UserId_Begin ON TimeTracking(u_id, tt_begin) INCLUDE (ttt_id, tt_end);'
PRINT ''

-- Summary
PRINT ''
PRINT '========================================'
PRINT 'Summary: All Existing Indexes on Key Tables'
PRINT '========================================'
SELECT
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS IndexedColumns
FROM sys.indexes i
INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.object_id IN (
    OBJECT_ID('[User]'),
    OBJECT_ID('xrefUserRole'),
    OBJECT_ID('xrefRoleFunction'),
    OBJECT_ID('TimeTracking')
)
AND i.type IN (1, 2) -- Clustered and Non-clustered only
AND ic.is_included_column = 0
GROUP BY OBJECT_NAME(i.object_id), i.name, i.type_desc, i.index_id
ORDER BY TableName, i.name;

PRINT ''
PRINT '========================================'
PRINT 'Index Check Complete'
PRINT '========================================'
