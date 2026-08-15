-- =============================================
-- Create Missing Authentication Performance Indexes
-- Run this script on your production database
-- =============================================

SET NOCOUNT ON;

PRINT '========================================'
PRINT 'Creating Authentication Performance Indexes'
PRINT '========================================'
PRINT ''

-- 1. User table - Index on username (CRITICAL for login performance)
PRINT '1. Creating index on User.u_username...'
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('[User]')
    AND name = 'IX_User_Username'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_User_Username
    ON [User](u_username)
    INCLUDE (u_password, u_active, u_2fa, u_firstname, u_lastname, u_picture, u_passwordchanged, u_id);
    PRINT '   ✓ Created IX_User_Username'
END
ELSE
    PRINT '   ⊘ IX_User_Username already exists'

PRINT ''

-- 2. xrefUserRole - Index on u_id for permission lookup
PRINT '2. Creating index on xrefUserRole.u_id...'
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('xrefUserRole')
    AND name = 'IX_xrefUserRole_UserId'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_xrefUserRole_UserId
    ON xrefUserRole(u_id)
    INCLUDE (r_id);
    PRINT '   ✓ Created IX_xrefUserRole_UserId'
END
ELSE
    PRINT '   ⊘ IX_xrefUserRole_UserId already exists'

PRINT ''

-- 3. xrefRoleFunction - Index on r_id for permission lookup
PRINT '3. Creating index on xrefRoleFunction.r_id...'
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('xrefRoleFunction')
    AND name = 'IX_xrefRoleFunction_RoleId'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_xrefRoleFunction_RoleId
    ON xrefRoleFunction(r_id)
    INCLUDE (f_id);
    PRINT '   ✓ Created IX_xrefRoleFunction_RoleId'
END
ELSE
    PRINT '   ⊘ IX_xrefRoleFunction_RoleId already exists'

PRINT ''

-- 4. TimeTracking - Composite index on u_id and tt_begin
PRINT '4. Creating index on TimeTracking.u_id, tt_begin...'
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('TimeTracking')
    AND name = 'IX_TimeTracking_UserId_Begin'
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_TimeTracking_UserId_Begin
    ON TimeTracking(u_id, tt_begin)
    INCLUDE (ttt_id, tt_end, tt_begin_lat, tt_begin_lon, tt_end_lat, tt_end_lon);
    PRINT '   ✓ Created IX_TimeTracking_UserId_Begin'
END
ELSE
    PRINT '   ⊘ IX_TimeTracking_UserId_Begin already exists'

PRINT ''
PRINT '========================================'
PRINT 'Index Creation Complete'
PRINT '========================================'
PRINT ''
PRINT 'You can verify the new indexes by running CheckAuthenticationIndexes.sql'
PRINT ''
