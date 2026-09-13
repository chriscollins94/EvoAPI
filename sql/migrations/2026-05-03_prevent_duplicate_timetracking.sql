-- =============================================
-- Prevent duplicate TimeTracking rows (Login / Clockin / Break)
-- Run this script on production.
--
-- Order of operations:
--   1. Preview duplicates (SELECT) — review before proceeding.
--   2. Collapse duplicates: close extras at tt_begin so they contribute
--      zero time but the audit row remains.
--   3. Create the filtered unique index as a DB-level safety net.
--
-- If step 3 fails with a duplicate-key error, step 2 missed something —
-- re-run step 1 to find the stragglers.
-- =============================================

SET NOCOUNT ON;

PRINT '========================================'
PRINT 'TimeTracking duplicate cleanup + unique index'
PRINT '========================================'
PRINT ''

-- -----------------------------------------------------------------
-- STEP 1: Preview duplicates (read-only)
-- -----------------------------------------------------------------
PRINT '1. Duplicates that will be closed (oldest per u_id/ttt_id is kept):'
;WITH Dupes AS (
    SELECT tt_id,
           u_id,
           ttt_id,
           tt_begin,
           ROW_NUMBER() OVER (PARTITION BY u_id, ttt_id ORDER BY tt_begin ASC, tt_id ASC) AS rn
    FROM TimeTracking
    WHERE tt_end IS NULL
      AND ttt_id IN (1, 2, 4)
)
SELECT tt_id, u_id, ttt_id, tt_begin
FROM Dupes
WHERE rn > 1
ORDER BY u_id, ttt_id, tt_begin;

PRINT ''

-- -----------------------------------------------------------------
-- STEP 2: Close extras (sets tt_end = tt_begin → zero duration)
-- -----------------------------------------------------------------
PRINT '2. Closing duplicate open rows...'
;WITH Dupes AS (
    SELECT tt_id,
           ROW_NUMBER() OVER (PARTITION BY u_id, ttt_id ORDER BY tt_begin ASC, tt_id ASC) AS rn
    FROM TimeTracking
    WHERE tt_end IS NULL
      AND ttt_id IN (1, 2, 4)
)
UPDATE tt
SET tt.tt_end = tt.tt_begin
FROM TimeTracking tt
JOIN Dupes d ON tt.tt_id = d.tt_id
WHERE d.rn > 1;

PRINT '   ✓ Closed ' + CAST(@@ROWCOUNT AS varchar(10)) + ' duplicate row(s)'
PRINT ''

-- -----------------------------------------------------------------
-- STEP 3: Create filtered unique index
-- -----------------------------------------------------------------
PRINT '3. Creating UX_TimeTracking_OneActivePerSingletonType...'
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('TimeTracking')
      AND name = 'UX_TimeTracking_OneActivePerSingletonType'
)
BEGIN
    CREATE UNIQUE INDEX UX_TimeTracking_OneActivePerSingletonType
    ON TimeTracking (u_id, ttt_id)
    WHERE tt_end IS NULL AND ttt_id IN (1, 2, 4);
    PRINT '   ✓ Created UX_TimeTracking_OneActivePerSingletonType'
END
ELSE
    PRINT '   ⊘ UX_TimeTracking_OneActivePerSingletonType already exists'

PRINT ''
PRINT '========================================'
PRINT 'Done'
PRINT '========================================'
