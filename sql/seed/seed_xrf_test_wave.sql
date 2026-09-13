-- TEST ONLY: build an XRF wave from real High Volume rows so the /xrf page has a realistic
-- spread of cards. Picks completed HV stops that have photos/answers (the normal case), plus a
-- few HV stops that were never completed (exercises the "HV not completed" pill/note), and a
-- few completed stops with no checklist answers at all.
--
-- Keyed on premise number (hvbd_premisenumber). The HV meter number is copied into the
-- informational xrfbd_meternumber column so the seed data looks like a real wave list.
--
-- Re-runnable: finds the wave by name (creates it if missing) and skips premises already in ANY
-- XRF wave, so running it again tops the wave up rather than duplicating. Teams are carried
-- over from the HV row; @ForceTeam overrides that when you want everything under one team.
--
-- Not for PROD. Real waves are loaded with sql/seed/insert_xrf_batch_template.sql.

SET NOCOUNT ON;

DECLARE @WaveName        NVARCHAR(50) = 'XRF Test Wave';
DECLARE @ForceTeam       NVARCHAR(10) = NULL;   -- e.g. '1'; NULL keeps each stop's HV team
DECLARE @CompletedCount  INT = 30;              -- completed HV stops with at least one photo answer
DECLARE @NoAnswerCount   INT = 4;               -- completed HV stops with no checklist answers
DECLARE @OpenCount       INT = 4;               -- HV stops never completed

BEGIN TRAN;

DECLARE @xrfb_id INT = (SELECT TOP 1 xrfb_id FROM dbo.XrfBatch WHERE xrfb_filename = @WaveName);
IF @xrfb_id IS NULL
BEGIN
    INSERT INTO dbo.XrfBatch (xrfb_filename) VALUES (@WaveName);
    SET @xrfb_id = SCOPE_IDENTITY();
END;

-- One HV row per premise: most recently completed wins, incomplete only when nothing completed exists
;WITH HvPerPremise AS (
    SELECT h.hvbd_id,
           LTRIM(RTRIM(CAST(h.hvbd_premisenumber AS NVARCHAR(50)))) AS premisenumber,
           CAST(h.hvbd_meternumber AS NVARCHAR(50)) AS meternumber,
           CAST(h.hvbd_team AS NVARCHAR(10)) AS hvbd_team,
           h.sr_id, h.hvbd_completeddatetime,
           ROW_NUMBER() OVER (
               PARTITION BY LTRIM(RTRIM(CAST(h.hvbd_premisenumber AS NVARCHAR(50))))
               ORDER BY CASE WHEN h.hvbd_completeddatetime IS NULL THEN 1 ELSE 0 END,
                        h.hvbd_completeddatetime DESC, h.hvbd_id DESC) AS rn
    FROM dbo.HighVolumeBatchDetail h
    WHERE h.hvbd_premisenumber IS NOT NULL AND LTRIM(RTRIM(CAST(h.hvbd_premisenumber AS NVARCHAR(50)))) <> ''
),
Candidates AS (
    SELECT hv.hvbd_id, hv.premisenumber, hv.meternumber, hv.hvbd_team, hv.hvbd_completeddatetime,
           CASE WHEN EXISTS (SELECT 1 FROM dbo.xrefservicerequestchecklistanswer a
                             WHERE a.sr_id = hv.sr_id AND a.att_id IS NOT NULL) THEN 1 ELSE 0 END AS has_photo,
           CASE WHEN EXISTS (SELECT 1 FROM dbo.xrefservicerequestchecklistanswer a
                             WHERE a.sr_id = hv.sr_id) THEN 1 ELSE 0 END AS has_answer
    FROM HvPerPremise hv
    WHERE hv.rn = 1
      AND NOT EXISTS (SELECT 1 FROM dbo.XrfBatchDetail x WHERE x.xrfbd_premisenumber = hv.premisenumber)
),
Picked AS (
    SELECT * FROM (SELECT TOP (@CompletedCount) c.* FROM Candidates c
                   WHERE c.hvbd_completeddatetime IS NOT NULL AND c.has_photo = 1
                   ORDER BY c.hvbd_completeddatetime DESC) a
    UNION ALL
    SELECT * FROM (SELECT TOP (@NoAnswerCount) c.* FROM Candidates c
                   WHERE c.hvbd_completeddatetime IS NOT NULL AND c.has_answer = 0
                   ORDER BY c.hvbd_completeddatetime DESC) b
    UNION ALL
    SELECT * FROM (SELECT TOP (@OpenCount) c.* FROM Candidates c
                   WHERE c.hvbd_completeddatetime IS NULL
                   ORDER BY c.hvbd_id DESC) d
)
INSERT INTO dbo.XrfBatchDetail (xrfb_id, hvbd_id, xrfbd_premisenumber, xrfbd_meternumber, xrfbd_team)
SELECT @xrfb_id, p.hvbd_id, p.premisenumber, p.meternumber, COALESCE(@ForceTeam, p.hvbd_team)
FROM Picked p;

PRINT CONCAT('Wave "', @WaveName, '" (xrfb_id ', @xrfb_id, '): ', @@ROWCOUNT, ' premise(s) added.');

COMMIT;

-- What the wave now contains, by team and HV state
SELECT x.xrfbd_team AS team,
       SUM(CASE WHEN h.hvbd_completeddatetime IS NOT NULL THEN 1 ELSE 0 END) AS hv_completed,
       SUM(CASE WHEN h.hvbd_completeddatetime IS NULL THEN 1 ELSE 0 END)     AS hv_open,
       SUM(CASE WHEN x.xrfbd_completeddatetime IS NOT NULL THEN 1 ELSE 0 END) AS xrf_submitted,
       COUNT(*) AS total
FROM dbo.XrfBatchDetail x
LEFT JOIN dbo.HighVolumeBatchDetail h ON h.hvbd_id = x.hvbd_id
WHERE x.xrfb_id = @xrfb_id
GROUP BY x.xrfbd_team
ORDER BY TRY_CAST(x.xrfbd_team AS INT), x.xrfbd_team;

-- Undo for this wave (uncomment and run to start over):
-- DELETE FROM dbo.XrfBatchDetail WHERE xrfb_id = @xrfb_id;
-- DELETE FROM dbo.XrfBatch WHERE xrfb_id = @xrfb_id;
