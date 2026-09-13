-- Load an XRF revisit wave. Edit the three inputs below, then run on the target database.
--
-- Re-runnable: finds the wave by name (creates it if missing) and skips premises already in
-- that wave, so a wave can be built up over several runs (e.g. one run per team).
--
-- Each premise is matched to its High Volume row (HighVolumeBatchDetail.hvbd_premisenumber).
-- When a premise appears in more than one HV wave, the most recently completed HV row wins
-- (an incomplete HV row is used only when no completed one exists). Premises with no HV row
-- at all are still loaded (hvbd_id NULL) and listed at the end so the number can be checked;
-- the page shows them without the High Volume details.
--
-- Meter number is optional and stored exactly as given on the list. It is never used for the
-- match and never shown to the tech; it exists so a report can compare it to High Volume later.

SET NOCOUNT ON;

DECLARE @WaveName NVARCHAR(50) = 'XRF Wave 1';   -- shown in the "Select a Wave" dropdown
DECLARE @Team     NVARCHAR(10) = NULL;           -- e.g. '3'; NULL = each premise keeps its High Volume team

DECLARE @Stops TABLE (premisenumber NVARCHAR(50) NOT NULL, meternumber NVARCHAR(50) NULL);
INSERT INTO @Stops (premisenumber, meternumber) VALUES
    ('1112790000', 'SNR72201466'),
    ('1112780000', NULL);
    -- one row per premise; meter may be NULL

BEGIN TRAN;

DECLARE @xrfb_id INT = (SELECT TOP 1 xrfb_id FROM dbo.XrfBatch WHERE xrfb_filename = @WaveName);
IF @xrfb_id IS NULL
BEGIN
    INSERT INTO dbo.XrfBatch (xrfb_filename) VALUES (@WaveName);
    SET @xrfb_id = SCOPE_IDENTITY();
END;

;WITH Input AS (
    SELECT LTRIM(RTRIM(premisenumber)) AS premisenumber,
           MAX(NULLIF(LTRIM(RTRIM(meternumber)), '')) AS meternumber
    FROM @Stops
    WHERE LTRIM(RTRIM(premisenumber)) <> ''
    GROUP BY LTRIM(RTRIM(premisenumber))
),
Matched AS (
    SELECT i.premisenumber, i.meternumber, hv.hvbd_id, hv.hvbd_team
    FROM Input i
    OUTER APPLY (
        SELECT TOP 1 h.hvbd_id, CAST(h.hvbd_team AS NVARCHAR(10)) AS hvbd_team
        FROM dbo.HighVolumeBatchDetail h
        WHERE CAST(h.hvbd_premisenumber AS NVARCHAR(50)) = i.premisenumber
        ORDER BY CASE WHEN h.hvbd_completeddatetime IS NULL THEN 1 ELSE 0 END,
                 h.hvbd_completeddatetime DESC,
                 h.hvbd_id DESC
    ) hv
)
INSERT INTO dbo.XrfBatchDetail (xrfb_id, hvbd_id, xrfbd_premisenumber, xrfbd_meternumber, xrfbd_team)
SELECT @xrfb_id, m.hvbd_id, m.premisenumber, m.meternumber, COALESCE(@Team, m.hvbd_team)
FROM Matched m
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.XrfBatchDetail x
    WHERE x.xrfb_id = @xrfb_id AND x.xrfbd_premisenumber = m.premisenumber
);

PRINT CONCAT('Wave "', @WaveName, '" (xrfb_id ', @xrfb_id, '): ', @@ROWCOUNT, ' premise(s) added.');

-- Premises in this wave with no High Volume match (loaded, but shown without HV details)
SELECT x.xrfbd_premisenumber AS unmatched_premise, x.xrfbd_meternumber AS meter_as_given, x.xrfbd_team AS team
FROM dbo.XrfBatchDetail x
WHERE x.xrfb_id = @xrfb_id AND x.hvbd_id IS NULL
ORDER BY x.xrfbd_premisenumber;

COMMIT;
