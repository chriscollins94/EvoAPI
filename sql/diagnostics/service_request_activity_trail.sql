-- =============================================================================
-- Service Request Activity trail (read-only)
--
-- Same data the office "Service Request Activity" report shows, pulled straight
-- from the trigger audit table (dbo.ServiceRequestActivity) with a plain-English
-- summary per row and times converted to Central.
--
-- Use this when the report page shows "0 records" for an SR that clearly has
-- history. The report's flip-flop filter (EvoApiController.GetServiceRequestActivity)
-- uses DATEDIFF(MILLISECOND, ...) which overflows once two updates to the same
-- record are more than ~24.8 days apart, so the API errors and the page shows an
-- empty list. Fixed in code 2026-10-22 (DATEADD/BETWEEN window); ships with the next
-- EvoAPI deploy. This script applies the same flip-flop suppression without the
-- overflow and stays useful for Central-time, plain-English output.
--
-- Parameters:
--   @RequestNumber     exact sr_requestnumber (quotes/parts SRs often share a prefix,
--                      result set 1 lists near-matches so you can pick the right one)
--   @From              only show activity on/after this date (Central). NULL = everything
--   @HideNoChangeRows  1 = hide UPDATE rows where nothing but *_modifieddatetime changed
--   @HideFlipFlops     1 = hide single-field updates that are reverted within 3 seconds
--                      (e.g. sr_tripcharge_worked_billed 75 -> 54.75 -> 75 on every save)
--
-- Result sets:
--   1. SR(s) matching the number
--   2. Full activity trail
--   3. Deleted records only, with their full content (recoverable from before_json)
-- =============================================================================

DECLARE @RequestNumber    varchar(100) = '1-6646667762';
DECLARE @From             date         = '2026-10-20';
DECLARE @HideNoChangeRows bit          = 1;
DECLARE @HideFlipFlops    bit          = 1;

SET NOCOUNT ON;
SET QUOTED_IDENTIFIER ON;   -- required by the XML-based changed_fields builder (sqlcmd defaults it off)

-- 1. Which SR(s) match
SELECT sr.sr_id, sr.sr_requestnumber, sr.sr_summary,
       sr.sr_insertdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS created_central,
       CASE WHEN LTRIM(RTRIM(sr.sr_requestnumber)) = @RequestNumber THEN 'EXACT MATCH (used below)' ELSE 'near match (not used)' END AS match_type
FROM ServiceRequest sr
WHERE sr.sr_requestnumber LIKE '%' + @RequestNumber + '%'
ORDER BY match_type, sr.sr_insertdatetime;

DECLARE @sr_id int = (SELECT TOP 1 sr_id FROM ServiceRequest WHERE LTRIM(RTRIM(sr_requestnumber)) = @RequestNumber ORDER BY sr_insertdatetime);
DECLARE @FromUtc datetime = CASE WHEN @From IS NULL THEN NULL
                                 ELSE CAST(CAST(@From AS datetime) AT TIME ZONE 'Central Standard Time' AT TIME ZONE 'UTC' AS datetime) END;

IF OBJECT_ID('tempdb..#trail') IS NOT NULL DROP TABLE #trail;
IF OBJECT_ID('tempdb..#diff')  IS NOT NULL DROP TABLE #diff;

SELECT a.sra_id, a.sra_insertdatetime, a.entity_name, a.entity_id, a.action, a.app_user, a.app_source, a.old_values, a.new_values,
       ISNULL(a.new_values, a.old_values) AS row_json
INTO #trail
FROM dbo.ServiceRequestActivity a
WHERE a.sr_id = @sr_id
  AND (@FromUtc IS NULL OR a.sra_insertdatetime >= @FromUtc);

-- Per-field diffs for UPDATE rows (ignoring the auto-stamped *_modifieddatetime)
SELECT t.sra_id, o.[key] AS field_name, o.[value] AS old_val, n.[value] AS new_val
INTO #diff
FROM #trail t
CROSS APPLY OPENJSON(t.old_values) o
CROSS APPLY (SELECT [value] FROM OPENJSON(t.new_values) WHERE [key] = o.[key]) n
WHERE t.action = 'U'
  AND o.[key] NOT LIKE '%[_]modifieddatetime'
  AND ISNULL(o.[value], N'') <> ISNULL(n.[value], N'');

;WITH diff_agg AS (
    SELECT sra_id, COUNT(*) AS change_count,
           MIN(field_name) AS single_field, MIN(old_val) AS single_old, MIN(new_val) AS single_new,
           STUFF((SELECT ', ' + d2.field_name + ': ' + REPLACE(REPLACE(ISNULL(d2.old_val, 'null'), CHAR(13), ' '), CHAR(10), ' / ')
                         + ' -> ' + REPLACE(REPLACE(ISNULL(d2.new_val, 'null'), CHAR(13), ' '), CHAR(10), ' / ')
                  FROM #diff d2 WHERE d2.sra_id = d.sra_id
                  FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 2, '') AS changed_fields
    FROM #diff d
    GROUP BY sra_id
),
-- Single-field update immediately reverted by another single-field update on the same record (within 3 s)
flipflop AS (
    SELECT DISTINCT a.sra_id
    FROM #trail ta JOIN diff_agg a ON a.sra_id = ta.sra_id AND a.change_count = 1
    JOIN #trail tb ON tb.entity_name = ta.entity_name AND ISNULL(tb.entity_id, -1) = ISNULL(ta.entity_id, -1) AND tb.sra_id <> ta.sra_id
                  AND tb.sra_insertdatetime BETWEEN DATEADD(SECOND, -3, ta.sra_insertdatetime) AND DATEADD(SECOND, 3, ta.sra_insertdatetime)
    JOIN diff_agg b ON b.sra_id = tb.sra_id AND b.change_count = 1
                  AND b.single_field = a.single_field
                  AND ISNULL(b.single_old, N'') = ISNULL(a.single_new, N'')
                  AND ISNULL(b.single_new, N'') = ISNULL(a.single_old, N'')
),
shaped AS (
    SELECT
        r.sra_id,
        r.sra_insertdatetime,
        r.sra_insertdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS when_central,
        CASE r.action WHEN 'I' THEN 'Insert' WHEN 'U' THEN 'Update' WHEN 'D' THEN 'DELETE' END AS action,
        r.entity_name AS what,
        r.entity_id   AS record_id,
        ISNULL(r.app_user, '(not captured)') AS by_user,
        r.app_source  AS via,
        CASE r.entity_name
            WHEN 'WorkOrderNote' THEN
                'WO ' + ISNULL((SELECT wo_workordernumber FROM WorkOrder WHERE wo_id = JSON_VALUE(r.row_json, '$.wo_id')), JSON_VALUE(r.row_json, '$.wo_id'))
                + ' - ' + ISNULL(JSON_VALUE(r.row_json, '$.won_user'), '') + ': '
                + REPLACE(REPLACE(ISNULL(JSON_VALUE(r.row_json, '$.won_note'), ''), CHAR(13), ' '), CHAR(10), ' / ')
                + CASE WHEN r.action = 'U' THEN '  [changed: ' + ISNULL(da.changed_fields, 'nothing') + ']' ELSE '' END
            WHEN 'TimeTracking' THEN
                'Check-in for ' + ISNULL((SELECT u_firstname + ' ' + u_lastname FROM [User] WHERE u_id = JSON_VALUE(r.row_json, '$.u_id')), 'u_id ' + ISNULL(JSON_VALUE(r.row_json, '$.u_id'), '?'))
                + ' on WO ' + ISNULL((SELECT wo_workordernumber FROM WorkOrder WHERE wo_id = JSON_VALUE(r.row_json, '$.wo_id')), ISNULL(JSON_VALUE(r.row_json, '$.wo_id'), '?'))
                + ' - in ' + ISNULL(CONVERT(varchar(16), CAST(JSON_VALUE(r.row_json, '$.tt_begin') AS datetime) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 120), '?')
                + ', out ' + ISNULL(CONVERT(varchar(16), CAST(JSON_VALUE(r.row_json, '$.tt_end') AS datetime) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time', 120), '(still checked in)')
                + ' Central'
                + CASE WHEN r.action = 'U' THEN '  [changed: ' + ISNULL(da.changed_fields, 'nothing') + ']' ELSE '' END
            WHEN 'WorkOrder' THEN
                'WO ' + ISNULL(JSON_VALUE(r.row_json, '$.wo_workordernumber'), '')
                + CASE WHEN r.action = 'U' THEN
                       ' - ' + ISNULL(da.changed_fields, 'no field changes')
                       + CASE WHEN JSON_VALUE(r.old_values, '$.ss_id') <> JSON_VALUE(r.new_values, '$.ss_id') THEN
                              ' - status ' + ISNULL((SELECT ss_statussecondary FROM StatusSecondary WHERE ss_id = JSON_VALUE(r.old_values, '$.ss_id')), '?')
                              + ' -> ' + ISNULL((SELECT ss_statussecondary FROM StatusSecondary WHERE ss_id = JSON_VALUE(r.new_values, '$.ss_id')), '?')
                         ELSE '' END
                  ELSE ' - status ' + ISNULL((SELECT ss_statussecondary FROM StatusSecondary WHERE ss_id = JSON_VALUE(r.row_json, '$.ss_id')), '?') END
            WHEN 'xrefWorkOrderUser' THEN
                CASE r.action WHEN 'I' THEN 'Tech assigned: ' WHEN 'D' THEN 'Tech removed: ' ELSE 'Tech assignment changed: ' END
                + ISNULL((SELECT u_firstname + ' ' + u_lastname FROM [User] WHERE u_id = JSON_VALUE(r.row_json, '$.u_id')), 'u_id ' + ISNULL(JSON_VALUE(r.row_json, '$.u_id'), '?'))
                + ' on WO ' + ISNULL((SELECT wo_workordernumber FROM WorkOrder WHERE wo_id = JSON_VALUE(r.row_json, '$.wo_id')), ISNULL(JSON_VALUE(r.row_json, '$.wo_id'), '?'))
            WHEN 'ServiceRequest' THEN
                CASE WHEN r.action = 'U' THEN ISNULL(da.changed_fields, 'no field changes')
                     + CASE WHEN JSON_VALUE(r.old_values, '$.s_id') <> JSON_VALUE(r.new_values, '$.s_id') THEN
                            ' - SR status ' + ISNULL((SELECT s_status FROM Status WHERE s_id = JSON_VALUE(r.old_values, '$.s_id')), '?')
                            + ' -> ' + ISNULL((SELECT s_status FROM Status WHERE s_id = JSON_VALUE(r.new_values, '$.s_id')), '?')
                       ELSE '' END
                     ELSE 'SR ' + ISNULL(JSON_VALUE(r.row_json, '$.sr_requestnumber'), '') END
            WHEN 'Attachment' THEN
                'Attachment: ' + ISNULL(JSON_VALUE(r.row_json, '$.att_description'), '') + ' (' + ISNULL(JSON_VALUE(r.row_json, '$.att_filename'), '') + ')'
                + CASE WHEN r.action = 'U' THEN '  [changed: ' + ISNULL(da.changed_fields, 'nothing') + ']' ELSE '' END
            WHEN 'xrefWorkOrderServiceItem' THEN
                'Service item ' + ISNULL((SELECT si_description FROM ServiceItem WHERE si_id = JSON_VALUE(r.row_json, '$.si_id')), 'si_id ' + ISNULL(JSON_VALUE(r.row_json, '$.si_id'), '?'))
                + ' qty ' + ISNULL(JSON_VALUE(r.row_json, '$.xwosi_quantity'), '?') + ' @ ' + ISNULL(JSON_VALUE(r.row_json, '$.xwosi_basecost'), '?')
                + CASE WHEN r.action = 'U' THEN '  [changed: ' + ISNULL(da.changed_fields, 'nothing') + ']' ELSE '' END
            WHEN 'TimeQuoted' THEN
                'Quoted time' + CASE WHEN r.action = 'U' THEN ': ' + ISNULL(da.changed_fields, 'nothing') ELSE '' END
            ELSE ISNULL(da.changed_fields, '')
        END AS summary,
        r.old_values AS before_json,
        r.new_values AS after_json,
        CASE WHEN r.action = 'U' AND da.sra_id IS NULL THEN 1 ELSE 0 END AS is_no_change,
        CASE WHEN ff.sra_id IS NOT NULL THEN 1 ELSE 0 END AS is_flipflop
    FROM #trail r
    LEFT JOIN diff_agg da ON da.sra_id = r.sra_id
    LEFT JOIN flipflop ff ON ff.sra_id = r.sra_id
)
SELECT * INTO #shaped FROM shaped;

-- 2. Full trail
SELECT when_central, action, what, record_id, by_user, via, summary, before_json, after_json
FROM #shaped
WHERE (@HideNoChangeRows = 0 OR is_no_change = 0)
  AND (@HideFlipFlops = 0 OR is_flipflop = 0)
ORDER BY sra_insertdatetime, sra_id;

-- 3. Deleted records only (everything needed to restore them is in before_json)
SELECT when_central AS deleted_central, what, record_id, by_user, summary AS deleted_content, before_json
FROM #shaped
WHERE action = 'DELETE'
ORDER BY sra_insertdatetime, sra_id;

DROP TABLE #shaped; DROP TABLE #diff; DROP TABLE #trail;
