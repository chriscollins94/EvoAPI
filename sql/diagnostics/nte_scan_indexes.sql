-- NTE notification: indexes and row counts on the tables the 5-minute scan touches.
-- Read-only. Run on TEST (and PROD before go-live) and paste the output back; used to decide
-- which indexes the scan needs so the recipient + spend queries run in seconds, not tens of seconds.

SET NOCOUNT ON;

SELECT t.name AS table_name, i.name AS index_name, i.type_desc,
       STUFF((SELECT ', ' + c.name + CASE WHEN ic.is_included_column = 1 THEN ' (incl)' ELSE '' END
              FROM sys.index_columns ic
              JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
              WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
              ORDER BY ic.is_included_column, ic.key_ordinal
              FOR XML PATH('')), 1, 2, '') AS columns,
       i.has_filter, i.filter_definition
FROM sys.indexes i
JOIN sys.tables t ON t.object_id = i.object_id
WHERE t.name IN ('TimeTracking', 'WorkOrder', 'xrefWorkOrderUser', 'xrefWorkOrderServiceItem', 'ServiceRequest')
  AND i.type > 0
ORDER BY t.name, i.index_id;

SELECT 'TimeTracking' AS table_name, COUNT(*) AS rows_total,
       SUM(CASE WHEN ttt_id = 3 AND tt_end IS NULL THEN 1 ELSE 0 END) AS open_checkins,
       SUM(CASE WHEN ttt_id = 3 AND tt_end IS NULL AND tt_begin < DATEADD(DAY, -7, GETUTCDATE()) THEN 1 ELSE 0 END) AS open_checkins_older_than_7d
FROM dbo.TimeTracking WITH (NOLOCK)
UNION ALL SELECT 'WorkOrder', COUNT(*), NULL, NULL FROM dbo.WorkOrder WITH (NOLOCK)
UNION ALL SELECT 'xrefWorkOrderUser', COUNT(*), NULL, NULL FROM dbo.xrefWorkOrderUser WITH (NOLOCK)
UNION ALL SELECT 'xrefWorkOrderServiceItem', COUNT(*), NULL, NULL FROM dbo.xrefWorkOrderServiceItem WITH (NOLOCK)
UNION ALL SELECT 'ServiceRequest (open, NTE > 0)', COUNT(*), NULL, NULL FROM dbo.ServiceRequest WITH (NOLOCK) WHERE s_id NOT IN (6, 9) AND sr_nte > 0;
