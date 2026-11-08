-- Why is a service request missing from /office/workorderlist?
-- Mirrors every exclusion in DataService.GetWorkOrdersAsync and the page's client-side filters.
-- Usage: set @RequestNumber and run against the environment in question (read-only).

DECLARE @RequestNumber nvarchar(100) = '6020549 - Q4';
DECLARE @NumberOfDays  int = 180;   -- value chosen in the page's "days" dropdown (default 180)

-- 1. Does the SR exist at all, and is the request number an exact match?
SELECT 'Exact match' AS Step, sr.sr_id, sr.sr_requestnumber, sr.s_id, sr.wo_id_primary, sr.xccc_id, sr.l_id, sr.p_id, sr.t_id, sr.sr_insertdatetime
FROM servicerequest sr
WHERE sr.sr_requestnumber = @RequestNumber;

-- 1b. Near matches (spacing / dash / case differences are a common cause)
SELECT 'Near match' AS Step, sr.sr_id, '[' + sr.sr_requestnumber + ']' AS sr_requestnumber, LEN(sr.sr_requestnumber) AS Len, sr.sr_insertdatetime
FROM servicerequest sr
WHERE REPLACE(REPLACE(sr.sr_requestnumber, ' ', ''), '-', '') LIKE '%' + REPLACE(REPLACE(@RequestNumber, ' ', ''), '-', '') + '%'
   OR sr.sr_requestnumber LIKE '%' + LEFT(@RequestNumber, 7) + '%';

-- 2. Condition-by-condition check against the list query. Any 'FAIL' row explains the omission.
;WITH sr AS (
    SELECT TOP 1 * FROM servicerequest WHERE sr_requestnumber = @RequestNumber ORDER BY sr_id DESC
)
SELECT
    sr.sr_id,
    CASE WHEN xccc.xccc_id IS NULL THEN 'FAIL (no xrefCompanyCallCenter)' ELSE 'ok' END AS JoinCompanyCallCenter,
    CASE WHEN c.c_id IS NULL     THEN 'FAIL (no company)'     ELSE 'ok ' + c.c_name END AS JoinCompany,
    CASE WHEN cc.cc_id IS NULL   THEN 'FAIL (no callcenter)'  ELSE 'ok ' + cc.cc_name END AS JoinCallCenter,
    CASE WHEN s.s_id IS NULL     THEN 'FAIL (no status)'      ELSE 'ok ' + s.s_status END AS JoinStatus,
    CASE WHEN l.l_id IS NULL     THEN 'FAIL (no location)'    ELSE 'ok' END AS JoinLocation,
    CASE WHEN a.a_id IS NULL     THEN 'FAIL (location has no address)' ELSE 'ok' END AS JoinAddress,
    CASE WHEN p.p_id IS NULL     THEN 'FAIL (no priority)'    ELSE 'ok ' + p.p_priority END AS JoinPriority,
    CASE WHEN t.t_id IS NULL     THEN 'FAIL (no trade)'       ELSE 'ok ' + t.t_trade END AS JoinTrade,
    CASE WHEN c.c_name IN ('Metro Pipe Program', 'Metro Pipe Program 2') THEN 'FAIL (Metro Pipe company excluded)' ELSE 'ok' END AS CompanyExclusion,
    wo.wo_id AS PrimaryWorkOrderId,
    wo.wo_startdatetime AS StartDateUtc,
    CASE
        WHEN wo.wo_id IS NULL AND sr.wo_id_primary IS NOT NULL THEN 'FAIL (wo_id_primary points to a missing work order)'
        WHEN wo.wo_startdatetime IS NULL AND s.s_status IN ('Paid', 'Invoiced') THEN 'FAIL (no start date and status is Paid/Invoiced)'
        WHEN wo.wo_startdatetime IS NULL THEN 'ok (unscheduled, shown because status is not Paid/Invoiced)'
        WHEN wo.wo_startdatetime < DATEADD(DAY, -@NumberOfDays, GETDATE()) THEN 'FAIL (start date older than ' + CAST(@NumberOfDays AS varchar) + ' days; raise the days dropdown)'
        WHEN wo.wo_startdatetime > DATEADD(DAY, @NumberOfDays, GETDATE()) THEN 'FAIL (start date more than ' + CAST(@NumberOfDays AS varchar) + ' days in the future; raise the days dropdown)'
        ELSE 'ok'
    END AS DateWindow,
    CASE WHEN s.s_status = 'Rejected' THEN 'HIDDEN by default (page hides Rejected unless the Rejected status filter is ticked)' ELSE 'ok' END AS ClientRejectedRule,
    CASE WHEN tech.cnt = 0 AND anyuser.cnt > 0 THEN 'FAIL (assigned user(s) have only non-Technician roles)'
         ELSE 'ok (' + CAST(ISNULL(tech.cnt,0) AS varchar) + ' technician row(s))' END AS AssignedUserRoleRule
FROM sr
LEFT JOIN xrefCompanyCallCenter xccc ON sr.xccc_id = xccc.xccc_id
LEFT JOIN company c   ON xccc.c_id = c.c_id
LEFT JOIN callcenter cc ON xccc.cc_id = cc.cc_id
LEFT JOIN [status] s  ON sr.s_id = s.s_id
LEFT JOIN location l  ON sr.l_id = l.l_id
LEFT JOIN address a   ON l.a_id = a.a_id
LEFT JOIN priority p  ON sr.p_id = p.p_id
LEFT JOIN trade t     ON sr.t_id = t.t_id
LEFT JOIN workorder wo ON sr.wo_id_primary = wo.wo_id
OUTER APPLY (
    SELECT COUNT(*) AS cnt
    FROM xrefworkorderuser xwou
    LEFT JOIN xrefuserrole xur ON xur.u_id = xwou.u_id
    LEFT JOIN role r ON r.r_id = xur.r_id
    WHERE xwou.wo_id = wo.wo_id AND (r.r_role = 'Technician' OR r.r_role IS NULL)
) tech
OUTER APPLY (
    SELECT COUNT(*) AS cnt FROM xrefworkorderuser xwou WHERE xwou.wo_id = wo.wo_id
) anyuser;

-- 3. All work orders on the SR (is wo_id_primary pointing at the right one?)
SELECT 'Work orders' AS Step, wo.wo_id, wo.wo_startdatetime, wo.wo_enddatetime, wo.ss_id,
       CASE WHEN wo.wo_id = sr.wo_id_primary THEN 'PRIMARY' ELSE '' END AS IsPrimary
FROM servicerequest sr
JOIN workorder wo ON wo.sr_id = sr.sr_id
WHERE sr.sr_requestnumber = @RequestNumber
ORDER BY wo.wo_id;

-- 4. Would the list query itself return it? (same WHERE clause as GetWorkOrdersAsync)
SELECT 'List query' AS Step, sr.sr_id, sr.sr_requestnumber, s.s_status, wo.wo_startdatetime
FROM servicerequest sr
INNER JOIN xrefCompanyCallCenter xccc ON sr.xccc_id = xccc.xccc_id
INNER JOIN company c ON xccc.c_id = c.c_id
INNER JOIN callcenter cc ON xccc.cc_id = cc.cc_id
INNER JOIN [status] s ON sr.s_id = s.s_id
INNER JOIN location l ON sr.l_id = l.l_id
INNER JOIN priority p ON sr.p_id = p.p_id
INNER JOIN address a ON l.a_id = a.a_id
INNER JOIN trade t ON sr.t_id = t.t_id
LEFT JOIN workorder wo ON sr.wo_id_primary = wo.wo_id
LEFT JOIN xrefworkorderuser xwou ON wo.wo_id = xwou.wo_id
LEFT JOIN [user] u ON xwou.u_id = u.u_id
LEFT JOIN xrefuserrole xur ON xur.u_id = u.u_id
LEFT JOIN role r ON r.r_id = xur.r_id
WHERE sr.sr_requestnumber = @RequestNumber
  AND (wo.wo_startdatetime BETWEEN DATEADD(DAY, -@NumberOfDays, GETDATE()) AND DATEADD(DAY, @NumberOfDays, GETDATE())
       OR (wo.wo_startdatetime IS NULL AND s.s_status NOT IN ('Paid', 'Invoiced')))
  AND c.c_name NOT IN ('Metro Pipe Program', 'Metro Pipe Program 2')
  AND (r.r_role = 'Technician' OR r.r_role IS NULL);
