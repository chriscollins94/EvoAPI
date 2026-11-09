-- NTE notification: explain the spend number for ONE service request. Read-only, safe on TEST or PROD.
-- Shows every input the scan uses (SqlNteSpendCalculator.ActiveSpendSql) and the arithmetic on each row,
-- so a figure in AuditNTE.an_detail can be traced back to the SR. Same rules as the scan:
--   labor        closed check-ins + open ones up to now (unless stale, D8); CEILING to the billable rule;
--                company minimum applied once to the first row; flat SR = sr_rateflat once any time exists
--   items        ((base * qty) + tax) * (1 + markup%) per row; quoted rows (xwosi_forquote = 1) never count (D7, 2026-11-09);
--                the legacy sr_nteexcludequotedsi flag is shown in section 1 for reference but not used
--   trip         worked only: lr_tripcharge if > 0 else rate * sr_tripcharge_worked%  (counted from the start, D3)
--   NOT counted  quoted trip charge (shown as quoted_trip_excluded, D3 2026-11-09);
--                TimeQuoted (quoted labor hours) - the invoice does not bill it either; listed below for reference.
-- Keep in sync with the C# constant when the formula changes.

SET NOCOUNT ON;
DECLARE @SrNumber varchar(50) = '20261108-3364';   -- <<< set me
DECLARE @MaxOpenCheckInHours int = 24;              -- NteConfig/MaxOpenCheckInHours

DECLARE @sr_id int = (SELECT TOP 1 sr_id FROM dbo.ServiceRequest WHERE sr_requestnumber = @SrNumber);
IF @sr_id IS NULL BEGIN PRINT 'No service request with that number'; RETURN; END;

-- 1) SR-level inputs
SELECT sr.sr_id, sr.sr_requestnumber, s.s_status, sr.sr_nte,
       ISNULL(sr.sr_nteexcludequotedsi, 0)                      AS legacy_exclude_flag_not_used,
       LOWER(ISNULL(NULLIF(sr.sr_flatorhourly, ''), 'hourly'))  AS flat_or_hourly,
       sr.sr_rateflat,
       lrt.lrt_fieldname                                        AS sr_rate_type,
       CASE lrt.lrt_fieldname
            WHEN 'lr_rateregular'             THEN lr.lr_rateregular
            WHEN 'lr_rateovertime'            THEN lr.lr_rateovertime
            WHEN 'lr_rateholiday'             THEN lr.lr_rateholiday
            WHEN 'lr_ratespecial'             THEN lr.lr_ratespecial
            WHEN 'lr_ratescheduledafterhours' THEN lr.lr_ratescheduledafterhours
            WHEN 'lr_rateregulardiscount'     THEN lr.lr_rateregulardiscount
            WHEN 'lr_ratehelper'              THEN lr.lr_ratehelper
            WHEN 'lr_ratehelperovertime'      THEN lr.lr_ratehelperovertime END AS sr_rate,
       xccc.xccc_minimumlaborchargeinminutes                    AS min_labor_minutes,
       br.br_roundtominute                                      AS round_to_minutes,
       lr.lr_tripcharge                                         AS trade_flat_tripcharge,
       sr.sr_tripcharge_worked                                  AS tripcharge_worked_pct,
       sr.sr_tripcharge_quote                                   AS tripcharge_quote_pct,
       sr.sr_quotedtimestart,
       sr.sr_tripcharge_worked_billed, sr.sr_tripcharge_quote_billed,
       sr.sr_totaldue                                           AS invoice_totaldue_snapshot,
       (SELECT COUNT(*) FROM dbo.LaborRate l2 WHERE l2.xccc_id = sr.xccc_id AND l2.t_id = sr.t_id) AS laborrate_rows_for_pairing_trade
FROM dbo.ServiceRequest sr
LEFT JOIN dbo.Status s ON s.s_id = sr.s_id
LEFT JOIN dbo.xrefCompanyCallCenter xccc ON xccc.xccc_id = sr.xccc_id
LEFT JOIN dbo.BillableRule br ON br.br_id = xccc.br_id
OUTER APPLY (SELECT TOP 1 * FROM dbo.LaborRate l WHERE l.xccc_id = sr.xccc_id AND l.t_id = sr.t_id ORDER BY l.lr_id) lr
LEFT JOIN dbo.LaborRateType lrt ON lrt.lrt_id = sr.lrt_id
WHERE sr.sr_id = @sr_id;

-- 2) Check-ins on service work orders (ttt_id = 3, wot_id = 1), as the scan bills them
;WITH Tt AS (
    SELECT tt.tt_id, wo.wo_workordernumber, u.u_firstname + ' ' + u.u_lastname AS tech,
           tt.tt_begin, tt.tt_end, ISNULL(tt.tt_invoicedrate, 0) AS rate,
           CASE WHEN tt.tt_end IS NULL THEN 1 ELSE 0 END AS is_open,
           CASE WHEN tt.tt_end IS NULL AND tt.tt_begin < DATEADD(HOUR, -@MaxOpenCheckInHours, GETUTCDATE()) THEN 1 ELSE 0 END AS is_stale,
           DATEDIFF(MINUTE, tt.tt_begin, ISNULL(tt.tt_end, GETUTCDATE())) AS minutes_raw,
           ISNULL(xccc.xccc_minimumlaborchargeinminutes, 0) AS min_minutes,
           ISNULL(br.br_roundtominute, 0) AS round_minutes
    FROM dbo.TimeTracking tt
    INNER JOIN dbo.WorkOrder wo ON wo.wo_id = tt.wo_id
    INNER JOIN dbo.ServiceRequest sr ON sr.sr_id = wo.sr_id
    LEFT JOIN dbo.xrefCompanyCallCenter xccc ON xccc.xccc_id = sr.xccc_id
    LEFT JOIN dbo.BillableRule br ON br.br_id = xccc.br_id
    LEFT JOIN dbo.[user] u ON u.u_id = tt.u_id
    WHERE wo.sr_id = @sr_id AND tt.ttt_id = 3 AND wo.wot_id = 1 AND tt.tt_begin IS NOT NULL
), Billed AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY tt_begin, tt_id) AS rn,
           CASE WHEN round_minutes > 0 THEN CEILING(minutes_raw * 1.0 / round_minutes) * round_minutes ELSE minutes_raw END AS minutes_rounded
    FROM Tt WHERE is_stale = 0
)
SELECT b.tt_id, b.wo_workordernumber, b.tech, b.tt_begin, b.tt_end, b.is_open, 0 AS is_stale,
       b.minutes_raw, b.minutes_rounded,
       CASE WHEN b.rn = 1 AND b.minutes_rounded < b.min_minutes THEN b.min_minutes ELSE b.minutes_rounded END AS minutes_billed,
       b.rate,
       b.rate * ROUND((CASE WHEN b.rn = 1 AND b.minutes_rounded < b.min_minutes THEN b.min_minutes ELSE b.minutes_rounded END) / 60.0, 2) AS labor_amount
FROM Billed b
UNION ALL
SELECT t.tt_id, t.wo_workordernumber, t.tech, t.tt_begin, t.tt_end, t.is_open, 1, t.minutes_raw, NULL, 0, t.rate, 0
FROM Tt t WHERE t.is_stale = 1
ORDER BY tt_begin, tt_id;

-- 3) Service items on service work orders
SELECT x.xwosi_id, wo.wo_workordernumber, si.si_name,
       ISNULL(x.xwosi_forquote, 0) AS for_quote,
       x.xwosi_basecost, x.xwosi_quantity, x.xwosi_percentagetax, x.xwosi_percentagemarkup,
       ROUND((ISNULL(x.xwosi_basecost, 0) * ISNULL(x.xwosi_quantity, 0))
             * (1 + ISNULL(x.xwosi_percentagetax, 0) / 100.0)
             * (1 + ISNULL(x.xwosi_percentagemarkup, 0) / 100.0), 2) AS amount,
       CASE WHEN ISNULL(x.xwosi_forquote, 0) = 1 THEN 'no (quoted)' ELSE 'yes' END AS counted
FROM dbo.xrefWorkOrderServiceItem x
INNER JOIN dbo.WorkOrder wo ON wo.wo_id = x.wo_id
LEFT JOIN dbo.ServiceItem si ON si.si_id = x.si_id
WHERE wo.sr_id = @sr_id AND wo.wot_id = 1
ORDER BY x.xwosi_id;

-- 4) Quoted labor hours (TimeQuoted) - NOT counted by the scan or the invoice; shown for reference
SELECT tq.* FROM dbo.TimeQuoted tq WHERE tq.sr_id = @sr_id;

-- 5) Totals the scan would report for this SR right now
;WITH A AS (
    SELECT sr.sr_id, sr.sr_nte,
           LOWER(ISNULL(NULLIF(sr.sr_flatorhourly, ''), 'hourly')) AS flatorhourly,
           ISNULL(sr.sr_rateflat, 0) AS sr_rateflat,
           ISNULL(sr.sr_tripcharge_worked, 0) AS tc_worked_pct, ISNULL(sr.sr_tripcharge_quote, 0) AS tc_quote_pct,
           sr.sr_tripcharge_quote_billed, sr.sr_quotedtimestart,
           ISNULL(xccc.xccc_minimumlaborchargeinminutes, 0) AS min_minutes, ISNULL(br.br_roundtominute, 0) AS round_minutes,
           ISNULL(lr.lr_tripcharge, 0) AS lr_tripcharge,
           CASE lrt.lrt_fieldname
                WHEN 'lr_rateregular' THEN lr.lr_rateregular WHEN 'lr_rateovertime' THEN lr.lr_rateovertime
                WHEN 'lr_rateholiday' THEN lr.lr_rateholiday WHEN 'lr_ratespecial' THEN lr.lr_ratespecial
                WHEN 'lr_ratescheduledafterhours' THEN lr.lr_ratescheduledafterhours WHEN 'lr_rateregulardiscount' THEN lr.lr_rateregulardiscount
                WHEN 'lr_ratehelper' THEN lr.lr_ratehelper WHEN 'lr_ratehelperovertime' THEN lr.lr_ratehelperovertime END AS sr_rate
    FROM dbo.ServiceRequest sr
    LEFT JOIN dbo.xrefCompanyCallCenter xccc ON xccc.xccc_id = sr.xccc_id
    LEFT JOIN dbo.BillableRule br ON br.br_id = xccc.br_id
    OUTER APPLY (SELECT TOP 1 * FROM dbo.LaborRate l WHERE l.xccc_id = sr.xccc_id AND l.t_id = sr.t_id ORDER BY l.lr_id) lr
    LEFT JOIN dbo.LaborRateType lrt ON lrt.lrt_id = sr.lrt_id
    WHERE sr.sr_id = @sr_id
), Tt AS (
    SELECT tt.tt_id, tt.tt_begin, ISNULL(tt.tt_invoicedrate, 0) AS rate,
           CASE WHEN tt.tt_end IS NULL THEN 1 ELSE 0 END AS is_open,
           CASE WHEN tt.tt_end IS NULL AND tt.tt_begin < DATEADD(HOUR, -@MaxOpenCheckInHours, GETUTCDATE()) THEN 1 ELSE 0 END AS is_stale,
           DATEDIFF(MINUTE, tt.tt_begin, ISNULL(tt.tt_end, GETUTCDATE())) AS minutes_raw
    FROM dbo.TimeTracking tt INNER JOIN dbo.WorkOrder wo ON wo.wo_id = tt.wo_id
    WHERE wo.sr_id = @sr_id AND tt.ttt_id = 3 AND wo.wot_id = 1 AND tt.tt_begin IS NOT NULL
), B AS (
    SELECT t.rate, t.is_open, ROW_NUMBER() OVER (ORDER BY t.tt_begin, t.tt_id) AS rn,
           CASE WHEN a.round_minutes > 0 THEN CEILING(t.minutes_raw * 1.0 / a.round_minutes) * a.round_minutes ELSE t.minutes_raw END AS minutes_rounded,
           a.min_minutes
    FROM Tt t CROSS JOIN A a WHERE t.is_stale = 0
), L AS (
    SELECT COUNT(*) AS tt_rows, SUM(is_open) AS open_checkins,
           SUM(rate * ROUND((CASE WHEN rn = 1 AND minutes_rounded < min_minutes THEN min_minutes ELSE minutes_rounded END) / 60.0, 2)) AS labor_hourly,
           MAX(CASE WHEN rn = 1 THEN NULLIF(rate, 0) END) AS first_rate
    FROM B
), I AS (
    SELECT SUM(CASE WHEN ISNULL(x.xwosi_forquote, 0) = 0 THEN amt ELSE 0 END) AS items_incurred,
           SUM(CASE WHEN ISNULL(x.xwosi_forquote, 0) = 1 THEN amt ELSE 0 END) AS items_quoted
    FROM (SELECT x.wo_id, x.xwosi_forquote,
                 ROUND((ISNULL(x.xwosi_basecost, 0) * ISNULL(x.xwosi_quantity, 0)) * (1 + ISNULL(x.xwosi_percentagetax, 0) / 100.0) * (1 + ISNULL(x.xwosi_percentagemarkup, 0) / 100.0), 2) AS amt
          FROM dbo.xrefWorkOrderServiceItem x) x
    INNER JOIN dbo.WorkOrder wo ON wo.wo_id = x.wo_id
    WHERE wo.sr_id = @sr_id AND wo.wot_id = 1
)
SELECT a.sr_nte AS nte,
       CAST(CASE WHEN a.flatorhourly = 'flat' THEN CASE WHEN ISNULL(l.tt_rows, 0) > 0 THEN a.sr_rateflat ELSE 0 END ELSE ISNULL(l.labor_hourly, 0) END AS decimal(18,2)) AS labor,
       CAST(ISNULL(i.items_incurred, 0) AS decimal(18,2)) AS service_items,
       CAST(ISNULL(i.items_quoted, 0) AS decimal(18,2)) AS quoted_items_excluded,
       CAST(CASE WHEN a.lr_tripcharge > 0 THEN a.lr_tripcharge ELSE ROUND(COALESCE(l.first_rate, a.sr_rate, 0) * a.tc_worked_pct / 100.0, 2) END AS decimal(18,2)) AS trip_worked,
       CAST(CASE WHEN a.sr_tripcharge_quote_billed IS NOT NULL THEN a.sr_tripcharge_quote_billed
                 WHEN a.sr_quotedtimestart IS NULL THEN 0
                 WHEN a.lr_tripcharge > 0 THEN a.lr_tripcharge
                 ELSE ROUND(ISNULL(a.sr_rate, 0) * a.tc_quote_pct / 100.0, 2) END AS decimal(18,2)) AS quoted_trip_excluded,
       CASE WHEN a.lr_tripcharge > 0 THEN 'trade flat $' ELSE 'rate ' + CAST(COALESCE(l.first_rate, a.sr_rate, 0) AS varchar(20)) + ' x ' + CAST(a.tc_worked_pct AS varchar(20)) + '%' END AS trip_worked_source,
       ISNULL(l.open_checkins, 0) AS open_checkins
FROM A a CROSS JOIN L l CROSS JOIN I i;
