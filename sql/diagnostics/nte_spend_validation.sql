-- NTE notification: validate the live spend formula (SqlNteSpendCalculator.ActiveSpendSql) against
-- sr_totaldue on recently invoiced service requests. Read-only.
--
-- Same arithmetic as the C# constant except the ActiveSr filter: this picks the most recent @TakeRows
-- service requests that have an invoice total stored, open or closed, so the two numbers can be compared.
-- Expected, explainable differences (do NOT chase these):
--   * trip_charge: the invoice zeroes the worked trip charge when no time was worked; the scan counts it (D3).
--   * service_items: when sr_nteexcludequotedsi = 1 the scan drops quoted items; sr_totaldue keeps them (D7).
--   * sr_totaldue is a snapshot from the last CreateInvoice; items or time added since will not be in it.
-- Everything else should match to the cent. Rows where it does not are the ones to look at.
-- Keep in sync with the C# constant when the formula changes.

SET NOCOUNT ON;
DECLARE @TakeRows int = 100;
DECLARE @MaxOpenCheckInHours int = 24;   -- mirror NteConfig/MaxOpenCheckInHours (D8)

WITH ActiveSr AS (
    SELECT TOP (@TakeRows)
           sr.sr_id, sr.sr_nte, sr.sr_requestnumber, sr.s_id, sr.sr_totaldue,
           ISNULL(sr.sr_nteexcludequotedsi, 0)                     AS exclude_quoted,
           LOWER(ISNULL(NULLIF(sr.sr_flatorhourly, ''), 'hourly'))  AS flatorhourly,
           ISNULL(sr.sr_rateflat, 0)                                AS sr_rateflat,
           ISNULL(sr.sr_tripcharge_worked, 0)                       AS tc_worked_pct,
           ISNULL(sr.sr_tripcharge_quote, 0)                        AS tc_quote_pct,
           sr.sr_tripcharge_quote_billed,
           sr.sr_tripcharge_worked_billed,
           sr.sr_quotedtimestart,
           ISNULL(xccc.xccc_minimumlaborchargeinminutes, 0)         AS min_minutes,
           ISNULL(br.br_roundtominute, 0)                           AS round_minutes,
           ISNULL(lr.lr_tripcharge, 0)                              AS lr_tripcharge,
           CASE lrt.lrt_fieldname
                WHEN 'lr_rateregular'             THEN lr.lr_rateregular
                WHEN 'lr_rateovertime'            THEN lr.lr_rateovertime
                WHEN 'lr_rateholiday'             THEN lr.lr_rateholiday
                WHEN 'lr_ratespecial'             THEN lr.lr_ratespecial
                WHEN 'lr_ratescheduledafterhours' THEN lr.lr_ratescheduledafterhours
                WHEN 'lr_rateregulardiscount'     THEN lr.lr_rateregulardiscount
                WHEN 'lr_ratehelper'              THEN lr.lr_ratehelper
                WHEN 'lr_ratehelperovertime'      THEN lr.lr_ratehelperovertime
                ELSE NULL END                                       AS sr_rate
    FROM dbo.ServiceRequest sr WITH (NOLOCK)
    LEFT JOIN dbo.xrefCompanyCallCenter xccc WITH (NOLOCK) ON xccc.xccc_id = sr.xccc_id
    LEFT JOIN dbo.BillableRule br WITH (NOLOCK)            ON br.br_id = xccc.br_id
    -- TOP 1: LaborRate has duplicate (xccc_id, t_id) rows until migration 2026-09-16 has run; a join would double-count.
    OUTER APPLY (SELECT TOP 1 lr.lr_tripcharge, lr.lr_rateregular, lr.lr_rateovertime, lr.lr_rateholiday,
                              lr.lr_ratespecial, lr.lr_ratescheduledafterhours, lr.lr_rateregulardiscount,
                              lr.lr_ratehelper, lr.lr_ratehelperovertime
                 FROM dbo.LaborRate lr WITH (NOLOCK)
                 WHERE lr.xccc_id = sr.xccc_id AND lr.t_id = sr.t_id
                 ORDER BY lr.lr_id) lr
    LEFT JOIN dbo.LaborRateType lrt WITH (NOLOCK)          ON lrt.lrt_id = sr.lrt_id
    WHERE sr.sr_totaldue IS NOT NULL
      AND sr.sr_totaldue > 0
    ORDER BY sr.sr_id DESC
),
Tt AS (
    SELECT wo.sr_id, tt.tt_id, tt.tt_begin,
           ISNULL(tt.tt_invoicedrate, 0) AS rate,
           CASE WHEN tt.tt_end IS NULL THEN 1 ELSE 0 END AS is_open,
           CASE WHEN tt.tt_end IS NULL AND tt.tt_begin < DATEADD(HOUR, -@MaxOpenCheckInHours, GETUTCDATE()) THEN 1 ELSE 0 END AS is_stale,
           DATEDIFF(MINUTE, tt.tt_begin, ISNULL(tt.tt_end, GETUTCDATE())) AS minutes_raw
    FROM dbo.TimeTracking tt WITH (NOLOCK)
    INNER JOIN dbo.WorkOrder wo WITH (NOLOCK) ON wo.wo_id = tt.wo_id
    INNER JOIN ActiveSr a ON a.sr_id = wo.sr_id
    WHERE tt.ttt_id = 3
      AND wo.wot_id = 1
      AND tt.tt_begin IS NOT NULL
),
TtBilled AS (
    SELECT t.sr_id, t.rate, t.is_open,
           ROW_NUMBER() OVER (PARTITION BY t.sr_id ORDER BY t.tt_begin, t.tt_id) AS rn,
           CASE WHEN a.round_minutes > 0
                THEN CEILING(t.minutes_raw * 1.0 / a.round_minutes) * a.round_minutes
                ELSE t.minutes_raw END AS minutes_rounded,
           a.min_minutes
    FROM Tt t
    INNER JOIN ActiveSr a ON a.sr_id = t.sr_id
    WHERE t.is_stale = 0
),
Labor AS (
    SELECT sr_id,
           COUNT(*)                                              AS tt_rows,
           SUM(is_open)                                          AS open_checkins,
           SUM(rate * ROUND((CASE WHEN rn = 1 AND minutes_rounded < min_minutes THEN min_minutes ELSE minutes_rounded END) / 60.0, 2)) AS labor_hourly,
           MAX(CASE WHEN rn = 1 THEN NULLIF(rate, 0) END)        AS first_rate
    FROM TtBilled
    GROUP BY sr_id
),
Stale AS (
    SELECT sr_id, COUNT(*) AS stale_checkins FROM Tt WHERE is_stale = 1 GROUP BY sr_id
),
Items AS (
    SELECT wo.sr_id,
           SUM(CASE WHEN ISNULL(x.xwosi_forquote, 0) = 0 THEN x.amt ELSE 0 END) AS items_incurred,
           SUM(CASE WHEN ISNULL(x.xwosi_forquote, 0) = 1 THEN x.amt ELSE 0 END) AS items_quoted
    FROM (
        SELECT x.wo_id, x.xwosi_forquote,
               ROUND((ISNULL(x.xwosi_basecost, 0) * ISNULL(x.xwosi_quantity, 0))
                     * (1 + ISNULL(x.xwosi_percentagetax, 0) / 100.0)
                     * (1 + ISNULL(x.xwosi_percentagemarkup, 0) / 100.0), 2) AS amt
        FROM dbo.xrefWorkOrderServiceItem x WITH (NOLOCK)
    ) x
    INNER JOIN dbo.WorkOrder wo WITH (NOLOCK) ON wo.wo_id = x.wo_id
    INNER JOIN ActiveSr a ON a.sr_id = wo.sr_id
    WHERE wo.wot_id = 1
    GROUP BY wo.sr_id
),
Calc AS (
    SELECT
        a.sr_id, a.sr_requestnumber, a.s_id, a.sr_nte, a.sr_totaldue, a.exclude_quoted, a.flatorhourly,
        ISNULL(l.tt_rows, 0) AS tt_rows, ISNULL(l.open_checkins, 0) AS open_checkins, ISNULL(s.stale_checkins, 0) AS stale_checkins,
        CAST(CASE WHEN a.flatorhourly = 'flat'
                  THEN CASE WHEN ISNULL(l.tt_rows, 0) > 0 THEN a.sr_rateflat ELSE 0 END
                  ELSE ISNULL(l.labor_hourly, 0) END AS decimal(18, 2))                       AS labor,
        CAST(ISNULL(i.items_incurred, 0) AS decimal(18, 2))                                   AS items_incurred,
        CAST(ISNULL(i.items_quoted, 0) AS decimal(18, 2))                                     AS items_quoted,
        CAST(ISNULL(i.items_incurred, 0)
             + CASE WHEN a.exclude_quoted = 1 THEN 0 ELSE ISNULL(i.items_quoted, 0) END
             AS decimal(18, 2))                                                               AS service_items,
        CAST(CASE WHEN a.lr_tripcharge > 0 THEN a.lr_tripcharge
                  ELSE ROUND(COALESCE(l.first_rate, a.sr_rate, 0) * a.tc_worked_pct / 100.0, 2) END
             + CASE WHEN a.sr_tripcharge_quote_billed IS NOT NULL THEN a.sr_tripcharge_quote_billed
                    WHEN a.sr_quotedtimestart IS NULL THEN 0
                    WHEN a.lr_tripcharge > 0 THEN a.lr_tripcharge
                    ELSE ROUND(ISNULL(a.sr_rate, 0) * a.tc_quote_pct / 100.0, 2) END
             AS decimal(18, 2))                                                               AS trip_charge,
        CAST(ISNULL(a.sr_tripcharge_worked_billed, 0) + ISNULL(a.sr_tripcharge_quote_billed, 0) AS decimal(18, 2)) AS trip_charge_invoiced
    FROM ActiveSr a
    LEFT JOIN Labor l ON l.sr_id = a.sr_id
    LEFT JOIN Stale s ON s.sr_id = a.sr_id
    LEFT JOIN Items i ON i.sr_id = a.sr_id
)
SELECT sr_id, sr_requestnumber, s_id, sr_nte, flatorhourly, exclude_quoted, tt_rows, open_checkins, stale_checkins,
       labor, service_items, items_quoted, trip_charge, trip_charge_invoiced,
       labor + service_items + trip_charge          AS spent_scan,
       sr_totaldue                                   AS spent_invoice,
       labor + service_items + trip_charge - sr_totaldue AS diff,
       CASE WHEN ABS(labor + service_items + trip_charge - sr_totaldue) < 0.01 THEN 'match'
            WHEN ABS(labor + service_items + trip_charge - sr_totaldue - (trip_charge - trip_charge_invoiced)) < 0.01 THEN 'trip charge only (D3)'
            WHEN exclude_quoted = 1 AND ABS(labor + service_items + items_quoted + trip_charge - sr_totaldue) < 0.01 THEN 'quoted items excluded (D7)'
            ELSE 'LOOK' END                          AS verdict,
       CASE WHEN sr_nte > 0 THEN CAST(ROUND((labor + service_items + trip_charge) / sr_nte * 100, 1) AS decimal(18, 1)) END AS pct_of_nte
FROM Calc
ORDER BY CASE WHEN ABS(labor + service_items + trip_charge - sr_totaldue) < 0.01 THEN 1 ELSE 0 END, sr_id DESC;
