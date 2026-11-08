-- NTE notification: how many open SRs with an NTE would the scan consider, and how many are
-- already past 75% / 100%, bucketed by last activity and by status. READ-ONLY. Safe on PROD.
-- Same spend arithmetic as SqlNteSpendCalculator.ActiveSpendSql (keep in sync).
-- Purpose: size the population before go-live. Under D9 only rows with an open check-in can alert, so
-- with_open_checkin is the number of SRs the scan actually considers; the rest is context.
DECLARE @MaxOpenCheckInHours int = 24;

        WITH ActiveSr AS (
            SELECT sr.sr_id, sr.sr_nte,
                   ISNULL(sr.sr_nteexcludequotedsi, 0)                     AS exclude_quoted,
                   LOWER(ISNULL(NULLIF(sr.sr_flatorhourly, ''), 'hourly'))  AS flatorhourly,
                   ISNULL(sr.sr_rateflat, 0)                                AS sr_rateflat,
                   ISNULL(sr.sr_tripcharge_worked, 0)                       AS tc_worked_pct,
                   ISNULL(sr.sr_tripcharge_quote, 0)                        AS tc_quote_pct,
                   sr.sr_tripcharge_quote_billed,
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
            -- TOP 1: LaborRate has duplicate (xccc_id, t_id) rows until migration
            -- 2026-09-16_add_laborrate_unique_customer_trade.sql has run; a join would double-count.
            OUTER APPLY (SELECT TOP 1 lr.lr_tripcharge, lr.lr_rateregular, lr.lr_rateovertime, lr.lr_rateholiday,
                                      lr.lr_ratespecial, lr.lr_ratescheduledafterhours, lr.lr_rateregulardiscount,
                                      lr.lr_ratehelper, lr.lr_ratehelperovertime
                         FROM dbo.LaborRate lr WITH (NOLOCK)
                         WHERE lr.xccc_id = sr.xccc_id AND lr.t_id = sr.t_id
                         ORDER BY lr.lr_id) lr
            LEFT JOIN dbo.LaborRateType lrt WITH (NOLOCK)          ON lrt.lrt_id = sr.lrt_id
            WHERE sr.s_id NOT IN (6, 9)
              AND sr.sr_nte IS NOT NULL
              AND sr.sr_nte > 0
        ),
        -- Check-in rows on service work orders. Open rows run to now, unless they are older than
        -- @MaxOpenCheckInHours: those are treated as a forgotten check-out and ignored (D8).
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
        -- Round each row up to the billable rule; apply the company minimum once, to the first row.
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
        -- ((base * qty) + tax) * (1 + markup%) per item, rounded like CreateInvoice.
        -- xwosi_percentagemarkup already includes the supplier markup.
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
        Spend AS (
SELECT
            a.sr_id  AS SrId,
            a.sr_nte AS Nte,
            CAST(CASE WHEN a.flatorhourly = 'flat'
                      THEN CASE WHEN ISNULL(l.tt_rows, 0) > 0 THEN a.sr_rateflat ELSE 0 END
                      ELSE ISNULL(l.labor_hourly, 0) END AS decimal(18, 2))                       AS Labor,
            CAST(ISNULL(i.items_incurred, 0)
                 + CASE WHEN a.exclude_quoted = 1 THEN 0 ELSE ISNULL(i.items_quoted, 0) END
                 AS decimal(18, 2))                                                               AS ServiceItems,
            CAST(
                -- worked trip charge: trade flat $ wins, else % of the hourly rate (first check-in rate, else the SR rate type)
                CASE WHEN a.lr_tripcharge > 0 THEN a.lr_tripcharge
                     ELSE ROUND(COALESCE(l.first_rate, a.sr_rate, 0) * a.tc_worked_pct / 100.0, 2) END
                -- quoted trip charge: whatever invoicing already billed, else the same cascade when the SR is quoted
                + CASE WHEN a.sr_tripcharge_quote_billed IS NOT NULL THEN a.sr_tripcharge_quote_billed
                       WHEN a.sr_quotedtimestart IS NULL THEN 0
                       WHEN a.lr_tripcharge > 0 THEN a.lr_tripcharge
                       ELSE ROUND(ISNULL(a.sr_rate, 0) * a.tc_quote_pct / 100.0, 2) END
                AS decimal(18, 2))                                                                AS TripCharge,
            ISNULL(l.open_checkins, 0)                                                            AS OpenCheckIns,
            ISNULL(s.stale_checkins, 0)                                                           AS StaleCheckIns
        FROM ActiveSr a
        LEFT JOIN Labor l ON l.sr_id = a.sr_id
        LEFT JOIN Stale s ON s.sr_id = a.sr_id
        LEFT JOIN Items i ON i.sr_id = a.sr_id
        ),
        Act AS (
            SELECT s.SrId, s.Nte, s.Labor + s.ServiceItems + s.TripCharge AS Spent,
                   CASE WHEN s.Nte > 0 THEN (s.Labor + s.ServiceItems + s.TripCharge) / s.Nte * 100 ELSE 0 END AS Pct,
                   s.OpenCheckIns, s.StaleCheckIns,
                   st.s_status AS status,
                   sr.sr_insertdatetime, sr.sr_modifieddatetime,
                   (SELECT MAX(tt.tt_begin) FROM dbo.TimeTracking tt WITH (NOLOCK)
                     INNER JOIN dbo.WorkOrder wo WITH (NOLOCK) ON wo.wo_id = tt.wo_id
                     WHERE wo.sr_id = s.SrId AND tt.ttt_id = 3) AS last_checkin,
                   (SELECT MAX(ISNULL(x.xwosi_modifieddatetime, x.xwosi_insertdatetime)) FROM dbo.xrefWorkOrderServiceItem x WITH (NOLOCK)
                     INNER JOIN dbo.WorkOrder wo WITH (NOLOCK) ON wo.wo_id = x.wo_id
                     WHERE wo.sr_id = s.SrId) AS last_item
            FROM Spend s
            INNER JOIN dbo.ServiceRequest sr WITH (NOLOCK) ON sr.sr_id = s.SrId
            LEFT JOIN dbo.Status st WITH (NOLOCK) ON st.s_id = sr.s_id
        ),
        Final AS (
            SELECT *, DATEDIFF(DAY, (SELECT MAX(d) FROM (VALUES (last_checkin), (last_item), (sr_modifieddatetime), (sr_insertdatetime)) AS v(d)), GETUTCDATE()) AS days_since_activity
            FROM Act
        )
        SELECT 'TOTAL' AS grp, 'all' AS val, COUNT(*) AS srs,
               SUM(CASE WHEN Pct >= 75 THEN 1 ELSE 0 END) AS ge75, SUM(CASE WHEN Pct >= 100 THEN 1 ELSE 0 END) AS ge100,
               SUM(CASE WHEN OpenCheckIns > 0 THEN 1 ELSE 0 END) AS with_open_checkin, SUM(StaleCheckIns) AS stale_checkins
        FROM Final
        UNION ALL
        SELECT 'ACTIVITY', CASE WHEN days_since_activity <= 7 THEN '1: 0-7 days'
                                WHEN days_since_activity <= 30 THEN '2: 8-30 days'
                                WHEN days_since_activity <= 90 THEN '3: 31-90 days'
                                WHEN days_since_activity <= 365 THEN '4: 91-365 days'
                                ELSE '5: over a year' END, COUNT(*),
               SUM(CASE WHEN Pct >= 75 THEN 1 ELSE 0 END), SUM(CASE WHEN Pct >= 100 THEN 1 ELSE 0 END),
               SUM(CASE WHEN OpenCheckIns > 0 THEN 1 ELSE 0 END), SUM(StaleCheckIns)
        FROM Final
        GROUP BY CASE WHEN days_since_activity <= 7 THEN '1: 0-7 days'
                      WHEN days_since_activity <= 30 THEN '2: 8-30 days'
                      WHEN days_since_activity <= 90 THEN '3: 31-90 days'
                      WHEN days_since_activity <= 365 THEN '4: 91-365 days'
                      ELSE '5: over a year' END
        UNION ALL
        SELECT 'STATUS', ISNULL(status, '?'), COUNT(*),
               SUM(CASE WHEN Pct >= 75 THEN 1 ELSE 0 END), SUM(CASE WHEN Pct >= 100 THEN 1 ELSE 0 END),
               SUM(CASE WHEN OpenCheckIns > 0 THEN 1 ELSE 0 END), SUM(StaleCheckIns)
        FROM Final
        GROUP BY status
        ORDER BY grp, val;
