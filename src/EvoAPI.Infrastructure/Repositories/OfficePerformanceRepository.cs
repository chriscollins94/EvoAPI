using System.Data.SqlClient;
using Dapper;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;

namespace EvoAPI.Infrastructure.Repositories;

/// <summary>
/// Live metrics for the office performance dashboard. Nothing is uploaded or stored for
/// this page — everything is computed on read from ServiceRequest, StatusSecondaryChange,
/// StatusChange and CustomerInquiry.
///
/// Attribution model: an admin's numbers cover the zones where they hold at least one
/// row in xrefAdminZoneStatusSecondary. Assignments carry no history, so this year's
/// numbers always reflect TODAY'S matrix, even for months before an assignment changed.
///
/// A service request's zone follows the attack-list convention: the primary work order's
/// first assigned tech → [user].z_id → zone, falling back to the denormalized
/// sr_zonenumber for requests with no dispatched tech.
///
/// Timestamps (ssc/sc/ci/sr insert datetimes) are UTC per the DB server clock; business
/// dates are Central. Jan 1 is always standard time (UTC-6), so the Central year
/// boundary is 06:00 UTC on Jan 1.
/// </summary>
public class OfficePerformanceRepository : IOfficePerformanceRepository
{
    private readonly string _connectionString;

    // Status secondaries with time-in-status cards. ss_code is stable across
    // environments where ss_id may not be, so groups resolve by code at query time.
    private static readonly (string Key, string Label)[] DurationGroups =
    {
        ("Incomplete-2",  "Needs to be Quoted"),
        ("Incomplete-20", "Needs Quote Written"),
        ("Incomplete-9",  "Need to Order Parts"),
        ("Complete-4",    "Complete - Ready to Invoice"),
    };

    // Status groups in the customer-inquiry analysis table. 'Complete-Other' is every
    // Complete-family status except Complete-4 (Ready to Invoice), reported as one row.
    private static readonly (string Key, string Label)[] InquiryGroups =
    {
        ("Incomplete-10",  "Needs to Be Scheduled"),
        ("Incomplete-12",  "Pending Tech Info"),
        ("Incomplete-17",  "Pending Tech Info - Basic"),
        ("Incomplete-20",  "Needs Quote Written"),
        ("Incomplete-9",   "Need to Order Parts"),
        ("Incomplete-5",   "Parts Ordered"),
        ("Complete-4",     "Complete - Ready to Invoice"),
        ("Complete-Other", "Complete - All Other Statuses"),
    };

    public OfficePerformanceRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
    }

    public async Task<List<OfficePerformanceAdminDto>> GetAdminsAsync()
    {
        const string sql = @"
            SELECT u.u_id                      AS UserId,
                   u.u_firstname               AS FirstName,
                   u.u_lastname                AS LastName,
                   COUNT(DISTINCT xazss.z_id)  AS ZoneCount,
                   COUNT(*)                    AS AssignmentCount
            FROM xrefAdminZoneStatusSecondary xazss WITH (NOLOCK)
            INNER JOIN [user] u WITH (NOLOCK) ON u.u_id = xazss.u_id
            WHERE u.u_active = 1
            GROUP BY u.u_id, u.u_firstname, u.u_lastname
            ORDER BY u.u_firstname, u.u_lastname";

        using var connection = new SqlConnection(_connectionString);
        var result = await connection.QueryAsync<OfficePerformanceAdminDto>(sql);
        return result.ToList();
    }

    // One round trip: temp tables scope the year + the admin's zones once, then each
    // result set aggregates off them. s_id 5/6/9 = Invoiced/Rejected/Paid — everything
    // else counts as "open".
    private const string SummarySql = @"
        SET NOCOUNT ON;

        DECLARE @NowUtc DATETIME = GETUTCDATE();
        DECLARE @NowCentral DATETIME = CONVERT(DATETIME, @NowUtc AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time');
        DECLARE @YearStart DATETIME = DATEADD(HOUR, 6, CONVERT(DATETIME, DATEFROMPARTS(YEAR(@NowCentral), 1, 1)));

        -- Zones the admin holds at least one status assignment in; @ZoneId narrows the metrics
        SELECT DISTINCT z.z_id
        INTO #AdminZones
        FROM xrefAdminZoneStatusSecondary xazss WITH (NOLOCK)
        INNER JOIN zone z WITH (NOLOCK) ON z.z_id = xazss.z_id
        WHERE xazss.u_id = @UserId
          AND (@ZoneId IS NULL OR z.z_id = @ZoneId);

        -- Status groups reported on. Named groups resolve by ss_code; 'Complete-Other'
        -- is the rest of the Complete family.
        SELECT ss.ss_id, ss.ss_code AS group_key
        INTO #StatusGroups
        FROM StatusSecondary ss WITH (NOLOCK)
        WHERE ss.ss_code IN ('Incomplete-2','Incomplete-20','Incomplete-9','Incomplete-10',
                             'Incomplete-12','Incomplete-17','Incomplete-5','Complete-4')
        UNION ALL
        SELECT ss.ss_id, 'Complete-Other'
        FROM StatusSecondary ss WITH (NOLOCK)
        WHERE ss.ss_code LIKE 'Complete-%' AND ss.ss_code <> 'Complete-4';

        -- Service requests any metric below can touch: open now, opened this year, or
        -- with a status change / customer inquiry this year (index-friendly unions).
        SELECT r.sr_id
        INTO #RelevantSr
        FROM (
            SELECT sr_id FROM servicerequest WITH (NOLOCK) WHERE sr_insertdatetime >= @YearStart
            UNION
            SELECT sr_id FROM servicerequest WITH (NOLOCK) WHERE s_id NOT IN (5, 6, 9)
            UNION
            SELECT sc.sr_id FROM StatusChange sc WITH (NOLOCK) WHERE sc.sc_insertdatetime >= @YearStart
            UNION
            SELECT wo.sr_id
            FROM StatusSecondaryChange ssc WITH (NOLOCK)
            INNER JOIN workorder wo WITH (NOLOCK) ON wo.wo_id = ssc.wo_id
            WHERE ssc.ssc_insertdatetime >= @YearStart
            UNION
            SELECT ci.sr_id FROM CustomerInquiry ci WITH (NOLOCK) WHERE ci.ci_insertdatetime >= @YearStart
        ) r;

        -- Zone attribution: primary WO's first assigned tech's zone (attack-list
        -- convention), else the SR's denormalized zone number. SRs outside the admin's
        -- zone set drop out here, which scopes every result set below.
        SELECT sr.sr_id,
               sr.sr_insertdatetime,
               sr.s_id,
               sr.wo_id_primary,
               wo.ss_id AS ss_id_current
        INTO #SrZone
        FROM #RelevantSr r
        INNER JOIN servicerequest sr WITH (NOLOCK) ON sr.sr_id = r.sr_id
        LEFT JOIN workorder wo WITH (NOLOCK) ON wo.wo_id = sr.wo_id_primary
        OUTER APPLY (
            SELECT TOP 1 u.z_id
            FROM xrefWorkOrderUser xwou WITH (NOLOCK)
            INNER JOIN [user] u WITH (NOLOCK) ON u.u_id = xwou.u_id
            WHERE xwou.wo_id = sr.wo_id_primary
            ORDER BY xwou.xwou_id ASC
        ) tz
        LEFT JOIN zone zf WITH (NOLOCK) ON zf.z_number = sr.sr_zonenumber
        WHERE COALESCE(tz.z_id, zf.z_id) IN (SELECT z_id FROM #AdminZones);

        CREATE CLUSTERED INDEX IX_SrZone ON #SrZone(sr_id);

        -- RS1: all zones the admin is assigned to (unfiltered — feeds the zone dropdown)
        SELECT DISTINCT z.z_id          AS ZoneId,
               z.z_number               AS ZoneNumber,
               z.z_acronym              AS ZoneAcronym,
               z.z_description          AS ZoneDescription
        FROM xrefAdminZoneStatusSecondary xazss WITH (NOLOCK)
        INNER JOIN zone z WITH (NOLOCK) ON z.z_id = xazss.z_id
        WHERE xazss.u_id = @UserId
        ORDER BY z.z_acronym;

        -- RS2: headline counts
        SELECT @YearStart AS YearStart,
               (SELECT COUNT(*) FROM #SrZone WHERE s_id NOT IN (5, 6, 9)) AS OpenNow,
               (SELECT COUNT(*) FROM #SrZone WHERE sr_insertdatetime >= @YearStart) AS OpenedYtd;

        -- RS3: invoiced this year. First transition into Invoiced (s_id 5) per SR so a
        -- request invoiced, reset and re-invoiced counts once.
        SELECT COUNT(*) AS InvoicedYtd,
               AVG(DATEDIFF(HOUR, i.sr_insertdatetime, i.first_invoiced) / 24.0) AS AvgDaysToInvoice
        FROM (
            SELECT sz.sr_id, sz.sr_insertdatetime, MIN(sc.sc_insertdatetime) AS first_invoiced
            FROM StatusChange sc WITH (NOLOCK)
            INNER JOIN #SrZone sz ON sz.sr_id = sc.sr_id
            WHERE sc.s_id_new = 5
              AND sc.sc_insertdatetime >= @YearStart
            GROUP BY sz.sr_id, sz.sr_insertdatetime
        ) i;

        -- RS4: times each status was entered this year (primary work order only)
        SELECT g.group_key AS GroupKey, COUNT(*) AS Entries
        FROM StatusSecondaryChange ssc WITH (NOLOCK)
        INNER JOIN #StatusGroups g ON g.ss_id = ssc.ss_id_new
        INNER JOIN workorder wo WITH (NOLOCK) ON wo.wo_id = ssc.wo_id
        INNER JOIN #SrZone sz ON sz.sr_id = wo.sr_id AND sz.wo_id_primary = ssc.wo_id
        WHERE ssc.ssc_insertdatetime >= @YearStart
        GROUP BY g.group_key;

        -- RS5: completed stays this year — the transition OUT of the status carries the
        -- minutes the request spent in it (NULL for pre-trigger history; AVG skips those)
        SELECT g.group_key AS GroupKey,
               COUNT(*) AS Stints,
               AVG(CAST(ssc.ssc_minutesinpriorstatus AS FLOAT)) AS AvgMinutes
        FROM StatusSecondaryChange ssc WITH (NOLOCK)
        INNER JOIN #StatusGroups g ON g.ss_id = ssc.ss_id_prior
        INNER JOIN workorder wo WITH (NOLOCK) ON wo.wo_id = ssc.wo_id
        INNER JOIN #SrZone sz ON sz.sr_id = wo.sr_id AND sz.wo_id_primary = ssc.wo_id
        WHERE ssc.ssc_insertdatetime >= @YearStart
        GROUP BY g.group_key;

        -- RS6: sitting in the status right now (current secondary status of the primary
        -- WO). Status began at the latest status-change row; SRs that never changed
        -- status fall back to their create date.
        SELECT g.group_key AS GroupKey,
               COUNT(*) AS CurrentCount,
               AVG(CAST(DATEDIFF(MINUTE, cs.status_start, @NowUtc) AS FLOAT)) AS AvgMinutes
        FROM #SrZone sz
        INNER JOIN #StatusGroups g ON g.ss_id = sz.ss_id_current
        CROSS APPLY (
            SELECT COALESCE(MAX(ssc.ssc_insertdatetime), sz.sr_insertdatetime) AS status_start
            FROM StatusSecondaryChange ssc WITH (NOLOCK)
            WHERE ssc.wo_id = sz.wo_id_primary
        ) cs
        WHERE sz.s_id NOT IN (6, 9)
        GROUP BY g.group_key;

        -- RS7: customer inquiries logged this year while the SR sat in each status.
        -- ci_minutesinstatus was stamped when the inquiry was logged — no reconstruction.
        SELECT g.group_key AS GroupKey,
               COUNT(*) AS InquiryCount,
               COUNT(DISTINCT ci.sr_id) AS SrCount,
               AVG(CAST(ci.ci_minutesinstatus AS FLOAT)) AS AvgMinutesAtInquiry
        FROM CustomerInquiry ci WITH (NOLOCK)
        INNER JOIN #StatusGroups g ON g.ss_id = ci.ss_id
        INNER JOIN #SrZone sz ON sz.sr_id = ci.sr_id
        WHERE ci.ci_active = 1
          AND ci.ci_insertdatetime >= @YearStart
        GROUP BY g.group_key;

        -- RS8: opened per Central-time month
        SELECT MONTH(sz.sr_insertdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time') AS [Month],
               COUNT(*) AS Cnt
        FROM #SrZone sz
        WHERE sz.sr_insertdatetime >= @YearStart
        GROUP BY MONTH(sz.sr_insertdatetime AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time');

        -- RS9: invoiced per Central-time month (same first-transition dedupe as RS3)
        SELECT MONTH(i.first_invoiced AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time') AS [Month],
               COUNT(*) AS Cnt
        FROM (
            SELECT sz.sr_id, MIN(sc.sc_insertdatetime) AS first_invoiced
            FROM StatusChange sc WITH (NOLOCK)
            INNER JOIN #SrZone sz ON sz.sr_id = sc.sr_id
            WHERE sc.s_id_new = 5
              AND sc.sc_insertdatetime >= @YearStart
            GROUP BY sz.sr_id
        ) i
        GROUP BY MONTH(i.first_invoiced AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time');

        DROP TABLE #AdminZones;
        DROP TABLE #StatusGroups;
        DROP TABLE #RelevantSr;
        DROP TABLE #SrZone;";

    public async Task<OfficePerformanceSummaryDto> GetSummaryAsync(int userId, int? zoneId)
    {
        using var connection = new SqlConnection(_connectionString);
        using var multi = await connection.QueryMultipleAsync(SummarySql,
            new { UserId = userId, ZoneId = zoneId }, commandTimeout: 120);

        var zones = (await multi.ReadAsync<OfficePerformanceZoneDto>()).ToList();
        var headline = await multi.ReadSingleAsync<(DateTime YearStart, int OpenNow, int OpenedYtd)>();
        var invoiced = await multi.ReadSingleAsync<(int InvoicedYtd, decimal? AvgDaysToInvoice)>();
        var entries = (await multi.ReadAsync<(string GroupKey, int Entries)>()).ToDictionary(r => r.GroupKey);
        var stints = (await multi.ReadAsync<(string GroupKey, int Stints, double? AvgMinutes)>()).ToDictionary(r => r.GroupKey);
        var current = (await multi.ReadAsync<(string GroupKey, int CurrentCount, double? AvgMinutes)>()).ToDictionary(r => r.GroupKey);
        var inquiries = (await multi.ReadAsync<(string GroupKey, int InquiryCount, int SrCount, double? AvgMinutesAtInquiry)>()).ToDictionary(r => r.GroupKey);
        var openedByMonth = (await multi.ReadAsync<(int Month, int Cnt)>()).ToDictionary(r => r.Month, r => r.Cnt);
        var invoicedByMonth = (await multi.ReadAsync<(int Month, int Cnt)>()).ToDictionary(r => r.Month, r => r.Cnt);

        static decimal? MinutesToDays(double? minutes) =>
            minutes.HasValue ? Math.Round((decimal)minutes.Value / 1440m, 2) : null;
        static decimal? MinutesToHours(double? minutes) =>
            minutes.HasValue ? Math.Round((decimal)minutes.Value / 60m, 1) : null;

        var summary = new OfficePerformanceSummaryDto
        {
            UserId = userId,
            ZoneId = zoneId,
            YearStart = headline.YearStart,
            Zones = zones,
            OpenNow = headline.OpenNow,
            OpenedYtd = headline.OpenedYtd,
            InvoicedYtd = invoiced.InvoicedYtd,
            AvgDaysToInvoice = invoiced.AvgDaysToInvoice.HasValue
                ? Math.Round(invoiced.AvgDaysToInvoice.Value, 1) : null,
        };

        foreach (var (key, label) in DurationGroups)
        {
            stints.TryGetValue(key, out var s);
            current.TryGetValue(key, out var c);
            summary.StatusDurations.Add(new OfficeStatusDurationDto
            {
                GroupKey = key,
                StatusName = label,
                CompletedStintsYtd = s.Stints,
                AvgDaysYtd = MinutesToDays(s.AvgMinutes),
                CurrentCount = c.CurrentCount,
                AvgDaysCurrent = MinutesToDays(c.AvgMinutes),
            });
        }

        foreach (var (key, label) in InquiryGroups)
        {
            entries.TryGetValue(key, out var e);
            stints.TryGetValue(key, out var s);
            inquiries.TryGetValue(key, out var q);
            summary.InquiryStatuses.Add(new OfficeInquiryStatusDto
            {
                GroupKey = key,
                StatusName = label,
                EntriesYtd = e.Entries,
                AvgHoursYtd = MinutesToHours(s.AvgMinutes),
                SrsWithInquiryYtd = q.SrCount,
                InquiryCountYtd = q.InquiryCount,
                AvgHoursAtInquiry = MinutesToHours(q.AvgMinutesAtInquiry),
            });
        }

        var lastMonth = Math.Max(openedByMonth.Keys.DefaultIfEmpty(0).Max(),
            invoicedByMonth.Keys.DefaultIfEmpty(0).Max());
        for (var month = 1; month <= lastMonth; month++)
        {
            summary.MonthlyTrend.Add(new OfficeMonthlyTrendDto
            {
                Month = month,
                Opened = openedByMonth.TryGetValue(month, out var o) ? o : 0,
                Invoiced = invoicedByMonth.TryGetValue(month, out var inv) ? inv : 0,
            });
        }

        return summary;
    }
}
