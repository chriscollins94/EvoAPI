using System.Data.SqlClient;
using Dapper;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;

namespace EvoAPI.Infrastructure.Repositories;

public class XrfRepository : IXrfRepository
{
    private readonly string _connectionString;

    public XrfRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
    }

    public async Task<List<XrfBatchDto>> GetBatchesAsync()
    {
        // Same "top 10 most recent" rule as the High Volume wave dropdown.
        const string sql = @"
            SELECT TOP 10
                b.xrfb_id             AS XrfbId,
                b.xrfb_filename       AS Filename,
                b.xrfb_insertdatetime AS InsertDateTime,
                COUNT(d.xrfbd_id)     AS TotalCount,
                SUM(CASE WHEN d.xrfbd_completeddatetime IS NULL THEN 0 ELSE 1 END) AS CompletedCount
            FROM dbo.XrfBatch b
            LEFT JOIN dbo.XrfBatchDetail d ON d.xrfb_id = b.xrfb_id
            GROUP BY b.xrfb_id, b.xrfb_filename, b.xrfb_insertdatetime
            ORDER BY b.xrfb_id DESC";

        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.QueryAsync<XrfBatchDto>(sql);
        return rows.ToList();
    }

    public async Task<List<XrfTeamDto>> GetTeamsAsync()
    {
        // Teams are stored as text (mirrors hvbd_team); sort numerically where possible.
        // Counts cover every wave, not just today's, so the dropdown reads as overall progress.
        const string sql = @"
            SELECT xrfbd_team AS Team,
                   COUNT(*)   AS TotalCount,
                   SUM(CASE WHEN xrfbd_completeddatetime IS NULL THEN 0 ELSE 1 END) AS CompletedCount
            FROM dbo.XrfBatchDetail
            WHERE xrfbd_team IS NOT NULL AND LTRIM(RTRIM(xrfbd_team)) <> ''
            GROUP BY xrfbd_team
            ORDER BY TRY_CAST(xrfbd_team AS INT), xrfbd_team";

        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.QueryAsync<XrfTeamDto>(sql);
        return rows.ToList();
    }

    public async Task<List<XrfLocationDto>> GetActiveAsync(string team, string batch, string filter)
    {
        // Mirrors GetHighVolumeActive: incomplete rows plus anything completed today, so the
        // page's Incomplete / Completed Today toggle works client-side.
        //
        // The High Volume row is resolved through OUTER APPLY so a location still shows its
        // HV details when hvbd_id was not resolved at load (falls back to the premise number,
        // most recently completed HV row first). Text columns are CAST so the shape of the
        // legacy HV table cannot break the mapping. The XRF row's own meter number is
        // informational only and deliberately not selected.
        const string locationsSql = @"
            SELECT TOP 250
                x.xrfbd_id                AS XrfbdId,
                x.xrfb_id                 AS XrfbId,
                b.xrfb_filename           AS BatchName,
                x.hvbd_id                 AS HvbdId,
                x.xrfbd_premisenumber     AS PremiseNumber,
                x.xrfbd_team              AS Team,
                x.xrfbd_comment           AS Comment,
                x.xrfbd_result            AS Result,
                x.xrfbd_completeddatetime AS CompletedDateTime,
                x.u_id                    AS CompletedByUserId,
                CASE WHEN xu.u_id IS NULL THEN NULL
                     ELSE LTRIM(RTRIM(ISNULL(xu.u_firstname, '') + ' ' + ISNULL(xu.u_lastname, ''))) END AS CompletedByName,
                x.xrfbd_latitude          AS Latitude,
                x.xrfbd_longitude         AS Longitude,
                x.xrfbd_geoaccuracy       AS GeoAccuracy,

                hv.sr_id                                        AS SrId,
                CAST(sr.sr_requestnumber AS NVARCHAR(100))      AS SrRequestNumber,
                CAST(hv.hvbd_address AS NVARCHAR(200))          AS Address,
                CAST(hv.hvbd_city AS NVARCHAR(100))             AS City,
                CAST(hv.hvbd_state AS NVARCHAR(50))             AS State,
                CAST(hv.hvbd_zip AS NVARCHAR(20))               AS Zip,
                CAST(hv.hvbd_meternumber AS NVARCHAR(50))       AS MeterNumber,
                CAST(hv.hvbd_meterlocation AS NVARCHAR(100))    AS MeterLocation,
                CAST(hv.hvbd_stanpar AS NVARCHAR(100))          AS Stanpar,
                CAST(hv.hvbd_instructions AS NVARCHAR(MAX))     AS Instructions,
                TRY_CAST(hv.hvbd_premiselatitude AS FLOAT)      AS PremiseLatitude,
                TRY_CAST(hv.hvbd_premiselongitude AS FLOAT)     AS PremiseLongitude,
                CAST(hv.hvbd_team AS NVARCHAR(10))              AS HvTeam,
                hb.hvb_filename                                 AS HvBatchName,
                hv.hvbd_completeddatetime                       AS HvCompletedDateTime,
                CASE WHEN hu.u_id IS NULL THEN NULL
                     ELSE LTRIM(RTRIM(ISNULL(hu.u_firstname, '') + ' ' + ISNULL(hu.u_lastname, ''))) END AS HvCompletedByName
            FROM dbo.XrfBatchDetail x
            INNER JOIN dbo.XrfBatch b ON b.xrfb_id = x.xrfb_id
            LEFT JOIN dbo.[user] xu ON xu.u_id = x.u_id
            OUTER APPLY (
                SELECT TOP 1 h.*
                FROM dbo.HighVolumeBatchDetail h
                WHERE (x.hvbd_id IS NOT NULL AND h.hvbd_id = x.hvbd_id)
                   OR (x.hvbd_id IS NULL AND CAST(h.hvbd_premisenumber AS NVARCHAR(50)) = x.xrfbd_premisenumber)
                ORDER BY CASE WHEN h.hvbd_completeddatetime IS NULL THEN 1 ELSE 0 END,
                         h.hvbd_completeddatetime DESC,
                         h.hvbd_id DESC
            ) hv
            LEFT JOIN dbo.HighVolumeBatch hb ON hb.hvb_id = hv.hvb_id
            LEFT JOIN dbo.servicerequest sr ON sr.sr_id = hv.sr_id
            LEFT JOIN dbo.[user] hu ON hu.u_id = hv.u_id
            WHERE (x.xrfbd_completeddatetime IS NULL
                   OR CONVERT(date, x.xrfbd_completeddatetime) = CONVERT(date, GETDATE()))
              AND (@Team = '' OR x.xrfbd_team = @Team)
              AND (@Batch = '' OR b.xrfb_filename = @Batch)
              AND (@Filter = ''
                   OR x.xrfbd_premisenumber LIKE '%' + @Filter + '%'
                   OR hv.hvbd_meternumber LIKE '%' + @Filter + '%'
                   OR hv.hvbd_address LIKE '%' + @Filter + '%'
                   OR hv.hvbd_stanpar LIKE '%' + @Filter + '%')
            ORDER BY TRY_CAST(x.xrfbd_team AS INT), x.xrfbd_team, hv.hvbd_address, x.xrfbd_premisenumber";

        // Latest answer per question for the matched service requests (same de-dupe rule as
        // GetHighVolumeChecklistAnswer: duplicate rows can exist from double submits).
        const string answersSql = @"
            WITH Ranked AS (
                SELECT a.sr_id, a.clq_id, a.xsrcla_question, a.xsrcla_answer, a.xsrcla_order, a.att_id,
                       ROW_NUMBER() OVER (PARTITION BY a.sr_id, a.clq_id ORDER BY a.xsrcla_id DESC) AS rn
                FROM dbo.xrefservicerequestchecklistanswer a
                WHERE a.sr_id IN @SrIds
            )
            SELECT r.sr_id                                  AS SrId,
                   r.clq_id                                 AS ClqId,
                   CAST(r.xsrcla_question AS NVARCHAR(200)) AS Question,
                   CAST(r.xsrcla_answer AS NVARCHAR(MAX))   AS Answer,
                   TRY_CAST(r.xsrcla_order AS INT)          AS [Order],
                   r.att_id                                 AS AttId,
                   CAST(att.att_filename AS NVARCHAR(500))  AS AttFilename
            FROM Ranked r
            LEFT JOIN dbo.attachment att ON att.att_id = r.att_id
            WHERE r.rn = 1
            ORDER BY r.sr_id, r.xsrcla_order, r.clq_id";

        using var connection = new SqlConnection(_connectionString);
        var locations = (await connection.QueryAsync<XrfLocationDto>(locationsSql,
            new { Team = team, Batch = batch, Filter = filter })).ToList();

        var srIds = locations.Where(l => l.SrId.HasValue).Select(l => l.SrId!.Value).Distinct().ToList();
        if (srIds.Count > 0)
        {
            var answers = await connection.QueryAsync<XrfHvAnswerDto>(answersSql, new { SrIds = srIds });
            var bySr = answers.GroupBy(a => a.SrId).ToDictionary(g => g.Key, g => g.ToList());
            foreach (var location in locations)
            {
                if (location.SrId.HasValue && bySr.TryGetValue(location.SrId.Value, out var list))
                    location.Answers = list;
            }
        }

        return locations;
    }

    public async Task<XrfCompleteOutcome> CompleteAsync(int xrfbdId, int userId, string result, string? comment,
        double? latitude, double? longitude, int? geoAccuracy)
    {
        const string completeSql = @"
            UPDATE dbo.XrfBatchDetail
            SET xrfbd_completeddatetime = GETDATE(),
                u_id                    = @UserId,
                xrfbd_result            = @Result,
                xrfbd_comment           = @Comment,
                xrfbd_latitude          = @Latitude,
                xrfbd_longitude         = @Longitude,
                xrfbd_geoaccuracy       = @GeoAccuracy,
                xrfbd_modifieddatetime  = GETDATE()
            OUTPUT INSERTED.xrfbd_completeddatetime
            WHERE xrfbd_id = @XrfbdId
              AND xrfbd_completeddatetime IS NULL;";

        const string existsSql = "SELECT COUNT(1) FROM dbo.XrfBatchDetail WHERE xrfbd_id = @XrfbdId";

        const string nameSql = @"
            SELECT LTRIM(RTRIM(ISNULL(u_firstname, '') + ' ' + ISNULL(u_lastname, '')))
            FROM dbo.[user] WHERE u_id = @UserId";

        var trimmedComment = string.IsNullOrWhiteSpace(comment) ? null : comment.Trim();
        if (trimmedComment != null && trimmedComment.Length > 1000)
            trimmedComment = trimmedComment.Substring(0, 1000);

        // Only keep a position when both halves arrived; column is DECIMAL(9,6).
        var hasLocation = latitude.HasValue && longitude.HasValue;
        decimal? lat = hasLocation ? (decimal?)Math.Round(latitude!.Value, 6) : null;
        decimal? lon = hasLocation ? (decimal?)Math.Round(longitude!.Value, 6) : null;
        int? accuracy = hasLocation ? geoAccuracy : null;

        using var connection = new SqlConnection(_connectionString);
        var completedAt = await connection.ExecuteScalarAsync<DateTime?>(completeSql, new
        {
            XrfbdId = xrfbdId,
            UserId = userId,
            Result = result,
            Comment = trimmedComment,
            Latitude = lat,
            Longitude = lon,
            GeoAccuracy = accuracy
        });

        if (!completedAt.HasValue)
        {
            var exists = await connection.ExecuteScalarAsync<int>(existsSql, new { XrfbdId = xrfbdId }) > 0;
            return new XrfCompleteOutcome { Status = exists ? XrfCompleteStatus.AlreadyCompleted : XrfCompleteStatus.NotFound };
        }

        var name = await connection.ExecuteScalarAsync<string>(nameSql, new { UserId = userId }) ?? string.Empty;

        return new XrfCompleteOutcome
        {
            Status = XrfCompleteStatus.Completed,
            Result = new XrfCompleteResult
            {
                XrfbdId = xrfbdId,
                Result = result,
                CompletedDateTime = completedAt.Value,
                CompletedByName = name,
                LocationCaptured = hasLocation
            }
        };
    }
}
