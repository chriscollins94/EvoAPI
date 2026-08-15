using System.Data.SqlClient;
using Dapper;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;

namespace EvoAPI.Infrastructure.Repositories;

public class PerformanceRepository : IPerformanceRepository
{
    private readonly string _connectionString;

    private const string EmployeeMetricColumns = @"
        pe.pe_utilization         AS Utilization,
        pe.pe_achlabortrip        AS AchLaborTrip,
        pe.pe_callouts            AS CallOuts,
        pe.pe_serviceitemspayback AS ServiceItemsPayback,
        pe.pe_truckfuelefficiency AS TruckFuelEfficiency,
        pe.pe_gallonsperday       AS GallonsPerDay,
        pe.pe_grossmargin         AS GrossMargin,
        pe.pe_receiptsviolations  AS ReceiptsViolations,
        pe.pe_callbacks           AS Callbacks,
        pe.pe_pendingtechinfo     AS PendingTechInfo,
        pe.pe_positiveqtrpct      AS PositiveQtrPct,
        pe.pe_profitgrade         AS ProfitGrade,
        pe.pe_healthscore         AS HealthScore";

    // Rev Per Tech Per Day and YTD Contribution are zone-file-only and have no
    // technician equivalent, so they appear here and not in EmployeeMetricColumns.
    // pz_profitgrade is intentionally absent: the zone file no longer carries a grade.
    private const string ZoneMetricColumns = @"
        pz.pz_revpertechperday    AS RevPerTechPerDay,
        pz.pz_ytdcontribution     AS YtdContribution,
        pz.pz_utilization         AS Utilization,
        pz.pz_achlabortrip        AS AchLaborTrip,
        pz.pz_callouts            AS CallOuts,
        pz.pz_serviceitemspayback AS ServiceItemsPayback,
        pz.pz_truckfuelefficiency AS TruckFuelEfficiency,
        pz.pz_gallonsperday       AS GallonsPerDay,
        pz.pz_grossmargin         AS GrossMargin,
        pz.pz_receiptsviolations  AS ReceiptsViolations,
        pz.pz_callbacks           AS Callbacks,
        pz.pz_pendingtechinfo     AS PendingTechInfo,
        pz.pz_positiveqtrpct      AS PositiveQtrPct";

    public PerformanceRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
    }

    public async Task<Dictionary<string, int>> GetEmployeeNumberMapAsync()
    {
        // Active users ordered last so they win duplicate employee numbers.
        const string sql = @"
            SELECT u_id, u_employeenumber
            FROM [user]
            WHERE u_employeenumber IS NOT NULL AND LTRIM(RTRIM(u_employeenumber)) <> ''
            ORDER BY u_active ASC";

        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.QueryAsync<(int UId, string EmployeeNumber)>(sql);

        var map = new Dictionary<string, int>();
        foreach (var row in rows)
        {
            var key = NormalizeEmployeeNumber(row.EmployeeNumber);
            if (key.Length > 0)
                map[key] = row.UId;
        }
        return map;
    }

    public async Task<Dictionary<string, int>> GetZoneAcronymMapAsync()
    {
        const string sql = "SELECT z_id, z_acronym FROM zone WHERE z_acronym IS NOT NULL";

        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.QueryAsync<(int ZId, string Acronym)>(sql);

        var map = new Dictionary<string, int>();
        foreach (var row in rows)
        {
            var key = row.Acronym?.Trim().ToUpperInvariant() ?? string.Empty;
            if (key.Length > 0)
                map[key] = row.ZId;
        }
        return map;
    }

    public async Task<HashSet<int>> GetUserIdsAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        var ids = await connection.QueryAsync<int>("SELECT u_id FROM [user]");
        return ids.ToHashSet();
    }

    public static string NormalizeEmployeeNumber(string? employeeNumber)
    {
        var trimmed = employeeNumber?.Trim() ?? string.Empty;
        var noZeros = trimmed.TrimStart('0');
        return noZeros.Length > 0 ? noZeros : trimmed;
    }

    public async Task<int> CreateEmployeeUploadAsync(DateTime reportDate, string? filename, int uploaderId, int skippedCount,
        List<(int UserId, PerformanceEmployeeRowDto Row)> rows)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        var uploadId = await connection.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.PerformanceUpload (pfu_type, pfu_reportdate, pfu_filename, u_id, pfu_rowcount, pfu_skippedcount)
            VALUES ('Employee', @ReportDate, @Filename, @UploaderId, @RowCount, @SkippedCount);
            SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { ReportDate = reportDate.Date, Filename = filename, UploaderId = uploaderId, RowCount = rows.Count, SkippedCount = skippedCount },
            transaction);

        const string insertRow = @"
            INSERT INTO dbo.PerformanceEmployee
                (pfu_id, u_id, pe_utilization, pe_achlabortrip, pe_callouts, pe_serviceitemspayback,
                 pe_truckfuelefficiency, pe_gallonsperday, pe_grossmargin, pe_receiptsviolations,
                 pe_callbacks, pe_pendingtechinfo, pe_positiveqtrpct, pe_profitgrade, pe_healthscore)
            VALUES
                (@UploadId, @UserId, @Utilization, @AchLaborTrip, @CallOuts, @ServiceItemsPayback,
                 @TruckFuelEfficiency, @GallonsPerDay, @GrossMargin, @ReceiptsViolations,
                 @Callbacks, @PendingTechInfo, @PositiveQtrPct, @ProfitGrade, @HealthScore)";

        foreach (var (userId, row) in rows)
        {
            await connection.ExecuteAsync(insertRow, new
            {
                UploadId = uploadId,
                UserId = userId,
                row.Utilization,
                row.AchLaborTrip,
                row.CallOuts,
                row.ServiceItemsPayback,
                row.TruckFuelEfficiency,
                row.GallonsPerDay,
                row.GrossMargin,
                row.ReceiptsViolations,
                row.Callbacks,
                row.PendingTechInfo,
                row.PositiveQtrPct,
                row.ProfitGrade,
                row.HealthScore
            }, transaction);
        }

        transaction.Commit();
        return uploadId;
    }

    public async Task<int> CreateZoneUploadAsync(DateTime reportDate, string? filename, int uploaderId, int skippedCount,
        List<(int ZoneId, PerformanceZoneRowDto Row)> rows)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        var uploadId = await connection.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.PerformanceUpload (pfu_type, pfu_reportdate, pfu_filename, u_id, pfu_rowcount, pfu_skippedcount)
            VALUES ('Zone', @ReportDate, @Filename, @UploaderId, @RowCount, @SkippedCount);
            SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { ReportDate = reportDate.Date, Filename = filename, UploaderId = uploaderId, RowCount = rows.Count, SkippedCount = skippedCount },
            transaction);

        const string insertRow = @"
            INSERT INTO dbo.PerformanceZone
                (pfu_id, z_id, pz_revpertechperday, pz_ytdcontribution,
                 pz_utilization, pz_achlabortrip, pz_callouts, pz_serviceitemspayback,
                 pz_truckfuelefficiency, pz_gallonsperday, pz_grossmargin, pz_receiptsviolations,
                 pz_callbacks, pz_pendingtechinfo, pz_positiveqtrpct)
            VALUES
                (@UploadId, @ZoneId, @RevPerTechPerDay, @YtdContribution,
                 @Utilization, @AchLaborTrip, @CallOuts, @ServiceItemsPayback,
                 @TruckFuelEfficiency, @GallonsPerDay, @GrossMargin, @ReceiptsViolations,
                 @Callbacks, @PendingTechInfo, @PositiveQtrPct)";

        foreach (var (zoneId, row) in rows)
        {
            await connection.ExecuteAsync(insertRow, new
            {
                UploadId = uploadId,
                ZoneId = zoneId,
                row.RevPerTechPerDay,
                row.YtdContribution,
                row.Utilization,
                row.AchLaborTrip,
                row.CallOuts,
                row.ServiceItemsPayback,
                row.TruckFuelEfficiency,
                row.GallonsPerDay,
                row.GrossMargin,
                row.ReceiptsViolations,
                row.Callbacks,
                row.PendingTechInfo,
                row.PositiveQtrPct
            }, transaction);
        }

        transaction.Commit();
        return uploadId;
    }

    public async Task<List<PerformanceUploadDto>> GetUploadsAsync(string? type)
    {
        var sql = @"
            SELECT
                pu.pfu_id             AS UploadId,
                pu.pfu_type           AS Type,
                pu.pfu_reportdate     AS ReportDate,
                pu.pfu_filename       AS Filename,
                pu.u_id               AS UploadedById,
                u.u_firstname + ' ' + u.u_lastname AS UploadedByName,
                pu.pfu_rowcount       AS [RowCount],
                pu.pfu_skippedcount   AS SkippedCount,
                pu.pfu_insertdatetime AS InsertDateTime
            FROM dbo.PerformanceUpload pu
            LEFT JOIN [user] u ON u.u_id = pu.u_id";

        if (!string.IsNullOrEmpty(type))
            sql += " WHERE pu.pfu_type = @Type";

        sql += " ORDER BY pu.pfu_reportdate DESC, pu.pfu_insertdatetime DESC";

        using var connection = new SqlConnection(_connectionString);
        var result = await connection.QueryAsync<PerformanceUploadDto>(sql, new { Type = type });
        return result.ToList();
    }

    public async Task<bool> DeleteUploadAsync(int uploadId)
    {
        using var connection = new SqlConnection(_connectionString);
        var affected = await connection.ExecuteAsync(
            "DELETE FROM dbo.PerformanceUpload WHERE pfu_id = @UploadId", new { UploadId = uploadId });
        return affected > 0;
    }

    public async Task<List<PerformanceEmployeeDto>> GetLatestEmployeePerformanceAsync()
    {
        var sql = $@"
            WITH Ranked AS (
                SELECT pe.*, pu.pfu_reportdate,
                       ROW_NUMBER() OVER (PARTITION BY pe.u_id ORDER BY pu.pfu_reportdate DESC, pu.pfu_insertdatetime DESC) AS rn
                FROM dbo.PerformanceEmployee pe
                INNER JOIN dbo.PerformanceUpload pu ON pu.pfu_id = pe.pfu_id
            )
            SELECT
                pe.u_id              AS UserId,
                u.u_firstname        AS FirstName,
                u.u_lastname         AS LastName,
                u.u_employeenumber   AS EmployeeNumber,
                z.z_acronym          AS ZoneAcronym,
                pe.pfu_id            AS UploadId,
                pe.pfu_reportdate    AS ReportDate,
                {EmployeeMetricColumns}
            FROM Ranked pe
            INNER JOIN [user] u ON u.u_id = pe.u_id
            LEFT JOIN zone z ON z.z_id = u.z_id
            WHERE pe.rn = 1
            ORDER BY u.u_firstname, u.u_lastname";

        using var connection = new SqlConnection(_connectionString);
        var result = await connection.QueryAsync<PerformanceEmployeeDto>(sql);
        return result.ToList();
    }

    public async Task<List<PerformanceEmployeeDto>> GetEmployeeHistoryAsync(int userId)
    {
        var sql = $@"
            SELECT
                pe.u_id              AS UserId,
                u.u_firstname        AS FirstName,
                u.u_lastname         AS LastName,
                u.u_employeenumber   AS EmployeeNumber,
                z.z_acronym          AS ZoneAcronym,
                pe.pfu_id            AS UploadId,
                pu.pfu_reportdate    AS ReportDate,
                {EmployeeMetricColumns}
            FROM dbo.PerformanceEmployee pe
            INNER JOIN dbo.PerformanceUpload pu ON pu.pfu_id = pe.pfu_id
            INNER JOIN [user] u ON u.u_id = pe.u_id
            LEFT JOIN zone z ON z.z_id = u.z_id
            WHERE pe.u_id = @UserId
            ORDER BY pu.pfu_reportdate ASC, pu.pfu_insertdatetime ASC";

        using var connection = new SqlConnection(_connectionString);
        var result = await connection.QueryAsync<PerformanceEmployeeDto>(sql, new { UserId = userId });
        return result.ToList();
    }

    public async Task<PerformanceEmployeeDto?> GetMyLatestPerformanceAsync(int userId)
    {
        var sql = $@"
            WITH Ranked AS (
                SELECT pe.*, pu.pfu_reportdate,
                       ROW_NUMBER() OVER (PARTITION BY pe.u_id ORDER BY pu.pfu_reportdate DESC, pu.pfu_insertdatetime DESC) AS rn
                FROM dbo.PerformanceEmployee pe
                INNER JOIN dbo.PerformanceUpload pu ON pu.pfu_id = pe.pfu_id
                WHERE pe.u_id = @UserId
            )
            SELECT
                pe.u_id              AS UserId,
                u.u_firstname        AS FirstName,
                u.u_lastname         AS LastName,
                u.u_employeenumber   AS EmployeeNumber,
                z.z_acronym          AS ZoneAcronym,
                pe.pfu_id            AS UploadId,
                pe.pfu_reportdate    AS ReportDate,
                {EmployeeMetricColumns}
            FROM Ranked pe
            INNER JOIN [user] u ON u.u_id = pe.u_id
            LEFT JOIN zone z ON z.z_id = u.z_id
            WHERE pe.rn = 1";

        using var connection = new SqlConnection(_connectionString);
        return await connection.QueryFirstOrDefaultAsync<PerformanceEmployeeDto>(sql, new { UserId = userId });
    }

    // Peer comparison for the technician-facing dashboard. The caller sees where they sit
    // among everyone, but the identifying columns are never selected for anyone else — not
    // nulled after the fact, simply never read. A column that isn't in the projection cannot
    // leak, however the result is later mapped or serialized.
    //
    // No zone is returned and no zone filter is offered: narrowing to a two-person zone would
    // identify people by elimination.
    //
    // Ordering matters as much as the columns. Alphabetical order would let anyone holding the
    // roster count down the list and name every row, so rows are ordered by health score
    // (nulls last) — position carries no identity signal.
    public async Task<List<PerformancePeerDto>> GetPeerComparisonAsync(int userId)
    {
        var sql = $@"
            WITH Ranked AS (
                SELECT pe.*, pu.pfu_reportdate,
                       ROW_NUMBER() OVER (PARTITION BY pe.u_id ORDER BY pu.pfu_reportdate DESC, pu.pfu_insertdatetime DESC) AS rn
                FROM dbo.PerformanceEmployee pe
                INNER JOIN dbo.PerformanceUpload pu ON pu.pfu_id = pe.pfu_id
            )
            SELECT
                CAST(CASE WHEN pe.u_id = @UserId THEN 1 ELSE 0 END AS bit) AS IsSelf,
                CASE WHEN pe.u_id = @UserId
                     THEN LTRIM(RTRIM(ISNULL(u.u_firstname, '') + ' ' + ISNULL(u.u_lastname, '')))
                END                  AS DisplayName,
                pe.pfu_reportdate    AS ReportDate,
                {EmployeeMetricColumns}
            FROM Ranked pe
            INNER JOIN [user] u ON u.u_id = pe.u_id
            WHERE pe.rn = 1
            ORDER BY CASE WHEN pe.pe_healthscore IS NULL THEN 1 ELSE 0 END,
                     pe.pe_healthscore DESC";

        using var connection = new SqlConnection(_connectionString);
        var result = await connection.QueryAsync<PerformancePeerDto>(sql, new { UserId = userId });
        return result.ToList();
    }

    public async Task<List<PerformanceZoneDto>> GetLatestZonePerformanceAsync()
    {
        var sql = $@"
            WITH Ranked AS (
                SELECT pz.*, pu.pfu_reportdate,
                       ROW_NUMBER() OVER (PARTITION BY pz.z_id ORDER BY pu.pfu_reportdate DESC, pu.pfu_insertdatetime DESC) AS rn
                FROM dbo.PerformanceZone pz
                INNER JOIN dbo.PerformanceUpload pu ON pu.pfu_id = pz.pfu_id
            )
            SELECT
                pz.z_id              AS ZoneId,
                z.z_acronym          AS ZoneAcronym,
                z.z_description      AS ZoneDescription,
                pz.pfu_id            AS UploadId,
                pz.pfu_reportdate    AS ReportDate,
                {ZoneMetricColumns}
            FROM Ranked pz
            INNER JOIN zone z ON z.z_id = pz.z_id
            WHERE pz.rn = 1
            ORDER BY z.z_acronym";

        using var connection = new SqlConnection(_connectionString);
        var result = await connection.QueryAsync<PerformanceZoneDto>(sql);
        return result.ToList();
    }

    public async Task<List<PerformanceZoneDto>> GetZoneHistoryAsync(int zoneId)
    {
        var sql = $@"
            SELECT
                pz.z_id              AS ZoneId,
                z.z_acronym          AS ZoneAcronym,
                z.z_description      AS ZoneDescription,
                pz.pfu_id            AS UploadId,
                pu.pfu_reportdate    AS ReportDate,
                {ZoneMetricColumns}
            FROM dbo.PerformanceZone pz
            INNER JOIN dbo.PerformanceUpload pu ON pu.pfu_id = pz.pfu_id
            INNER JOIN zone z ON z.z_id = pz.z_id
            WHERE pz.z_id = @ZoneId
            ORDER BY pu.pfu_reportdate ASC, pu.pfu_insertdatetime ASC";

        using var connection = new SqlConnection(_connectionString);
        var result = await connection.QueryAsync<PerformanceZoneDto>(sql, new { ZoneId = zoneId });
        return result.ToList();
    }

    public async Task<List<PerformanceTargetDto>> GetTargetsAsync()
    {
        const string sql = @"
            SELECT cs_identifier, cs_value, cs_description
            FROM ConfigSetting
            WHERE cs_type = 'PerformanceTarget'
            ORDER BY cs_id";

        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.QueryAsync<(string Identifier, string? Value, string? Description)>(sql);

        return rows.Select(r => new PerformanceTargetDto
        {
            Identifier = r.Identifier,
            Value = decimal.TryParse(r.Value, out var v) ? v : (decimal?)null,
            Description = r.Description
        }).ToList();
    }

    public async Task<PerformanceJobMixDto> GetEmployeeJobMixAsync(int userId)
    {
        using var connection = new SqlConnection(_connectionString);

        var lookbackValue = await connection.ExecuteScalarAsync<string?>(
            "SELECT TOP 1 cs_value FROM ConfigSetting WHERE cs_type = 'PerformanceTarget' AND cs_identifier = 'Perf.JobMixLookbackDays'");
        var lookbackDays = int.TryParse(lookbackValue, out var days) && days > 0 ? days : 365;

        // One row per distinct service request the tech touched (any work order);
        // an SR has a single trade + call center, so DISTINCT on sr_id is safe.
        //
        // Administrative time (vacation, training, doctor's appointments, personal, etc.)
        // is excluded from the job-mix charts: it isn't billable job work, and its
        // subtrades otherwise crowd out the real trades. Filtering on the parent trade
        // drops all 13 Administrative subtrades from the subtrade chart at the same time.
        // The Administrative* call centers are filtered too — today they carry only
        // Administrative-trade SRs, so this is a safety net rather than a second cut.
        const string sql = @"
            SELECT DISTINCT
                sr.sr_id,
                ISNULL(tp.t_trade, t.t_trade) AS ParentTrade,
                t.t_trade                     AS SubTrade,
                cc.cc_name                    AS CallCenter
            FROM servicerequest sr
            INNER JOIN xrefCompanyCallCenter xccc ON sr.xccc_id = xccc.xccc_id
            INNER JOIN callcenter cc ON xccc.cc_id = cc.cc_id
            INNER JOIN company c     ON xccc.c_id  = c.c_id
            INNER JOIN [status] s    ON sr.s_id    = s.s_id
            INNER JOIN trade t       ON sr.t_id    = t.t_id
            LEFT  JOIN trade tp      ON t.t_id_parent = tp.t_id
            INNER JOIN workorder wo  ON wo.sr_id   = sr.sr_id
            INNER JOIN xrefworkorderuser xwou ON xwou.wo_id = wo.wo_id
            WHERE xwou.u_id = @UserId
              AND sr.sr_insertdatetime >= DATEADD(DAY, -@LookbackDays, GETDATE())
              AND s.s_status <> 'Rejected'
              AND c.c_name NOT IN ('Metro Pipe Program', 'Metro Pipe Program 2')
              AND ISNULL(tp.t_trade, t.t_trade) <> 'Administrative'
              AND cc.cc_name NOT LIKE 'Administrative%'";

        var rows = (await connection.QueryAsync<(int SrId, string? ParentTrade, string? SubTrade, string? CallCenter)>(
            sql, new { UserId = userId, LookbackDays = lookbackDays })).ToList();

        static List<JobMixSliceDto> GroupBy(IEnumerable<(int SrId, string? ParentTrade, string? SubTrade, string? CallCenter)> data,
            Func<(int SrId, string? ParentTrade, string? SubTrade, string? CallCenter), string?> selector)
        {
            return data
                .GroupBy(r => string.IsNullOrWhiteSpace(selector(r)) ? "Unknown" : selector(r)!.Trim())
                .Select(g => new JobMixSliceDto { Label = g.Key, Count = g.Count() })
                .OrderByDescending(s => s.Count)
                .ThenBy(s => s.Label)
                .ToList();
        }

        return new PerformanceJobMixDto
        {
            LookbackDays = lookbackDays,
            TotalJobs = rows.Count,
            ParentTrades = GroupBy(rows, r => r.ParentTrade),
            SubTrades = GroupBy(rows, r => r.SubTrade),
            CallCenters = GroupBy(rows, r => r.CallCenter)
        };
    }

    public async Task<int> UpdateTargetsAsync(List<PerformanceTargetDto> targets)
    {
        const string sql = @"
            UPDATE ConfigSetting
            SET cs_value = @Value, cs_modifieddatetime = GETDATE()
            WHERE cs_type = 'PerformanceTarget' AND cs_identifier = @Identifier";

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        var updated = 0;
        foreach (var target in targets)
        {
            updated += await connection.ExecuteAsync(sql, new
            {
                target.Identifier,
                Value = target.Value?.ToString(System.Globalization.CultureInfo.InvariantCulture)
            }, transaction);
        }

        transaction.Commit();
        return updated;
    }
}
