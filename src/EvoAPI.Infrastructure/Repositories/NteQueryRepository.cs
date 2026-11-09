using System.Data.SqlClient;
using Dapper;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;

namespace EvoAPI.Infrastructure.Repositories;

public class NteQueryRepository : INteQueryRepository
{
    private readonly string _connectionString;

    public NteQueryRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
    }

    // Scope + recipient rule (NTE_NOTIFICATION_PLAN.md D2 / D9):
    //   Only service requests with a tech currently checked in (TimeTracking ttt_id = 3 with no
    //   tt_end, on any work order of the SR) enter the scan. Open check-ins older than
    //   @MaxOpenCheckInHours are a forgotten check-out (D8) and do not count. One row per
    //   checked-in tech; two techs checked in at once both get the text.
    //   SRs nobody is checked in on are not returned at all: a job that is already past its NTE
    //   alerts the moment someone next checks in, which is when the warning matters.
    // Phone = Employee Directory "Evo Cell" (u_phonemobile) falling back to
    // "Personal Cell" (u_phonehome). Zone = the tech's own zone ([user].z_id).
    // Active = not closed/cancelled (s_id NOT IN (6, 9), matches DataService usage).
    public const string ActiveServiceRequestsSql = @"
        WITH CheckedIn AS (
            SELECT DISTINCT wo.sr_id, tt.u_id
            FROM dbo.TimeTracking tt WITH (NOLOCK)
            INNER JOIN dbo.WorkOrder wo WITH (NOLOCK) ON wo.wo_id = tt.wo_id
            WHERE tt.ttt_id = 3
              AND tt.tt_end IS NULL
              AND tt.u_id IS NOT NULL
              AND tt.tt_begin >= DATEADD(HOUR, -@MaxOpenCheckInHours, GETUTCDATE())
        )
        SELECT
            sr.sr_id                                AS SrId,
            sr.sr_requestnumber                     AS SrNumber,
            sr.sr_nte                               AS Nte,
            c.c_name                                AS CustomerName,
            l.l_location                            AS LocationName,
            sr.wo_id_primary                        AS PrimaryWoId,
            u.u_id                                  AS TechUserId,
            u.u_firstname                           AS TechFirstName,
            u.u_lastname                            AS TechLastName,
            COALESCE(NULLIF(LTRIM(RTRIM(u.u_phonemobile)), ''),
                     NULLIF(LTRIM(RTRIM(u.u_phonehome)), ''))   AS TechMobile,
            CASE WHEN NULLIF(LTRIM(RTRIM(u.u_phonemobile)), '') IS NOT NULL THEN 'EvoCell'
                 WHEN NULLIF(LTRIM(RTRIM(u.u_phonehome)), '')   IS NOT NULL THEN 'PersonalCell'
                 ELSE NULL END                      AS TechMobileSource,
            CAST(1 AS bit)                          AS TechIsCheckedIn,
            z.z_id                                  AS ZoneId,
            z.z_description                         AS ZoneName,
            z.z_email                               AS ZoneEmail
        FROM dbo.ServiceRequest sr WITH (NOLOCK)
        INNER JOIN CheckedIn ci ON ci.sr_id = sr.sr_id
        LEFT JOIN dbo.[user] u WITH (NOLOCK) ON u.u_id = ci.u_id
        LEFT JOIN dbo.Zone z WITH (NOLOCK) ON z.z_id = u.z_id
        LEFT JOIN dbo.xrefCompanyCallCenter xccc WITH (NOLOCK) ON xccc.xccc_id = sr.xccc_id
        LEFT JOIN dbo.Company c WITH (NOLOCK) ON c.c_id = xccc.c_id
        LEFT JOIN dbo.Location l WITH (NOLOCK) ON l.l_id = sr.l_id
        WHERE sr.s_id NOT IN (6, 9)
          AND sr.sr_nte IS NOT NULL
          AND sr.sr_nte > 0
        ORDER BY sr.sr_id, u.u_id;";

    public async Task<List<NteServiceRequestRow>> GetActiveServiceRequestsAsync(int maxOpenCheckInHours, CancellationToken ct)
    {
        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.QueryAsync<NteServiceRequestRow>(
            new CommandDefinition(ActiveServiceRequestsSql, new { MaxOpenCheckInHours = maxOpenCheckInHours }, cancellationToken: ct));
        return rows.AsList();
    }
}
