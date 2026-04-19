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

    public async Task<List<NteServiceRequestRow>> GetActiveServiceRequestsAsync(CancellationToken ct)
    {
        // Active = not closed/cancelled (s_id NOT IN (6, 9), matches existing usage in DataService.cs).
        // Tech is taken from the most recent xrefWorkOrderUser row for the primary work order
        // (matches the lookup pattern used in ServiceItemRepository for the "Tech" column).
        const string sql = @"
            SELECT
                sr.sr_id                  AS SrId,
                sr.sr_requestnumber       AS SrNumber,
                sr.sr_nte                 AS Nte,
                ISNULL(sr.sr_nteexcludequotedsi, 0) AS ExcludeQuotedSi,
                wo.wo_id                  AS WoId,
                wo.wo_nte                 AS WoNte,
                tech.u_id                 AS TechUserId,
                tech.u_firstname          AS TechFirstName,
                tech.u_lastname           AS TechLastName,
                tech.u_phonemobile        AS TechMobile,
                tech.u_email              AS TechEmail,
                z.z_id                    AS ZoneId,
                z.z_zone                  AS ZoneName,
                z.z_email                 AS ZoneEmail
            FROM dbo.ServiceRequest sr WITH (NOLOCK)
            LEFT JOIN dbo.WorkOrder wo WITH (NOLOCK)
                ON wo.wo_id = (
                    SELECT TOP 1 wo_inner.wo_id
                    FROM dbo.WorkOrder wo_inner WITH (NOLOCK)
                    WHERE wo_inner.sr_id = sr.sr_id
                    ORDER BY wo_inner.wo_id DESC
                )
            OUTER APPLY (
                SELECT TOP 1 u.u_id, u.u_firstname, u.u_lastname, u.u_phonemobile, u.u_email
                FROM dbo.xrefWorkOrderUser xwou WITH (NOLOCK)
                INNER JOIN dbo.[user] u WITH (NOLOCK) ON xwou.u_id = u.u_id
                WHERE xwou.wo_id = wo.wo_id
                ORDER BY xwou.xwou_id DESC
            ) tech
            LEFT JOIN dbo.Zone z WITH (NOLOCK) ON sr.z_id = z.z_id
            WHERE sr.s_id NOT IN (6, 9)
              AND sr.sr_nte IS NOT NULL
              AND sr.sr_nte > 0;";

        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.QueryAsync<NteServiceRequestRow>(
            new CommandDefinition(sql, cancellationToken: ct));
        return rows.AsList();
    }
}
