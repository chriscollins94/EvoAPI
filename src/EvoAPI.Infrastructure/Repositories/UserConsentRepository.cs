using System.Data.SqlClient;
using Dapper;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;

namespace EvoAPI.Infrastructure.Repositories;

public class UserConsentRepository : IUserConsentRepository
{
    private readonly string _connectionString;

    public UserConsentRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
    }

    public async Task<bool> HasOptedInAsync(int userId, string consentType, string disclosureVersion, CancellationToken ct)
    {
        const string sql = @"
            SELECT TOP 1 1
            FROM dbo.UserConsent
            WHERE u_id = @UserId
              AND uc_consenttype = @ConsentType
              AND uc_disclosureversion = @DisclosureVersion
              AND uc_status = 'OptedIn';";

        using var connection = new SqlConnection(_connectionString);
        var found = await connection.ExecuteScalarAsync<int?>(
            new CommandDefinition(sql, new { UserId = userId, ConsentType = consentType, DisclosureVersion = disclosureVersion }, cancellationToken: ct));
        return found.HasValue;
    }

    public async Task<int> InsertAsync(UserConsentRecord record, CancellationToken ct)
    {
        const string sql = @"
            INSERT INTO dbo.UserConsent
                (u_id, uc_consenttype, uc_contactvalue, uc_status,
                 uc_disclosureversion, uc_source, uc_ipaddress, uc_useragent)
            VALUES
                (@UserId, @ConsentType, @ContactValue, @Status,
                 @DisclosureVersion, @Source, @IpAddress, @UserAgent);
            SELECT CAST(SCOPE_IDENTITY() AS INT);";

        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(sql, record, cancellationToken: ct));
    }
}
