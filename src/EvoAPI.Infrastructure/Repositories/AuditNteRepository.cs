using System.Data.SqlClient;
using Dapper;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;

namespace EvoAPI.Infrastructure.Repositories;

public class AuditNteRepository : IAuditNteRepository
{
    private readonly string _connectionString;

    // SQL Server unique constraint violation error numbers.
    private const int SqlErrorUniqueConstraint = 2627;
    private const int SqlErrorUniqueIndex = 2601;

    public AuditNteRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
    }

    public async Task<HashSet<(string Key, string Channel)>> GetFiredAsync(int srId, CancellationToken ct)
    {
        const string sql = @"
            SELECT an_key AS [Key], an_channel AS Channel
            FROM dbo.AuditNTE
            WHERE sr_id = @SrId
              AND an_status IN ('Sending', 'Sent', 'Skipped');";

        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.QueryAsync<(string Key, string Channel)>(
            new CommandDefinition(sql, new { SrId = srId }, cancellationToken: ct));
        return new HashSet<(string Key, string Channel)>(rows);
    }

    public async Task<int?> TryInsertSendingAsync(AuditNteEntry entry, CancellationToken ct)
    {
        const string insertSql = @"
            INSERT INTO dbo.AuditNTE
                (an_channel, an_threshold, an_key, an_status,
                 sr_id, an_srnumber, u_id, an_techname, z_id, an_zonename,
                 an_intendedrecipient, an_recipient, an_overridden,
                 an_subject, an_body, an_description, an_detail,
                 an_error, an_errormessage, an_servername,
                 an_nte, an_spent, an_percent)
            VALUES
                (@Channel, @Threshold, @Key, @Status,
                 @SrId, @SrNumber, @TechUserId, @TechName, @ZoneId, @ZoneName,
                 @IntendedRecipient, @Recipient, @Overridden,
                 @Subject, @Body, @Description, @Detail,
                 CASE WHEN @Status = 'Failed' THEN 1 ELSE 0 END, @ErrorMessage, @ServerName,
                 @Nte, @Spent, @Percent);
            SELECT CAST(SCOPE_IDENTITY() AS INT);";

        // Same key already logged. Only a Failed row may be taken over for a retry;
        // Sending / Sent / Skipped / Reviewed mean it is owned elsewhere or must not resend.
        const string reclaimSql = @"
            UPDATE dbo.AuditNTE
            SET an_status            = @Status,
                an_attempts          = an_attempts + 1,
                an_modifieddatetime  = GETDATE(),
                an_error             = 0,
                an_intendedrecipient = @IntendedRecipient,
                an_recipient         = @Recipient,
                an_overridden        = @Overridden,
                an_subject           = @Subject,
                an_body              = @Body,
                an_description       = @Description,
                an_detail            = @Detail,
                an_servername        = @ServerName,
                an_nte               = @Nte,
                an_spent             = @Spent,
                an_percent           = @Percent
            OUTPUT INSERTED.an_id
            WHERE sr_id = @SrId
              AND an_channel = @Channel
              AND an_key = @Key
              AND an_status = 'Failed';";

        using var connection = new SqlConnection(_connectionString);
        try
        {
            return await connection.ExecuteScalarAsync<int?>(
                new CommandDefinition(insertSql, entry, cancellationToken: ct));
        }
        catch (SqlException ex) when (ex.Number == SqlErrorUniqueConstraint || ex.Number == SqlErrorUniqueIndex)
        {
            if (entry.Status != "Sending") return null;

            var reclaimed = await connection.QueryAsync<int>(
                new CommandDefinition(reclaimSql, entry, cancellationToken: ct));
            return reclaimed.FirstOrDefault() is var id && id > 0 ? id : null;
        }
    }

    public async Task UpdateStatusAsync(int anId, string status, string? error, CancellationToken ct)
    {
        const string sql = @"
            UPDATE dbo.AuditNTE
            SET an_status           = @Status,
                an_modifieddatetime = GETDATE(),
                an_sentdatetime     = CASE WHEN @Status = 'Sent' THEN GETDATE() ELSE an_sentdatetime END,
                an_error            = CASE WHEN @Status = 'Failed' THEN 1 ELSE 0 END,
                an_errormessage     = CASE WHEN @Error IS NULL THEN an_errormessage
                                           WHEN an_errormessage IS NULL THEN @Error
                                           ELSE @Error + ' || earlier: ' + an_errormessage END
            WHERE an_id = @AnId;";

        using var connection = new SqlConnection(_connectionString);
        await connection.ExecuteAsync(
            new CommandDefinition(sql, new { AnId = anId, Status = status, Error = error }, cancellationToken: ct));
    }

    public async Task<int> AbandonStaleSendingAsync(TimeSpan olderThan, CancellationToken ct)
    {
        const string sql = @"
            UPDATE dbo.AuditNTE
            SET an_status           = 'Failed',
                an_error            = 1,
                an_modifieddatetime = GETDATE(),
                an_errormessage     = 'Abandoned: still Sending after ' + CAST(@Minutes AS varchar(10)) + ' minutes (scan stopped mid-send); will retry'
                                      + CASE WHEN an_errormessage IS NULL THEN '' ELSE ' || earlier: ' + an_errormessage END
            WHERE an_status = 'Sending'
              AND ISNULL(an_modifieddatetime, an_insertdatetime) < DATEADD(MINUTE, -@Minutes, GETDATE());";

        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteAsync(
            new CommandDefinition(sql, new { Minutes = (int)Math.Ceiling(olderThan.TotalMinutes) }, cancellationToken: ct));
    }
}
