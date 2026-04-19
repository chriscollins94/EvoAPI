using System.Data.SqlClient;
using Dapper;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;

namespace EvoAPI.Infrastructure.Repositories;

public class NotificationLogRepository : INotificationLogRepository
{
    private readonly string _connectionString;

    // SQL Server unique constraint violation error numbers.
    private const int SqlErrorUniqueConstraint = 2627;
    private const int SqlErrorUniqueIndex = 2601;

    public NotificationLogRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
    }

    public async Task<HashSet<(string Key, string Channel)>> GetFiredAsync(
        string type, string entityType, int entityId, CancellationToken ct)
    {
        const string sql = @"
            SELECT nl_key AS [Key], nl_channel AS Channel
            FROM dbo.NotificationLog
            WHERE nl_type = @Type
              AND nl_entity_type = @EntityType
              AND nl_entity_id = @EntityId
              AND nl_send_status IN ('Sending', 'Sent', 'Skipped');";

        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.QueryAsync<(string Key, string Channel)>(
            new CommandDefinition(sql, new { Type = type, EntityType = entityType, EntityId = entityId }, cancellationToken: ct));
        return new HashSet<(string Key, string Channel)>(rows);
    }

    public async Task<int?> TryInsertSendingAsync(NotificationLogEntry entry, CancellationToken ct)
    {
        const string sql = @"
            INSERT INTO dbo.NotificationLog
                (nl_type, nl_key, nl_entity_type, nl_entity_id, nl_channel,
                 nl_recipient, nl_subject, nl_body, nl_send_status, nl_send_error, nl_metadata)
            VALUES
                (@Type, @Key, @EntityType, @EntityId, @Channel,
                 @Recipient, @Subject, @Body, @Status, @Error, @Metadata);
            SELECT CAST(SCOPE_IDENTITY() AS INT);";

        try
        {
            using var connection = new SqlConnection(_connectionString);
            return await connection.ExecuteScalarAsync<int?>(
                new CommandDefinition(sql, entry, cancellationToken: ct));
        }
        catch (SqlException ex) when (ex.Number == SqlErrorUniqueConstraint || ex.Number == SqlErrorUniqueIndex)
        {
            // Another scan instance already logged this (type, key, entity, channel). Skip.
            return null;
        }
    }

    public async Task UpdateStatusAsync(int nlId, string status, string? error, CancellationToken ct)
    {
        const string sql = @"
            UPDATE dbo.NotificationLog
            SET nl_send_status = @Status,
                nl_send_error  = @Error
            WHERE nl_id = @NlId;";

        using var connection = new SqlConnection(_connectionString);
        await connection.ExecuteAsync(
            new CommandDefinition(sql, new { NlId = nlId, Status = status, Error = error }, cancellationToken: ct));
    }
}
