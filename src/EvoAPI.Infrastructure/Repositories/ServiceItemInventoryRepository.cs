using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;

namespace EvoAPI.Infrastructure.Repositories
{
    public class ServiceItemInventoryRepository : IServiceItemInventoryRepository
    {
        private readonly string _connectionString;

        public ServiceItemInventoryRepository(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection")
                ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
        }

        // The legacy query used comma joins mixed with a LEFT JOIN, which silently
        // turned the manufacturer join inner. Explicit joins throughout.
        private const string BaseFrom = @"
            FROM dbo.ServiceItemInventory sii
            INNER JOIN dbo.ServiceItem si ON sii.si_id = si.si_id
            INNER JOIN dbo.ServiceItemFacility sif ON sii.sif_id = sif.sif_id
            INNER JOIN dbo.ServiceItemRack sir ON sii.sir_id = sir.sir_id
            LEFT JOIN dbo.ServiceItemManufacturer sim ON si.sim_id = sim.sim_id
            LEFT JOIN dbo.ServiceItemUnit siu ON si.siu_id = siu.siu_id";

        private const string SelectColumns = @"
            SELECT
                sii.sii_id,
                sii.si_id,
                sii.sif_id,
                sii.sir_id,
                si.sim_id,
                si.si_name,
                si.si_description,
                si.si_partnumber,
                si.si_keywords,
                si.si_basecost,
                si.si_datasource,
                si.si_hoursperunit,
                si.si_status,
                sim.sim_manufacturer,
                siu.siu_unit,
                sif.sif_facility,
                sir.sir_rack,
                sii.sii_shelf,
                sii.sii_bin,
                sii.sii_countavailable,
                sii.sii_countallocated,
                sii.sii_insertdatetime,
                sii.sii_modifieddatetime";

        // Whitelist: the sort field arrives from the client and is interpolated into SQL.
        private static readonly Dictionary<string, string> SortColumns = new(StringComparer.OrdinalIgnoreCase)
        {
            ["si_name"] = "si.si_name",
            ["sim_manufacturer"] = "sim.sim_manufacturer",
            ["sif_facility"] = "sif.sif_facility",
            ["sir_rack"] = "sir.sir_rack",
            ["sii_shelf"] = "sii.sii_shelf",
            ["sii_bin"] = "sii.sii_bin",
            ["si_basecost"] = "si.si_basecost",
            ["sii_countavailable"] = "sii.sii_countavailable",
            ["sii_countallocated"] = "sii.sii_countallocated",
            ["valueAvailable"] = "(ISNULL(si.si_basecost, 0) * sii.sii_countavailable)",
            ["valueAllocated"] = "(ISNULL(si.si_basecost, 0) * sii.sii_countallocated)",
            ["sii_modifieddatetime"] = "sii.sii_modifieddatetime"
        };

        // The legacy filter pasted user input straight into LIKE, so a stray % or _
        // quietly widened the search and a [ could break it outright.
        private static string EscapeLike(string value) => value
            .Replace("\\", "\\\\")
            .Replace("%", "\\%")
            .Replace("_", "\\_")
            .Replace("[", "\\[");

        private static string BuildWhere(string? filterText, int? facilityId, int? rackId, bool lowStockOnly, DynamicParameters parameters)
        {
            var where = " WHERE 1 = 1";

            if (!string.IsNullOrWhiteSpace(filterText))
            {
                where += @" AND (
                    si.si_name LIKE @FilterText ESCAPE '\'
                    OR si.si_description LIKE @FilterText ESCAPE '\'
                    OR si.si_keywords LIKE @FilterText ESCAPE '\'
                    OR si.si_partnumber LIKE @FilterText ESCAPE '\'
                    OR sif.sif_facility LIKE @FilterText ESCAPE '\'
                    OR sim.sim_manufacturer LIKE @FilterText ESCAPE '\'
                )";
                parameters.Add("FilterText", $"%{EscapeLike(filterText.Trim())}%");
            }

            if (facilityId.HasValue && facilityId.Value > 0)
            {
                where += " AND sii.sif_id = @FacilityId";
                parameters.Add("FacilityId", facilityId.Value);
            }

            if (rackId.HasValue && rackId.Value > 0)
            {
                where += " AND sii.sir_id = @RackId";
                parameters.Add("RackId", rackId.Value);
            }

            if (lowStockOnly)
            {
                where += " AND sii.sii_countavailable <= @LowStockThreshold";
            }

            return where;
        }

        public async Task<ServiceItemInventoryListDto> GetInventoryAsync(
            string? filterText,
            int? facilityId,
            int? rackId,
            bool lowStockOnly,
            int lowStockThreshold,
            string? sortField,
            string? sortDirection,
            int limit)
        {
            if (limit <= 0) limit = 500;
            if (limit > 5000) limit = 5000;

            using var connection = new SqlConnection(_connectionString);

            var parameters = new DynamicParameters();
            parameters.Add("LowStockThreshold", lowStockThreshold);
            var where = BuildWhere(filterText, facilityId, rackId, lowStockOnly, parameters);

            var orderColumn = !string.IsNullOrWhiteSpace(sortField) && SortColumns.TryGetValue(sortField, out var mapped)
                ? mapped
                : "si.si_name";
            var direction = string.Equals(sortDirection, "desc", StringComparison.OrdinalIgnoreCase) ? "DESC" : "ASC";

            // sii_id tiebreaker keeps paging/limit deterministic - the legacy TOP 250
            // had no ORDER BY at all, so which 250 rows you got was up to the optimizer.
            var listSql = $@"{SelectColumns}{BaseFrom}{where}
                ORDER BY {orderColumn} {direction}, sii.sii_id
                OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY";

            var summarySql = $@"
                SELECT
                    COUNT(*)                                                          AS [RowCount],
                    COUNT(DISTINCT sii.si_id)                                         AS DistinctItemCount,
                    ISNULL(SUM(sii.sii_countavailable), 0)                            AS TotalAvailable,
                    ISNULL(SUM(sii.sii_countallocated), 0)                            AS TotalAllocated,
                    ISNULL(SUM(ISNULL(si.si_basecost, 0) * sii.sii_countavailable), 0) AS TotalValueAvailable,
                    ISNULL(SUM(ISNULL(si.si_basecost, 0) * sii.sii_countallocated), 0) AS TotalValueAllocated,
                    ISNULL(SUM(CASE WHEN sii.sii_countavailable <= 0 THEN 1 ELSE 0 END), 0) AS OutOfStockCount
                {BaseFrom}{where}";

            var items = (await connection.QueryAsync<ServiceItemInventoryDto>(listSql, parameters)).ToList();
            var summary = await connection.QueryFirstAsync<ServiceItemInventorySummaryDto>(summarySql, parameters);

            return new ServiceItemInventoryListDto
            {
                Items = items,
                Summary = summary,
                Truncated = summary.RowCount > items.Count
            };
        }

        public async Task<ServiceItemInventoryDto?> GetByIdAsync(int inventoryId)
        {
            using var connection = new SqlConnection(_connectionString);
            var sql = $"{SelectColumns}{BaseFrom} WHERE sii.sii_id = @InventoryId";
            return await connection.QueryFirstOrDefaultAsync<ServiceItemInventoryDto>(sql, new { InventoryId = inventoryId });
        }

        private static async Task<ServiceItemInventoryDto?> GetByIdAsync(SqlConnection connection, SqlTransaction? transaction, int inventoryId)
        {
            var sql = $"{SelectColumns}{BaseFrom} WHERE sii.sii_id = @InventoryId";
            return await connection.QueryFirstOrDefaultAsync<ServiceItemInventoryDto>(sql, new { InventoryId = inventoryId }, transaction);
        }

        public async Task<(int? Id, string? Error)> CreateAsync(CreateServiceItemInventoryRequest request, int userId, string username)
        {
            if (request.sii_countavailable < 0 || request.sii_countallocated < 0)
                return (null, "Counts cannot be negative");

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var transaction = connection.BeginTransaction();

            try
            {
                if (await LocationTakenAsync(connection, transaction, request.si_id, request.sif_id, request.sir_id, request.sii_shelf, request.sii_bin, null))
                    return (null, "This service item already has an inventory record at that facility, rack, shelf and bin. Edit that record instead.");

                const string sql = @"
                    INSERT INTO dbo.ServiceItemInventory
                        (si_id, sif_id, sir_id, sii_shelf, sii_bin, sii_countavailable, sii_countallocated, sii_insertdatetime)
                    VALUES
                        (@si_id, @sif_id, @sir_id, @sii_shelf, @sii_bin, @sii_countavailable, @sii_countallocated, GETDATE());
                    SELECT CAST(SCOPE_IDENTITY() AS INT);";

                var newId = await connection.ExecuteScalarAsync<int>(sql, request, transaction);

                await LogTransactionAsync(connection, transaction, new ServiceItemInventoryTransactionDto
                {
                    sii_id = newId,
                    si_id = request.si_id,
                    sif_id = request.sif_id,
                    sir_id = request.sir_id,
                    siit_type = "Create",
                    siit_availabledelta = request.sii_countavailable,
                    siit_allocateddelta = request.sii_countallocated,
                    siit_availableafter = request.sii_countavailable,
                    siit_allocatedafter = request.sii_countallocated,
                    siit_reason = "Inventory record created",
                    u_id = userId,
                    siit_username = username
                });

                transaction.Commit();
                return (newId, null);
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }

        public async Task<(bool Success, string? Error)> UpdateAsync(UpdateServiceItemInventoryRequest request, int userId, string username)
        {
            if (request.sii_countavailable < 0 || request.sii_countallocated < 0)
                return (false, "Counts cannot be negative");

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var transaction = connection.BeginTransaction();

            try
            {
                var existing = await GetByIdAsync(connection, transaction, request.sii_id);
                if (existing == null)
                    return (false, "Inventory record not found");

                if (await LocationTakenAsync(connection, transaction, existing.si_id, request.sif_id, request.sir_id, request.sii_shelf, request.sii_bin, request.sii_id))
                    return (false, "Another inventory record for this service item already sits at that facility, rack, shelf and bin.");

                // sii_modifieddatetime existed all along; the legacy update never wrote it.
                const string sql = @"
                    UPDATE dbo.ServiceItemInventory SET
                        sif_id = @sif_id,
                        sir_id = @sir_id,
                        sii_shelf = @sii_shelf,
                        sii_bin = @sii_bin,
                        sii_countavailable = @sii_countavailable,
                        sii_countallocated = @sii_countallocated,
                        sii_modifieddatetime = GETDATE()
                    WHERE sii_id = @sii_id";

                var rows = await connection.ExecuteAsync(sql, new
                {
                    request.sii_id,
                    request.sif_id,
                    request.sir_id,
                    request.sii_shelf,
                    request.sii_bin,
                    request.sii_countavailable,
                    request.sii_countallocated
                }, transaction);

                if (rows == 0)
                    return (false, "Inventory record not found");

                var availableDelta = request.sii_countavailable - existing.sii_countavailable;
                var allocatedDelta = request.sii_countallocated - existing.sii_countallocated;
                var moved = existing.sif_id != request.sif_id || existing.sir_id != request.sir_id
                            || existing.sii_shelf != request.sii_shelf || existing.sii_bin != request.sii_bin;

                // Skip the history row when nothing actually changed - an edit that only
                // re-saves the same values is noise, not a movement.
                if (availableDelta != 0 || allocatedDelta != 0 || moved)
                {
                    await LogTransactionAsync(connection, transaction, new ServiceItemInventoryTransactionDto
                    {
                        sii_id = request.sii_id,
                        si_id = existing.si_id,
                        sif_id = request.sif_id,
                        sir_id = request.sir_id,
                        siit_type = "Update",
                        siit_availabledelta = availableDelta,
                        siit_allocateddelta = allocatedDelta,
                        siit_availableafter = request.sii_countavailable,
                        siit_allocatedafter = request.sii_countallocated,
                        // The reason is optional, so fall back to describing what changed.
                        siit_reason = string.IsNullOrWhiteSpace(request.Reason)
                            ? (moved ? "Edited (location changed)" : "Edited")
                            : request.Reason,
                        u_id = userId,
                        siit_username = username
                    });
                }

                transaction.Commit();
                return (true, null);
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }

        public async Task<bool> DeleteAsync(int inventoryId, int userId, string username, string? reason)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var transaction = connection.BeginTransaction();

            try
            {
                var existing = await GetByIdAsync(connection, transaction, inventoryId);
                if (existing == null)
                    return false;

                // Logged before the delete, while sii_id still resolves. The denormalized
                // si_id/sif_id/sir_id are what keep the history readable afterwards.
                await LogTransactionAsync(connection, transaction, new ServiceItemInventoryTransactionDto
                {
                    sii_id = inventoryId,
                    si_id = existing.si_id,
                    sif_id = existing.sif_id,
                    sir_id = existing.sir_id,
                    siit_type = "Delete",
                    siit_availabledelta = -existing.sii_countavailable,
                    siit_allocateddelta = -existing.sii_countallocated,
                    siit_availableafter = null,
                    siit_allocatedafter = null,
                    siit_reason = string.IsNullOrWhiteSpace(reason) ? "Inventory record deleted" : reason,
                    u_id = userId,
                    siit_username = username
                });

                // Detach the history explicitly rather than relying on the FK's
                // ON DELETE SET NULL - the migration script skips that FK when the
                // legacy table has no declared primary key on sii_id.
                await connection.ExecuteAsync(
                    "UPDATE dbo.ServiceItemInventoryTransaction SET sii_id = NULL WHERE sii_id = @InventoryId",
                    new { InventoryId = inventoryId }, transaction);

                var rows = await connection.ExecuteAsync(
                    "DELETE FROM dbo.ServiceItemInventory WHERE sii_id = @InventoryId",
                    new { InventoryId = inventoryId }, transaction);

                transaction.Commit();
                return rows > 0;
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }

        public async Task<(InventoryDecrementResultDto? Result, string? Error)> DecrementAsync(InventoryDecrementRequest request, int userId, string username)
        {
            if (request.Quantity <= 0)
                return (null, "Quantity must be greater than zero");
            if (request.si_id <= 0 && !request.sii_id.HasValue)
                return (null, "A service item or inventory record is required");

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var transaction = connection.BeginTransaction();

            try
            {
                // The legacy version applied the full quantity to EVERY row sharing the
                // si_id, so an item stocked in two facilities was decremented twice.
                // Draw the requested total from the fullest matching rows instead.
                var candidateSql = @"
                    SELECT sii_id, si_id, sif_id, sir_id, sii_countavailable, sii_countallocated
                    FROM dbo.ServiceItemInventory WITH (UPDLOCK, ROWLOCK)
                    WHERE 1 = 1";
                var candidateParams = new DynamicParameters();

                if (request.sii_id.HasValue)
                {
                    candidateSql += " AND sii_id = @InventoryId";
                    candidateParams.Add("InventoryId", request.sii_id.Value);
                }
                else
                {
                    candidateSql += " AND si_id = @ServiceItemId";
                    candidateParams.Add("ServiceItemId", request.si_id);

                    if (request.sif_id.HasValue && request.sif_id.Value > 0)
                    {
                        candidateSql += " AND sif_id = @FacilityId";
                        candidateParams.Add("FacilityId", request.sif_id.Value);
                    }
                }

                candidateSql += " ORDER BY sii_countavailable DESC, sii_id";

                var candidates = (await connection.QueryAsync(candidateSql, candidateParams, transaction)).ToList();
                if (candidates.Count == 0)
                {
                    transaction.Rollback();
                    return (null, "No inventory record found for that service item");
                }

                var remaining = request.Quantity;
                var touchedIds = new List<int>();

                foreach (var row in candidates)
                {
                    if (remaining <= 0) break;

                    int available = row.sii_countavailable;
                    if (available <= 0) continue;

                    var take = Math.Min(remaining, available);
                    int siiId = row.sii_id;

                    await connection.ExecuteAsync(@"
                        UPDATE dbo.ServiceItemInventory SET
                            sii_countavailable = sii_countavailable - @Take,
                            sii_countallocated = sii_countallocated + @Take,
                            sii_modifieddatetime = GETDATE()
                        WHERE sii_id = @InventoryId",
                        new { Take = take, InventoryId = siiId }, transaction);

                    await LogTransactionAsync(connection, transaction, new ServiceItemInventoryTransactionDto
                    {
                        sii_id = siiId,
                        si_id = (int)row.si_id,
                        sif_id = (int)row.sif_id,
                        sir_id = (int)row.sir_id,
                        siit_type = "Decrement",
                        siit_availabledelta = -take,
                        siit_allocateddelta = take,
                        siit_availableafter = available - take,
                        siit_allocatedafter = (int)row.sii_countallocated + take,
                        siit_reason = string.IsNullOrWhiteSpace(request.Reason) ? "Allocated to work order" : request.Reason,
                        u_id = userId,
                        siit_username = username
                    });

                    remaining -= take;
                    touchedIds.Add(siiId);
                }

                transaction.Commit();

                var affected = new List<ServiceItemInventoryDto>();
                foreach (var id in touchedIds)
                {
                    var dto = await GetByIdAsync(id);
                    if (dto != null) affected.Add(dto);
                }

                // Shortfall is reported rather than swallowed - the legacy endpoint
                // returned plain OK even when it could only fill part of the request.
                return (new InventoryDecrementResultDto
                {
                    Requested = request.Quantity,
                    Decremented = request.Quantity - remaining,
                    Shortfall = remaining,
                    AffectedRows = affected
                }, null);
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }

        public async Task<List<ServiceItemInventoryTransactionDto>> GetTransactionsAsync(int? inventoryId, int? serviceItemId, int limit)
        {
            if (limit <= 0) limit = 200;
            if (limit > 2000) limit = 2000;

            using var connection = new SqlConnection(_connectionString);

            var sql = $@"
                SELECT
                    t.siit_id, t.sii_id, t.si_id, t.sif_id, t.sir_id, t.siit_type,
                    t.siit_availabledelta, t.siit_allocateddelta,
                    t.siit_availableafter, t.siit_allocatedafter,
                    t.siit_reason, t.u_id, t.siit_username, t.siit_insertdatetime,
                    si.si_name, sif.sif_facility, sir.sir_rack
                FROM dbo.ServiceItemInventoryTransaction t
                LEFT JOIN dbo.ServiceItem si ON t.si_id = si.si_id
                LEFT JOIN dbo.ServiceItemFacility sif ON t.sif_id = sif.sif_id
                LEFT JOIN dbo.ServiceItemRack sir ON t.sir_id = sir.sir_id
                WHERE 1 = 1";

            var parameters = new DynamicParameters();

            if (inventoryId.HasValue && inventoryId.Value > 0)
            {
                sql += " AND t.sii_id = @InventoryId";
                parameters.Add("InventoryId", inventoryId.Value);
            }

            if (serviceItemId.HasValue && serviceItemId.Value > 0)
            {
                sql += " AND t.si_id = @ServiceItemId";
                parameters.Add("ServiceItemId", serviceItemId.Value);
            }

            sql += $" ORDER BY t.siit_insertdatetime DESC, t.siit_id DESC OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY";

            return (await connection.QueryAsync<ServiceItemInventoryTransactionDto>(sql, parameters)).ToList();
        }

        private static async Task<bool> LocationTakenAsync(
            SqlConnection connection, SqlTransaction transaction,
            int serviceItemId, int facilityId, int rackId, int? shelf, int? bin, int? excludeInventoryId)
        {
            // NULL shelf/bin has to compare equal here, matching the unique index.
            const string sql = @"
                SELECT COUNT(*)
                FROM dbo.ServiceItemInventory
                WHERE si_id = @ServiceItemId
                  AND sif_id = @FacilityId
                  AND sir_id = @RackId
                  AND ISNULL(sii_shelf, -1) = ISNULL(@Shelf, -1)
                  AND ISNULL(sii_bin, -1) = ISNULL(@Bin, -1)
                  AND (@ExcludeId IS NULL OR sii_id <> @ExcludeId)";

            var count = await connection.ExecuteScalarAsync<int>(sql, new
            {
                ServiceItemId = serviceItemId,
                FacilityId = facilityId,
                RackId = rackId,
                Shelf = shelf,
                Bin = bin,
                ExcludeId = excludeInventoryId
            }, transaction);

            return count > 0;
        }

        private static async Task LogTransactionAsync(SqlConnection connection, SqlTransaction transaction, ServiceItemInventoryTransactionDto entry)
        {
            const string sql = @"
                INSERT INTO dbo.ServiceItemInventoryTransaction
                    (sii_id, si_id, sif_id, sir_id, siit_type,
                     siit_availabledelta, siit_allocateddelta,
                     siit_availableafter, siit_allocatedafter,
                     siit_reason, u_id, siit_username, siit_insertdatetime)
                VALUES
                    (@sii_id, @si_id, @sif_id, @sir_id, @siit_type,
                     @siit_availabledelta, @siit_allocateddelta,
                     @siit_availableafter, @siit_allocatedafter,
                     @siit_reason, @u_id, @siit_username, GETDATE())";

            await connection.ExecuteAsync(sql, new
            {
                entry.sii_id,
                entry.si_id,
                entry.sif_id,
                entry.sir_id,
                entry.siit_type,
                entry.siit_availabledelta,
                entry.siit_allocateddelta,
                entry.siit_availableafter,
                entry.siit_allocatedafter,
                siit_reason = string.IsNullOrWhiteSpace(entry.siit_reason) ? null : entry.siit_reason!.Trim(),
                u_id = entry.u_id > 0 ? entry.u_id : null,
                entry.siit_username
            }, transaction);
        }
    }
}
