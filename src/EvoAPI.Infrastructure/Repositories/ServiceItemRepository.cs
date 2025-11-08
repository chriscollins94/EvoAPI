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
    public class ServiceItemRepository : IServiceItemRepository
    {
        private readonly string _connectionString;

        public ServiceItemRepository(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection") 
                ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
        }

        public async Task<List<ServiceItemDto>> GetServiceItemsAsync(string? filterText, string? filterStatus, int? filterTradeParent, int? filterServiceItemType, int limit = 10000)
        {
            using var connection = new SqlConnection(_connectionString);
            
            var sql = @"
                SELECT 
                    si.si_id,
                    si.o_id,
                    si.siu_id,
                    si.t_id,
                    si.sim_id,
                    si.sit_id,
                    si.si_insertdatetime,
                    si.si_modifieddatetime,
                    si.si_name,
                    si.si_description,
                    si.si_partnumber,
                    si.si_basecost,
                    si.si_markupenabled,
                    si.si_taxable,
                    si.si_quickbooksnumber,
                    si.si_active,
                    si.si_override,
                    si.si_keywords,
                    si.si_status,
                    si.si_datasource,
                    si.si_hoursperunit,
                    siu.siu_unit,
                    sim.sim_manufacturer,
                    t.t_trade,
                    sit.sit_serviceitemtype,
                    (SELECT COUNT(DISTINCT sr.sr_id) 
                     FROM dbo.xrefWorkOrderServiceItem xwosi 
                     INNER JOIN dbo.WorkOrder wo ON xwosi.wo_id = wo.wo_id
                     INNER JOIN dbo.ServiceRequest sr ON wo.sr_id = sr.sr_id
                     WHERE xwosi.si_id = si.si_id) AS UsageCount
                FROM serviceitem si
                LEFT JOIN serviceitemunit siu ON si.siu_id = siu.siu_id
                LEFT JOIN serviceitemmanufacturer sim ON si.sim_id = sim.sim_id
                LEFT JOIN trade t ON si.t_id = t.t_id
                LEFT JOIN serviceitemtype sit ON si.sit_id = sit.sit_id
                WHERE 1=1";

            var parameters = new DynamicParameters();

            // Filter by text (name, keywords, description, or part number)
            if (!string.IsNullOrWhiteSpace(filterText))
            {
                sql += @" AND (
                    si.si_name LIKE @FilterText 
                    OR si.si_keywords LIKE @FilterText 
                    OR si.si_description LIKE @FilterText
                    OR si.si_partnumber LIKE @FilterText
                )";
                parameters.Add("FilterText", $"%{filterText}%");
            }

            // Filter by status
            if (!string.IsNullOrWhiteSpace(filterStatus) && filterStatus != "All")
            {
                sql += " AND si.si_status = @FilterStatus";
                parameters.Add("FilterStatus", filterStatus);
            }

            // Filter by parent trade
            if (filterTradeParent.HasValue && filterTradeParent.Value > 0)
            {
                sql += " AND si.t_id = @FilterTradeParent";
                parameters.Add("FilterTradeParent", filterTradeParent.Value);
            }

            // Filter by service item type
            if (filterServiceItemType.HasValue && filterServiceItemType.Value > 0)
            {
                sql += " AND si.sit_id = @FilterServiceItemType";
                parameters.Add("FilterServiceItemType", filterServiceItemType.Value);
            }

            sql += " ORDER BY si.si_name";
            sql += $" OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY";

            var serviceItems = await connection.QueryAsync<ServiceItemDto>(sql, parameters);
            return serviceItems.ToList();
        }

        public async Task<ServiceItemDto?> GetServiceItemByIdAsync(int serviceItemId)
        {
            using var connection = new SqlConnection(_connectionString);
            
            var sql = @"
                SELECT 
                    si.si_id,
                    si.o_id,
                    si.siu_id,
                    si.t_id,
                    si.sim_id,
                    si.si_insertdatetime,
                    si.si_modifieddatetime,
                    si.si_name,
                    si.si_description,
                    si.si_partnumber,
                    si.si_basecost,
                    si.si_markupenabled,
                    si.si_taxable,
                    si.si_quickbooksnumber,
                    si.si_active,
                    si.si_override,
                    si.si_keywords,
                    si.si_status,
                    si.si_datasource,
                    si.si_hoursperunit,
                    siu.siu_unit,
                    sim.sim_manufacturer,
                    t.t_trade
                FROM serviceitem si
                LEFT JOIN serviceitemunit siu ON si.siu_id = siu.siu_id
                LEFT JOIN serviceitemmanufacturer sim ON si.sim_id = sim.sim_id
                LEFT JOIN trade t ON si.t_id = t.t_id
                WHERE si.si_id = @ServiceItemId";

            return await connection.QueryFirstOrDefaultAsync<ServiceItemDto>(sql, new { ServiceItemId = serviceItemId });
        }

        public async Task<int> CreateServiceItemAsync(CreateServiceItemRequest request)
        {
            using var connection = new SqlConnection(_connectionString);
            
            // Ensure si_hoursperunit is 0 instead of null
            if (!request.si_hoursperunit.HasValue || request.si_hoursperunit.Value == 0)
            {
                request.si_hoursperunit = 0;
            }
            
            var sql = @"
                INSERT INTO serviceitem (
                    o_id, siu_id, t_id, sim_id, sit_id,
                    si_insertdatetime,
                    si_name, si_description, si_keywords, si_partnumber,
                    si_basecost, si_markupenabled, si_taxable,
                    si_quickbooksnumber, si_active, si_override,
                    si_status, si_datasource, si_hoursperunit
                )
                VALUES (
                    @o_id, @siu_id, @t_id, @sim_id, @sit_id,
                    GETDATE(),
                    @si_name, @si_description, @si_keywords, @si_partnumber,
                    @si_basecost, @si_markupenabled, @si_taxable,
                    @si_quickbooksnumber, @si_active, @si_override,
                    @si_status, @si_datasource, @si_hoursperunit
                );
                SELECT CAST(SCOPE_IDENTITY() as int);";

            return await connection.ExecuteScalarAsync<int>(sql, request);
        }

        public async Task<bool> UpdateServiceItemAsync(UpdateServiceItemRequest request)
        {
            using var connection = new SqlConnection(_connectionString);
            
            // Ensure si_hoursperunit is 0 instead of null
            if (!request.si_hoursperunit.HasValue || request.si_hoursperunit.Value == 0)
            {
                request.si_hoursperunit = 0;
            }
            
            var sql = @"
                UPDATE serviceitem
                SET 
                    o_id = @o_id,
                    siu_id = @siu_id,
                    t_id = @t_id,
                    sim_id = @sim_id,
                    sit_id = @sit_id,
                    si_modifieddatetime = GETDATE(),
                    si_name = @si_name,
                    si_description = @si_description,
                    si_keywords = @si_keywords,
                    si_partnumber = @si_partnumber,
                    si_basecost = @si_basecost,
                    si_markupenabled = @si_markupenabled,
                    si_taxable = @si_taxable,
                    si_quickbooksnumber = @si_quickbooksnumber,
                    si_active = @si_active,
                    si_override = @si_override,
                    si_status = @si_status,
                    si_datasource = @si_datasource,
                    si_hoursperunit = @si_hoursperunit
                WHERE si_id = @si_id";

            var rowsAffected = await connection.ExecuteAsync(sql, request);
            return rowsAffected > 0;
        }

        public async Task<List<ServiceItemUnitDto>> GetServiceItemUnitsAsync()
        {
            using var connection = new SqlConnection(_connectionString);
            
            var sql = @"
                SELECT 
                    siu_id,
                    siu_insertdatetime,
                    siu_modifieddatetime,
                    siu_unit,
                    siu_description,
                    siu_order
                FROM serviceitemunit
                ORDER BY siu_order, siu_unit";

            var units = await connection.QueryAsync<ServiceItemUnitDto>(sql);
            return units.ToList();
        }

        public async Task<List<ServiceItemManufacturerDto>> GetServiceItemManufacturersAsync()
        {
            using var connection = new SqlConnection(_connectionString);
            
            var sql = @"
                SELECT 
                    sim_id,
                    sim_insertdatetime,
                    sim_modifieddatetime,
                    sim_manufacturer
                FROM serviceitemmanufacturer
                ORDER BY sim_manufacturer";

            var manufacturers = await connection.QueryAsync<ServiceItemManufacturerDto>(sql);
            return manufacturers.ToList();
        }

        public async Task<List<TradeDto>> GetTradesAsync(bool parentOnlyFilter = false)
        {
            using var connection = new SqlConnection(_connectionString);
            
            var sql = @"
                SELECT 
                    t_id,
                    o_id,
                    t_id_parent,
                    t_insertdatetime,
                    t_modifieddatetime,
                    t_trade,
                    t_description,
                    t_nte,
                    t_parentonly,
                    t_highvolume
                FROM trade
                WHERE 1=1";

            if (parentOnlyFilter)
            {
                sql += " AND t_parentonly = 1";
            }

            sql += " ORDER BY t_trade";

            var trades = await connection.QueryAsync<TradeDto>(sql);
            return trades.ToList();
        }

        public async Task<List<ServiceItemTypeDto>> GetServiceItemTypesAsync()
        {
            using var connection = new SqlConnection(_connectionString);
            
            var sql = @"
                SELECT 
                    sit_id,
                    sit_insertdatetime,
                    sit_modifieddatetime,
                    sit_serviceitemtype
                FROM serviceitemtype
                ORDER BY sit_serviceitemtype";

            var types = await connection.QueryAsync<ServiceItemTypeDto>(sql);
            return types.ToList();
        }

        public async Task<List<ServiceItemUsageDto>> GetServiceItemUsageAsync(int serviceItemId, DateTime? startDate, DateTime? endDate)
        {
            using var connection = new SqlConnection(_connectionString);
            
            var sql = @"
                SELECT 
                    sr.sr_id AS ServiceRequestId,
                    sr.sr_requestnumber AS RequestNumber,
                    sr.sr_summary AS Summary,
                    sr.sr_insertdatetime AS InsertDateTime,
                    sr.sr_datedue AS DateDue,
                    c.c_name AS CompanyName,
                    l.l_location AS LocationName,
                    SUM(xwosi.xwosi_quantity) AS TotalQuantity,
                    AVG(xwosi.xwosi_basecost) AS AverageCost,
                    SUM(xwosi.xwosi_quantity * xwosi.xwosi_basecost) AS TotalCost,
                    s.s_status AS Status,
                    s.s_color AS StatusColor
                FROM dbo.xrefWorkOrderServiceItem xwosi
                INNER JOIN dbo.WorkOrder wo ON xwosi.wo_id = wo.wo_id
                INNER JOIN dbo.ServiceRequest sr ON wo.sr_id = sr.sr_id
                LEFT JOIN dbo.xrefCompanyCallCenter xccc ON sr.xccc_id = xccc.xccc_id
                LEFT JOIN dbo.company c ON xccc.c_id = c.c_id
                LEFT JOIN dbo.location l ON sr.l_id = l.l_id
                LEFT JOIN dbo.status s ON sr.s_id = s.s_id
                WHERE xwosi.si_id = @ServiceItemId";

            if (startDate.HasValue)
            {
                sql += " AND sr.sr_insertdatetime >= @StartDate";
            }

            if (endDate.HasValue)
            {
                sql += " AND sr.sr_insertdatetime <= @EndDate";
            }

            sql += @"
                GROUP BY 
                    sr.sr_id,
                    sr.sr_requestnumber,
                    sr.sr_summary,
                    sr.sr_insertdatetime,
                    sr.sr_datedue,
                    c.c_name,
                    l.l_location,
                    s.s_status,
                    s.s_color
                ORDER BY sr.sr_insertdatetime DESC";

            var parameters = new
            {
                ServiceItemId = serviceItemId,
                StartDate = startDate,
                EndDate = endDate
            };

            var usage = await connection.QueryAsync<ServiceItemUsageDto>(sql, parameters);
            return usage.ToList();
        }
    }
}
