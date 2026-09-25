using System.Data.SqlClient;
using System.Text.Json;
using Dapper;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;

namespace EvoAPI.Infrastructure.Repositories;

/// <summary>
/// Data access for the PM ticket / visit layer (build slice 3): ServiceType, the PM sub-trades a company can enter a
/// Preventative ticket under, first-time detection, and the PMVisit + PMVisitUnit snapshot created at ticket entry.
/// Tables come from sql/migrations/2026-09-24_create_pm_ticket_tables.sql.
/// </summary>
public class PmVisitRepository : IPmVisitRepository
{
    private readonly string _connectionString;

    // Same recognition rule as FormRepository: PM trades are known by name.
    private const string PmTradeNamePredicate = "(t.t_trade LIKE 'PM %' OR t.t_trade LIKE 'PM-%' OR t.t_trade LIKE '% PM' OR t.t_trade LIKE '% PM %')";

    private const string VisitSelect = @"
        SELECT
            v.pmv_id                        AS PmvId,
            v.sr_id                         AS SrId,
            v.fr_id                         AS FrId,
            v.ft_id                         AS FtId,
            v.ft_version                    AS FtVersion,
            v.pms_id                        AS PmsId,
            v.pmv_visittype                 AS VisitType,
            v.pmv_firsttime                 AS FirstTime,
            v.pmv_firsttimesource           AS FirstTimeSource,
            v.pmv_billingmode               AS BillingMode,
            v.pmv_nteguideline              AS NteGuideline,
            v.pmv_firsttimerule             AS FirstTimeRule,
            v.pmv_contracttotal             AS ContractTotal,
            v.pmv_pricingjson               AS PricingJson,
            CAST(v.pmv_coolingsetpoint AS INT) AS CoolingSetpoint,
            CAST(v.pmv_heatingsetpoint AS INT) AS HeatingSetpoint,
            v.pmv_thermostatschedule        AS ThermostatSchedule,
            v.pmv_thermostatschedulenote    AS ThermostatScheduleNote,
            v.pmv_thermostatlock            AS ThermostatLock,
            v.pmv_antialgae                 AS AntiAlgae,
            v.pmv_phototimestamp            AS PhotoTimestamp,
            v.pmv_managerseesoldfilters     AS ManagerSeesOldFilters,
            v.pmv_pricingdiscussallowed     AS PricingDiscussAllowed,
            v.pmv_immediatequoterequired    AS ImmediateQuoteRequired,
            v.pmv_immediatecallifincomplete AS ImmediateCallIfIncomplete,
            v.pmv_submissiondeadline        AS SubmissionDeadline,
            v.pmv_submissiondeadlinenote    AS SubmissionDeadlineNote,
            v.pmv_closeoutdocs              AS CloseoutDocs,
            v.pmv_submissiondestination     AS SubmissionDestination,
            v.pmv_ivrrequired               AS IvrRequired,
            v.pmv_customerform              AS CustomerForm,
            v.pmv_customerformurl           AS CustomerFormUrl,
            v.att_id_customerform           AS AttIdCustomerForm,
            v.pmv_customerformnote          AS CustomerFormNote,
            v.pmv_paramsconfirmed_u_id      AS ParamsConfirmedUId,
            v.pmv_paramsconfirmeddatetime   AS ParamsConfirmedDateTime,
            v.pmv_status                    AS Status,
            v.pmv_insertdatetime            AS InsertDateTime
        FROM dbo.PMVisit v";

    private const string UnitSelect = @"
        SELECT
            u.pmvu_id               AS PmvuId,
            u.pmv_id                AS PmvId,
            u.pmvu_sequence         AS Sequence,
            u.pmvu_source           AS Source,
            u.as_id                 AS AsId,
            u.asc_id                AS AscId,
            LTRIM(RTRIM(ac.asc_category)) AS EquipmentType,
            u.pmvu_capacitytons     AS CapacityTons,
            u.pmvu_label            AS Label,
            u.pmvu_price            AS Price,
            u.pmvu_tierlabel        AS TierLabel,
            u.pmvu_status           AS Status,
            u.pmvu_assetconfirmed   AS AssetConfirmed
        FROM dbo.PMVisitUnit u
        LEFT JOIN dbo.AssetCategory ac ON ac.asc_id = u.asc_id";

    public PmVisitRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
    }

    #region lookups

    public async Task<List<ServiceTypeDto>> GetServiceTypesAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<ServiceTypeDto>(@"
            SELECT svt_id AS SvtId, svt_servicetype AS ServiceType, svt_code AS Code, svt_description AS Description, svt_order AS [Order], svt_active AS Active
            FROM dbo.ServiceType WHERE svt_active = 1 ORDER BY svt_order, svt_id")).ToList();
    }

    public async Task<int?> GetServiceTypeIdAsync(string code)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int?>("SELECT svt_id FROM dbo.ServiceType WHERE svt_code = @Code", new { Code = code });
    }

    public async Task<List<PmTicketTradeDto>> GetTicketTradesAsync(int xcccId)
    {
        var sql = $@"
            SELECT
                t.t_id              AS TId,
                t.t_trade           AS Trade,
                tp.t_id             AS ParentTId,
                tp.t_trade          AS ParentTrade,
                lr.lr_id            AS LrId,
                r.fr_id             AS FrId,
                s.pms_id            AS PmsId,
                s.pms_name          AS SeasonName,
                s.pms_visittype     AS VisitType,
                s.pms_startmonthday AS StartMonthDay,
                s.pms_endmonthday   AS EndMonthDay
            FROM dbo.LaborRate lr
            JOIN dbo.Trade t  ON t.t_id = lr.t_id AND t.t_active = 1
            JOIN dbo.Trade tp ON tp.t_id = t.t_id_parent
            JOIN dbo.FormRule r ON r.xccc_id = lr.xccc_id AND r.t_id = tp.t_id AND r.fr_active = 1
            LEFT JOIN dbo.PMRule m ON m.fr_id = r.fr_id
            OUTER APPLY (
                SELECT TOP 1 ps.pms_id, ps.pms_name, ps.pms_visittype, ps.pms_startmonthday, ps.pms_endmonthday
                FROM dbo.PMSeason ps
                WHERE ps.pmr_id = m.pmr_id AND ps.t_id_season = t.t_id AND ps.pms_active = 1
                ORDER BY ps.pms_order, ps.pms_id) s
            WHERE lr.xccc_id = @XcccId AND {PmTradeNamePredicate}
            ORDER BY tp.t_trade, t.t_trade";
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<PmTicketTradeDto>(sql, new { XcccId = xcccId })).ToList();
    }

    #endregion

    #region first-time detection

    public async Task<PmFirstTimeDto> GetFirstTimeAsync(int lId, int parentTId)
    {
        // Status 6 = Rejected; completed legacy tickets are Complete (4), Invoiced (5) or Paid (9).
        var sql = $@"
            SELECT COUNT(*) AS Cnt, MAX(sr.sr_insertdatetime) AS LastDate
            FROM dbo.PMVisit v
            JOIN dbo.ServiceRequest sr ON sr.sr_id = v.sr_id
            JOIN dbo.Trade t ON t.t_id = sr.t_id
            WHERE sr.l_id = @LId AND (t.t_id_parent = @TId OR t.t_id = @TId) AND ISNULL(sr.s_id, 0) <> 6;

            SELECT COUNT(*) AS Cnt, MAX(sr.sr_insertdatetime) AS LastDate
            FROM dbo.ServiceRequest sr
            JOIN dbo.Trade t ON t.t_id = sr.t_id
            WHERE sr.l_id = @LId AND t.t_id_parent = @TId AND {PmTradeNamePredicate}
              AND sr.s_id IN (4, 5, 9)
              AND NOT EXISTS (SELECT 1 FROM dbo.PMVisit v WHERE v.sr_id = sr.sr_id);";
        using var connection = new SqlConnection(_connectionString);
        using var multi = await connection.QueryMultipleAsync(sql, new { LId = lId, TId = parentTId });
        var visits = await multi.ReadFirstAsync<(int Cnt, DateTime? LastDate)>();
        var legacy = await multi.ReadFirstAsync<(int Cnt, DateTime? LastDate)>();

        var result = new PmFirstTimeDto
        {
            PriorVisitCount = visits.Cnt,
            PriorLegacyCount = legacy.Cnt,
            LastVisitDate = new[] { visits.LastDate, legacy.LastDate }.Where(d => d.HasValue).Max(),
            FirstTime = visits.Cnt == 0 && legacy.Cnt == 0
        };
        result.Detail = result.FirstTime
            ? "No earlier PM found at this location for this trade."
            : $"{visits.Cnt} earlier PM visit{(visits.Cnt == 1 ? "" : "s")}, {legacy.Cnt} completed legacy PM ticket{(legacy.Cnt == 1 ? "" : "s")}"
              + (result.LastVisitDate.HasValue ? $"; latest {result.LastVisitDate:MM/dd/yyyy}." : ".");
        return result;
    }

    #endregion

    #region visit snapshot

    public async Task<bool> ServiceRequestExistsAsync(int srId)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM dbo.ServiceRequest WHERE sr_id = @SrId", new { SrId = srId }) > 0;
    }

    public async Task<PmVisitDto?> GetVisitBySrAsync(int srId)
    {
        var sql = VisitSelect + " WHERE v.sr_id = @SrId;"
                + UnitSelect + " WHERE u.pmv_id = (SELECT pmv_id FROM dbo.PMVisit WHERE sr_id = @SrId) ORDER BY u.pmvu_sequence, u.pmvu_id;";
        using var connection = new SqlConnection(_connectionString);
        using var multi = await connection.QueryMultipleAsync(sql, new { SrId = srId });
        var visit = await multi.ReadFirstOrDefaultAsync<PmVisitDto>();
        if (visit == null) return null;
        visit.Units = (await multi.ReadAsync<PmVisitUnitDto>()).ToList();
        return visit;
    }

    public async Task<int> CreateVisitAsync(CreatePmVisitRequest request, FormRuleDto? rule, int? userId)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        var existing = await connection.ExecuteScalarAsync<int?>("SELECT pmv_id FROM dbo.PMVisit WHERE sr_id = @SrId", new { request.SrId }, transaction);
        if (existing.HasValue)
        {
            transaction.Commit();
            return existing.Value;
        }

        var pm = rule?.Pm;
        var pricingJson = request.Pricing == null ? null : JsonSerializer.Serialize(request.Pricing);
        var pmvId = await connection.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.PMVisit (
                sr_id, u_id_createdby, fr_id, ft_id, ft_version, pms_id, pmv_visittype, pmv_firsttime, pmv_firsttimesource,
                pmv_billingmode, pmv_nteguideline, pmv_firsttimerule, pmv_contracttotal, pmv_pricingjson,
                pmv_coolingsetpoint, pmv_heatingsetpoint, pmv_thermostatschedule, pmv_thermostatschedulenote, pmv_thermostatlock,
                pmv_antialgae, pmv_phototimestamp, pmv_managerseesoldfilters, pmv_pricingdiscussallowed, pmv_immediatequoterequired,
                pmv_immediatecallifincomplete, pmv_submissiondeadline, pmv_submissiondeadlinenote, pmv_closeoutdocs, pmv_submissiondestination,
                pmv_ivrrequired, pmv_customerform, pmv_customerformurl, att_id_customerform, pmv_customerformnote,
                pmv_paramsconfirmed_u_id, pmv_paramsconfirmeddatetime)
            VALUES (
                @SrId, @UserId, @FrId, @FtId, @FtVersion, @PmsId, @VisitType, @FirstTime, @FirstTimeSource,
                @BillingMode, @NteGuideline, @FirstTimeRule, @ContractTotal, @PricingJson,
                @CoolingSetpoint, @HeatingSetpoint, @ThermostatSchedule, @ThermostatScheduleNote, @ThermostatLock,
                @AntiAlgae, @PhotoTimestamp, @ManagerSeesOldFilters, @PricingDiscussAllowed, @ImmediateQuoteRequired,
                @ImmediateCallIfIncomplete, @SubmissionDeadline, @SubmissionDeadlineNote, @CloseoutDocs, @SubmissionDestination,
                @IvrRequired, @CustomerForm, @CustomerFormUrl, @AttIdCustomerForm, @CustomerFormNote,
                @ParamsConfirmedUId, @ParamsConfirmedDateTime);
            SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new
            {
                request.SrId,
                UserId = userId,
                FrId = rule?.FrId,
                FtId = rule?.FtId,
                FtVersion = rule?.TemplateVersion,
                request.PmsId,
                VisitType = NullIfBlank(request.VisitType),
                request.FirstTime,
                FirstTimeSource = NullIfBlank(request.FirstTimeSource) ?? "Auto",
                BillingMode = pm?.BillingMode,
                NteGuideline = pm?.NteGuideline,
                FirstTimeRule = pm?.FirstTimeRule,
                request.ContractTotal,
                PricingJson = pricingJson,
                CoolingSetpoint = (byte?)pm?.CoolingSetpoint,
                HeatingSetpoint = (byte?)pm?.HeatingSetpoint,
                ThermostatSchedule = pm?.ThermostatSchedule,
                ThermostatScheduleNote = pm?.ThermostatScheduleNote,
                ThermostatLock = pm?.ThermostatLock,
                AntiAlgae = pm?.AntiAlgae,
                PhotoTimestamp = pm?.PhotoTimestamp,
                ManagerSeesOldFilters = pm?.ManagerSeesOldFilters,
                PricingDiscussAllowed = pm?.PricingDiscussAllowed,
                ImmediateQuoteRequired = pm?.ImmediateQuoteRequired,
                ImmediateCallIfIncomplete = pm?.ImmediateCallIfIncomplete,
                SubmissionDeadline = pm?.SubmissionDeadline,
                SubmissionDeadlineNote = pm?.SubmissionDeadlineNote,
                CloseoutDocs = pm?.CloseoutDocs,
                SubmissionDestination = pm?.SubmissionDestination,
                IvrRequired = pm?.IvrRequired,
                CustomerForm = pm?.CustomerForm,
                CustomerFormUrl = pm?.CustomerFormUrl,
                AttIdCustomerForm = pm?.AttIdCustomerForm,
                CustomerFormNote = pm?.CustomerFormNote,
                ParamsConfirmedUId = request.ParamsConfirmed ? userId : null,
                ParamsConfirmedDateTime = request.ParamsConfirmed ? DateTime.Now : (DateTime?)null
            }, transaction);

        var seq = 0;
        foreach (var u in request.Units.OrderBy(x => x.Sequence))
        {
            seq++;
            await connection.ExecuteAsync(@"
                INSERT INTO dbo.PMVisitUnit (pmv_id, pmvu_sequence, pmvu_source, as_id, asc_id, pmvu_capacitytons, pmvu_label, pmvu_price, pmvu_tierlabel)
                VALUES (@PmvId, @Sequence, 'Ticket', @AsId, @AscId, @CapacityTons, @Label, @Price, @TierLabel)",
                new
                {
                    PmvId = pmvId,
                    Sequence = (byte)Math.Min(seq, 255),
                    u.AsId,
                    u.AscId,
                    u.CapacityTons,
                    Label = Left(NullIfBlank(u.Label), 120),
                    u.Price,
                    TierLabel = Left(NullIfBlank(u.TierLabel), 80)
                }, transaction);
        }

        transaction.Commit();
        return pmvId;
    }

    #endregion

    private static string? NullIfBlank(string? s) => string.IsNullOrWhiteSpace(s) ? null : s.Trim();
    private static string? Left(string? s, int max) => s == null ? null : (s.Length <= max ? s : s[..max]);
}
