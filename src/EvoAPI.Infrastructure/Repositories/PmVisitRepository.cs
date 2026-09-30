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
            v.pmv_insertdatetime            AS InsertDateTime,
            v.pmv_weather                   AS Weather,
            v.pmv_outdoortempf              AS OutdoorTempF,
            v.pmv_weathersource             AS WeatherSource,
            v.pmv_weatherdatetime           AS WeatherDateTime,
            v.pmv_fullscopecompleted        AS FullScopeCompleted,
            v.pmv_incompletereason          AS IncompleteReason,
            v.pmv_customerformcompleted     AS CustomerFormCompleted,
            v.pmv_submitteddatetime         AS SubmittedDateTime
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

    #region the tech's visit (slice 4a)

    // TimeTracking type 3 = check-in to a work order (appsettings TimeTracking:CheckIn); an open row is one with no tt_end.
    private const string CheckedInSub = "CASE WHEN EXISTS (SELECT 1 FROM dbo.TimeTracking tt WHERE tt.wo_id = wo.wo_id AND tt.ttt_id = 3 AND tt.tt_end IS NULL AND tt.u_id = @UserId) THEN 1 ELSE 0 END";

    public async Task<List<PmVisitListItemDto>> GetMyVisitsAsync(int userId, bool allTechs)
    {
        var sql = $@"
            SELECT
                sr.sr_id                AS SrId,
                wo.wo_id                AS WoId,
                sr.sr_requestnumber     AS RequestNumber,
                wo.wo_workordernumber   AS WoNumber,
                cc.cc_name              AS CallCenter,
                c.c_name                AS Company,
                l.l_location            AS Location,
                a.a_address1            AS Address,
                a.a_city                AS City,
                a.a_state               AS State,
                t.t_trade               AS Trade,
                v.pmv_visittype         AS VisitType,
                wo.wo_startdatetime     AS StartDateTime,
                wo.wo_enddatetime       AS EndDateTime,
                sr.sr_servicebydate     AS ServiceByDate,
                ss.ss_statussecondary   AS SecondaryStatus,
                ss.ss_color             AS StatusColor,
                u.u_firstname           AS AssignedFirstName,
                u.u_lastname            AS AssignedLastName,
                v.pmv_id                AS PmvId,
                v.pmv_status            AS PmvStatus,
                (SELECT COUNT(*) FROM dbo.PMVisitUnit pu WHERE pu.pmv_id = v.pmv_id) AS UnitCount,
                {CheckedInSub}          AS CheckedIn
            FROM dbo.PMVisit v
            JOIN dbo.ServiceRequest sr ON sr.sr_id = v.sr_id
            JOIN dbo.WorkOrder wo ON wo.sr_id = sr.sr_id
            LEFT JOIN dbo.xrefWorkOrderUser xwou ON xwou.wo_id = wo.wo_id
            LEFT JOIN dbo.[User] u ON u.u_id = xwou.u_id
            JOIN dbo.xrefCompanyCallCenter x ON x.xccc_id = sr.xccc_id
            JOIN dbo.Company c ON c.c_id = x.c_id
            JOIN dbo.CallCenter cc ON cc.cc_id = x.cc_id
            JOIN dbo.Location l ON l.l_id = sr.l_id
            LEFT JOIN dbo.Address a ON a.a_id = l.a_id
            JOIN dbo.Trade t ON t.t_id = sr.t_id
            LEFT JOIN dbo.StatusSecondary ss ON ss.ss_id = wo.ss_id
            WHERE (@AllTechs = 1 OR xwou.u_id = @UserId)
              AND ISNULL(sr.s_id, 0) <> 6
              AND (wo.wo_startdatetime IS NULL
                   OR wo.wo_startdatetime BETWEEN DATEADD(DAY, -60, GETDATE()) AND DATEADD(DAY, 180, GETDATE())
                   OR v.pmv_status IN ('InProgress', 'Incomplete'))
            ORDER BY CASE WHEN wo.wo_startdatetime IS NULL THEN 1 ELSE 0 END, wo.wo_startdatetime, sr.sr_id";
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<PmVisitListItemDto>(sql, new { UserId = userId, AllTechs = allTechs ? 1 : 0 })).ToList();
    }

    public async Task<PmVisitHeaderDto?> GetHeaderAsync(int srId, int userId)
    {
        // The caller's own work order on this SR when they are assigned one, else the primary work order (office / admin view).
        var sql = $@"
            SELECT TOP 1
                sr.sr_id                    AS SrId,
                sr.sr_requestnumber         AS RequestNumber,
                wo.wo_id                    AS WoId,
                wo.wo_workordernumber       AS WoNumber,
                sr.xccc_id                  AS XcccId,
                sr.l_id                     AS LId,
                sr.t_id                     AS TIdSub,
                ISNULL(t.t_id_parent, t.t_id) AS TIdParent,
                t.t_trade                   AS Trade,
                cc.cc_name                  AS CallCenter,
                c.c_name                    AS Company,
                l.l_location                AS Location,
                a.a_address1                AS Address1,
                a.a_address2                AS Address2,
                a.a_city                    AS City,
                a.a_state                   AS State,
                a.a_zip                     AS Zip,
                TRY_CAST(a.a_latitude AS DECIMAL(9,6))  AS Latitude,
                TRY_CAST(a.a_longitude AS DECIMAL(9,6)) AS Longitude,
                l.l_phone                   AS LocationPhone,
                l.l_hours                   AS LocationHours,
                l.l_note                    AS LocationNote,
                sr.sr_callnote              AS CallNote,
                sr.sr_ivrrequestnumber      AS IvrRequestNumber,
                sr.sr_servicebydate         AS ServiceByDate,
                sr.sr_pmunitcount           AS PmUnitCount,
                sr.sr_nte                   AS Nte,
                sr.sr_flatorhourly          AS FlatOrHourly,
                sr.sr_rateflat              AS RateFlat,
                wo.wo_startdatetime         AS StartDateTime,
                wo.wo_enddatetime           AS EndDateTime,
                ss.ss_statussecondary       AS SecondaryStatus,
                ss.ss_code                  AS SecondaryStatusCode,
                sr.sr_sitecontact_name      AS SiteContactName,
                sr.sr_sitecontact_phone     AS SiteContactPhone,
                CASE WHEN mine.wo_id IS NOT NULL THEN 1 ELSE 0 END AS AssignedToCaller,
                {CheckedInSub}              AS CheckedIn,
                (SELECT MAX(tt.tt_begin) FROM dbo.TimeTracking tt WHERE tt.wo_id = wo.wo_id AND tt.ttt_id = 3 AND tt.tt_end IS NULL AND tt.u_id = @UserId) AS CheckInDateTime
            FROM dbo.ServiceRequest sr
            JOIN dbo.Trade t ON t.t_id = sr.t_id
            JOIN dbo.xrefCompanyCallCenter x ON x.xccc_id = sr.xccc_id
            JOIN dbo.Company c ON c.c_id = x.c_id
            JOIN dbo.CallCenter cc ON cc.cc_id = x.cc_id
            JOIN dbo.Location l ON l.l_id = sr.l_id
            LEFT JOIN dbo.Address a ON a.a_id = l.a_id
            OUTER APPLY (SELECT TOP 1 w.wo_id FROM dbo.WorkOrder w JOIN dbo.xrefWorkOrderUser xw ON xw.wo_id = w.wo_id WHERE w.sr_id = sr.sr_id AND xw.u_id = @UserId ORDER BY w.wo_id) mine
            LEFT JOIN dbo.WorkOrder wo ON wo.wo_id = ISNULL(mine.wo_id, sr.wo_id_primary)
            LEFT JOIN dbo.StatusSecondary ss ON ss.ss_id = wo.ss_id
            WHERE sr.sr_id = @SrId";
        using var connection = new SqlConnection(_connectionString);
        return await connection.QueryFirstOrDefaultAsync<PmVisitHeaderDto>(sql, new { SrId = srId, UserId = userId });
    }

    private const string UnitOpenSelect = @"
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
            u.pmvu_assetconfirmed   AS AssetConfirmed,
            u.pmvu_notservicedreason AS NotServicedReason,
            u.pmvu_note             AS Note
        FROM dbo.PMVisitUnit u
        LEFT JOIN dbo.AssetCategory ac ON ac.asc_id = u.asc_id";

    public async Task<PmVisitUnitOpenDto?> GetUnitAsync(int pmvuId)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.QueryFirstOrDefaultAsync<PmVisitUnitOpenDto>(UnitOpenSelect + " WHERE u.pmvu_id = @PmvuId", new { PmvuId = pmvuId });
    }

    public async Task<int> AddUnitAsync(int pmvId, int asId, int? ascId, decimal? capacityTons, string? label)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int>(@"
            DECLARE @seq INT = ISNULL((SELECT MAX(pmvu_sequence) FROM dbo.PMVisitUnit WHERE pmv_id = @PmvId), 0) + 1;
            INSERT INTO dbo.PMVisitUnit (pmv_id, pmvu_sequence, pmvu_source, as_id, asc_id, pmvu_capacitytons, pmvu_label)
            VALUES (@PmvId, CASE WHEN @seq > 255 THEN 255 ELSE @seq END, 'Tech', @AsId, @AscId, @CapacityTons, @Label);
            SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { PmvId = pmvId, AsId = asId, AscId = ascId, CapacityTons = capacityTons, Label = Left(NullIfBlank(label), 120) });
    }

    public async Task<bool> UpdateUnitAsync(int pmvuId, SavePmVisitUnitRequest request)
    {
        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.ExecuteAsync(@"
            UPDATE dbo.PMVisitUnit SET
                pmvu_status = ISNULL(@Status, pmvu_status),
                pmvu_notservicedreason = CASE WHEN @Status = 'NotServiced' THEN @Reason WHEN @Status IS NOT NULL THEN NULL ELSE pmvu_notservicedreason END,
                pmvu_assetconfirmed = ISNULL(@AssetConfirmed, pmvu_assetconfirmed),
                pmvu_note = CASE WHEN @NoteGiven = 1 THEN @Note ELSE pmvu_note END,
                pmvu_completeddatetime = CASE WHEN @Status IN ('Serviced', 'NotServiced') THEN GETDATE() WHEN @Status = 'Pending' THEN NULL ELSE pmvu_completeddatetime END,
                pmvu_modifieddatetime = GETDATE()
            WHERE pmvu_id = @PmvuId",
            new
            {
                PmvuId = pmvuId,
                Status = NullIfBlank(request.Status),
                Reason = Left(NullIfBlank(request.NotServicedReason), 200),
                request.AssetConfirmed,
                NoteGiven = request.Note != null ? 1 : 0,
                Note = Left(NullIfBlank(request.Note), 2000)
            });
        return rows > 0;
    }

    public async Task<bool> LinkUnitAssetAsync(int pmvuId, int asId, int? ascId, decimal? capacityTons, string? label)
    {
        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.ExecuteAsync(@"
            UPDATE dbo.PMVisitUnit SET as_id = @AsId, asc_id = ISNULL(@AscId, asc_id), pmvu_capacitytons = ISNULL(@CapacityTons, pmvu_capacitytons),
                pmvu_label = ISNULL(@Label, pmvu_label), pmvu_modifieddatetime = GETDATE()
            WHERE pmvu_id = @PmvuId",
            new { PmvuId = pmvuId, AsId = asId, AscId = ascId, CapacityTons = capacityTons, Label = Left(NullIfBlank(label), 120) });
        return rows > 0;
    }

    private const string AnswerSelect = @"
        SELECT
            an.pmva_id              AS PmvaId,
            an.pmv_id               AS PmvId,
            an.pmvu_id              AS PmvuId,
            an.fq_id                AS FqId,
            an.pmva_code            AS Code,
            an.pmva_repeatindex     AS RepeatIndex,
            an.pmva_question        AS Question,
            an.pmva_answer          AS Answer,
            an.pmva_numeric         AS Numeric,
            an.att_id               AS AttId,
            att.att_filename        AS AttFilename,
            an.pmva_latitude        AS Latitude,
            an.pmva_longitude       AS Longitude,
            an.u_id                 AS UId,
            ISNULL(an.pmva_modifieddatetime, an.pmva_insertdatetime) AS ModifiedDateTime
        FROM dbo.PMVisitAnswer an
        LEFT JOIN dbo.Attachment att ON att.att_id = an.att_id";

    public async Task<List<PmVisitAnswerDto>> GetAnswersAsync(int pmvId)
    {
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<PmVisitAnswerDto>(AnswerSelect + " WHERE an.pmv_id = @PmvId ORDER BY an.pmvu_id, an.fq_id, an.pmva_repeatindex", new { PmvId = pmvId })).ToList();
    }

    public async Task<PmVisitAnswerDto> UpsertAnswerAsync(int pmvId, SavePmVisitAnswerRequest r, string? code, int? userId)
    {
        using var connection = new SqlConnection(_connectionString);
        var id = await connection.ExecuteScalarAsync<int>(@"
            DECLARE @id INT = (SELECT pmva_id FROM dbo.PMVisitAnswer WHERE pmv_id = @PmvId AND fq_id = @FqId AND pmva_repeatindex = @RepeatIndex
                               AND ((pmvu_id IS NULL AND @PmvuId IS NULL) OR pmvu_id = @PmvuId));
            IF @id IS NULL
            BEGIN
                INSERT INTO dbo.PMVisitAnswer (pmv_id, pmvu_id, fq_id, pmva_code, pmva_repeatindex, pmva_question, pmva_answer, pmva_numeric, att_id, pmva_latitude, pmva_longitude, u_id)
                VALUES (@PmvId, @PmvuId, @FqId, @Code, @RepeatIndex, @Question, @Answer, @Numeric, @AttId, @Latitude, @Longitude, @UserId);
                SET @id = SCOPE_IDENTITY();
            END
            ELSE
                UPDATE dbo.PMVisitAnswer SET pmva_answer = @Answer, pmva_numeric = @Numeric, att_id = @AttId, pmva_latitude = @Latitude, pmva_longitude = @Longitude,
                    pmva_question = ISNULL(@Question, pmva_question), pmva_code = ISNULL(@Code, pmva_code), u_id = @UserId, pmva_modifieddatetime = GETDATE()
                WHERE pmva_id = @id;
            UPDATE dbo.PMVisit SET pmv_status = CASE WHEN pmv_status = 'NotStarted' THEN 'InProgress' ELSE pmv_status END, pmv_modifieddatetime = GETDATE() WHERE pmv_id = @PmvId;
            SELECT @id;",
            new
            {
                PmvId = pmvId,
                r.PmvuId,
                r.FqId,
                Code = Left(NullIfBlank(code), 12),
                RepeatIndex = (byte)Math.Max(0, Math.Min(255, r.RepeatIndex)),
                Question = Left(NullIfBlank(r.Question), 400),
                Answer = r.Answer,
                r.Numeric,
                r.AttId,
                r.Latitude,
                r.Longitude,
                UserId = userId
            });
        return (await connection.QueryFirstAsync<PmVisitAnswerDto>(AnswerSelect + " WHERE an.pmva_id = @Id", new { Id = id }));
    }

    public async Task<bool> RecordCheckInAsync(int pmvId, WeatherResultDto? weather)
    {
        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.ExecuteAsync(@"
            UPDATE dbo.PMVisit SET
                pmv_status = CASE WHEN pmv_status IN ('NotStarted', 'Incomplete') THEN 'InProgress' ELSE pmv_status END,
                pmv_weather = CASE WHEN @HasWeather = 1 THEN @Weather ELSE pmv_weather END,
                pmv_outdoortempf = CASE WHEN @HasWeather = 1 THEN @TempF ELSE pmv_outdoortempf END,
                pmv_weathersource = CASE WHEN @HasWeather = 1 THEN @Source ELSE pmv_weathersource END,
                pmv_weatherdatetime = CASE WHEN @HasWeather = 1 THEN @ObservedAt ELSE pmv_weatherdatetime END,
                pmv_modifieddatetime = GETDATE()
            WHERE pmv_id = @PmvId",
            new
            {
                PmvId = pmvId,
                HasWeather = weather != null ? 1 : 0,
                Weather = Left(weather?.Description, 100),
                TempF = weather?.TemperatureF,
                Source = Left(weather?.Source, 40),
                ObservedAt = weather?.ObservedAt
            });
        return rows > 0;
    }

    #endregion

    #region checkout (slice 4b)

    private const string FindingSelect = @"
        SELECT
            f.pmvf_id                   AS PmvfId,
            f.pmv_id                    AS PmvId,
            f.pmvu_id                   AS PmvuId,
            u.pmvu_label                AS UnitLabel,
            f.pmvf_component            AS Component,
            f.pmvf_description          AS Description,
            f.pmvf_severity             AS Severity,
            f.pmvf_impact               AS Impact,
            f.pmvf_risk                 AS Risk,
            f.pmvf_action               AS [Action],
            f.pmvf_parts                AS Parts,
            f.pmvf_laborestimate        AS LaborEstimate,
            f.pmvf_quoterequired        AS QuoteRequired,
            f.pmvf_callcentercontacted  AS CallCenterContacted,
            f.pmvf_contactdetails       AS ContactDetails,
            f.att_id                    AS AttId,
            att.att_filename            AS AttFilename,
            f.sr_id_proposal            AS SrIdProposal,
            psr.sr_requestnumber        AS ProposalRequestNumber,
            f.u_id                      AS UId,
            f.pmvf_insertdatetime       AS InsertDateTime
        FROM dbo.PMVisitFinding f
        LEFT JOIN dbo.PMVisitUnit u ON u.pmvu_id = f.pmvu_id
        LEFT JOIN dbo.Attachment att ON att.att_id = f.att_id
        LEFT JOIN dbo.ServiceRequest psr ON psr.sr_id = f.sr_id_proposal";

    public async Task<List<PmVisitFindingDto>> GetFindingsAsync(int pmvId)
    {
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<PmVisitFindingDto>(FindingSelect + " WHERE f.pmv_id = @PmvId ORDER BY f.pmvf_id", new { PmvId = pmvId })).ToList();
    }

    public async Task<PmVisitFindingDto?> GetFindingAsync(int pmvfId)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.QueryFirstOrDefaultAsync<PmVisitFindingDto>(FindingSelect + " WHERE f.pmvf_id = @Id", new { Id = pmvfId });
    }

    private static object FindingArgs(SavePmVisitFindingRequest r, int? userId) => new
    {
        r.PmvuId,
        Component = Left(NullIfBlank(r.Component), 120),
        Description = NullIfBlank(r.Description),
        Severity = Left(NullIfBlank(r.Severity), 30),
        Impact = Left(NullIfBlank(r.Impact), 40),
        Risk = Left(NullIfBlank(r.Risk), 20),
        Action = Left(NullIfBlank(r.Action), 40),
        Parts = Left(NullIfBlank(r.Parts), 400),
        LaborEstimate = Left(NullIfBlank(r.LaborEstimate), 200),
        r.QuoteRequired,
        CallCenterContacted = Left(NullIfBlank(r.CallCenterContacted), 10),
        ContactDetails = Left(NullIfBlank(r.ContactDetails), 400),
        r.AttId,
        UserId = userId
    };

    public async Task<int> CreateFindingAsync(int pmvId, SavePmVisitFindingRequest request, int? userId)
    {
        using var connection = new SqlConnection(_connectionString);
        var args = new Dapper.DynamicParameters(FindingArgs(request, userId));
        args.Add("PmvId", pmvId);
        return await connection.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.PMVisitFinding (pmv_id, pmvu_id, pmvf_component, pmvf_description, pmvf_severity, pmvf_impact, pmvf_risk, pmvf_action, pmvf_parts, pmvf_laborestimate,
                pmvf_quoterequired, pmvf_callcentercontacted, pmvf_contactdetails, att_id, u_id)
            VALUES (@PmvId, @PmvuId, @Component, @Description, @Severity, @Impact, @Risk, @Action, @Parts, @LaborEstimate, @QuoteRequired, @CallCenterContacted, @ContactDetails, @AttId, @UserId);
            SELECT CAST(SCOPE_IDENTITY() AS INT);", args);
    }

    public async Task<bool> UpdateFindingAsync(int pmvfId, SavePmVisitFindingRequest request, int? userId)
    {
        using var connection = new SqlConnection(_connectionString);
        var args = new Dapper.DynamicParameters(FindingArgs(request, userId));
        args.Add("Id", pmvfId);
        return await connection.ExecuteAsync(@"
            UPDATE dbo.PMVisitFinding SET pmvu_id = @PmvuId, pmvf_component = @Component, pmvf_description = @Description, pmvf_severity = @Severity, pmvf_impact = @Impact,
                pmvf_risk = @Risk, pmvf_action = @Action, pmvf_parts = @Parts, pmvf_laborestimate = @LaborEstimate, pmvf_quoterequired = @QuoteRequired,
                pmvf_callcentercontacted = @CallCenterContacted, pmvf_contactdetails = @ContactDetails, att_id = @AttId, u_id = @UserId, pmvf_modifieddatetime = GETDATE()
            WHERE pmvf_id = @Id", args) > 0;
    }

    public async Task<bool> DeleteFindingAsync(int pmvfId)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteAsync("DELETE FROM dbo.PMVisitFinding WHERE pmvf_id = @Id AND sr_id_proposal IS NULL", new { Id = pmvfId }) > 0;
    }

    public async Task<List<PmVisitMaterialDto>> GetMaterialsAsync(int srId)
    {
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<PmVisitMaterialDto>(@"
            SELECT x.xwosi_id AS XwosiId, x.wo_id AS WoId, x.as_id AS AsId,
                   COALESCE(pu.pmvu_label, a.as_unittag) AS UnitLabel,
                   si.si_name AS Name, si.si_description AS Description, x.xwosi_basecost AS BaseCost, x.xwosi_quantity AS Quantity,
                   CAST(ISNULL(x.xwosi_forquote, 0) AS BIT) AS ForQuote, x.xwosi_insertdatetime AS InsertDateTime
            FROM dbo.xrefWorkOrderServiceItem x
            JOIN dbo.WorkOrder wo ON wo.wo_id = x.wo_id
            LEFT JOIN dbo.ServiceItem si ON si.si_id = x.si_id
            LEFT JOIN dbo.Asset a ON a.as_id = x.as_id
            OUTER APPLY (SELECT TOP 1 u.pmvu_label FROM dbo.PMVisitUnit u JOIN dbo.PMVisit v ON v.pmv_id = u.pmv_id WHERE v.sr_id = wo.sr_id AND u.as_id = x.as_id) pu
            WHERE wo.sr_id = @SrId
            ORDER BY x.xwosi_id", new { SrId = srId })).ToList();
    }

    public async Task<PmVisitSrBasicsDto?> GetSrBasicsAsync(int srId)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.QueryFirstOrDefaultAsync<PmVisitSrBasicsDto>(@"
            SELECT sr.sr_id AS SrId, sr.xccc_id AS XcccId, sr.l_id AS LId, sr.t_id AS TId, ISNULL(t.t_id_parent, t.t_id) AS TIdParent, sr.p_id AS PId,
                   sr.sr_requestnumber AS RequestNumber, sr.sr_tripcharge_worked AS TripCharge, sr.sr_ivrrequestnumber AS IvrRequestNumber,
                   sr.sr_sitecontact_name AS SiteContactName, sr.sr_sitecontact_phone AS SiteContactPhone, sr.sr_sitecontact_email AS SiteContactEmail
            FROM dbo.ServiceRequest sr JOIN dbo.Trade t ON t.t_id = sr.t_id WHERE sr.sr_id = @SrId", new { SrId = srId });
    }

    public async Task<int?> GetProposalTradeAsync(int xcccId, int parentTId)
    {
        var sql = $@"
            SELECT TOP 1 t.t_id
            FROM dbo.LaborRate lr JOIN dbo.Trade t ON t.t_id = lr.t_id
            WHERE lr.xccc_id = @XcccId AND t.t_id_parent = @ParentTId AND t.t_active = 1 AND NOT {PmTradeNamePredicate}
            ORDER BY CASE WHEN t.t_trade LIKE '%Repair%' THEN 0 ELSE 1 END, t.t_trade";
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int?>(sql, new { XcccId = xcccId, ParentTId = parentTId });
    }

    public async Task<bool> SetWorkOrderStatusByCodeAsync(int srId, string ssCode)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteAsync(@"
            UPDATE wo SET ss_id = ss.ss_id FROM dbo.WorkOrder wo CROSS JOIN dbo.StatusSecondary ss WHERE wo.sr_id = @SrId AND ss.ss_code = @Code;
            UPDATE sr SET ss_id = ss.ss_id FROM dbo.ServiceRequest sr CROSS JOIN dbo.StatusSecondary ss WHERE sr.sr_id = @SrId AND ss.ss_code = @Code;",
            new { SrId = srId, Code = ssCode }) > 0;
    }

    public async Task<int> LinkFindingsToProposalAsync(int pmvId, int srIdProposal)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteAsync("UPDATE dbo.PMVisitFinding SET sr_id_proposal = @SrIdProposal, pmvf_modifieddatetime = GETDATE() WHERE pmv_id = @PmvId AND pmvf_quoterequired = 1 AND sr_id_proposal IS NULL",
            new { PmvId = pmvId, SrIdProposal = srIdProposal });
    }

    public async Task<bool> SubmitAsync(int pmvId, string status, bool? fullScopeCompleted, string? incompleteReason, string? callCenterNotified, string? notificationDetails,
        string? customerRep, bool? customerFormCompleted, string? ivrCheckout, string? managerName, string? managerNotified, int? userId)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteAsync(@"
            UPDATE dbo.PMVisit SET
                pmv_status = @Status,
                pmv_fullscopecompleted = @FullScopeCompleted,
                pmv_incompletereason = @IncompleteReason,
                pmv_callcenternotified = @CallCenterNotified,
                pmv_notificationdetails = @NotificationDetails,
                pmv_customerrep = @CustomerRep,
                pmv_customerformcompleted = ISNULL(@CustomerFormCompleted, pmv_customerformcompleted),
                pmv_ivrcheckout = @IvrCheckout,
                pmv_managername = ISNULL(@ManagerName, pmv_managername),
                pmv_managernotified = ISNULL(@ManagerNotified, pmv_managernotified),
                pmv_submitteddatetime = GETDATE(),
                u_id_submitted = @UserId,
                pmv_modifieddatetime = GETDATE()
            WHERE pmv_id = @PmvId",
            new
            {
                PmvId = pmvId, Status = status, FullScopeCompleted = fullScopeCompleted, IncompleteReason = Left(incompleteReason, 1000), CallCenterNotified = Left(callCenterNotified, 10),
                NotificationDetails = Left(notificationDetails, 500), CustomerRep = Left(customerRep, 100), CustomerFormCompleted = customerFormCompleted, IvrCheckout = Left(ivrCheckout, 40),
                ManagerName = Left(managerName, 100), ManagerNotified = Left(managerNotified, 10), UserId = userId
            }) > 0;
    }

    #endregion

    private static string? NullIfBlank(string? s) => string.IsNullOrWhiteSpace(s) ? null : s.Trim();
    private static string? Left(string? s, int max) => s == null ? null : (s.Length <= max ? s : s[..max]);
}
