using System.Data.SqlClient;
using Dapper;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;

namespace EvoAPI.Infrastructure.Repositories;

/// <summary>
/// Data access for the forms engine (FormTemplate / FormSection / FormQuestion / FormAnswerList / FormRule /
/// xrefFormRuleQuestion) and the PM terms hanging off a rule (PMRule / PMRateTier / PMSeason).
/// Tables come from create_form_tables.sql; seed data from sql/forms/.
/// </summary>
public class FormRepository : IFormRepository
{
    private readonly string _connectionString;

    // "PM" trades are recognised by name, the same way the schedule does it (PM - HVAC, PM - Spring, Oven PM ...).
    private const string PmTradeNamePredicate = "(t.t_trade LIKE 'PM %' OR t.t_trade LIKE 'PM-%' OR t.t_trade LIKE '% PM' OR t.t_trade LIKE '% PM %')";

    private const string TemplateSelect = @"
        SELECT
            p.ft_id               AS FtId,
            p.t_id                AS TId,
            t.t_trade             AS TradeName,
            p.ft_name             AS Name,
            p.ft_version          AS Version,
            p.ft_active           AS Active,
            p.ft_note             AS Note,
            p.ft_insertdatetime   AS InsertDateTime,
            p.ft_modifieddatetime AS ModifiedDateTime,
            (SELECT COUNT(*) FROM dbo.FormSection s WHERE s.ft_id = p.ft_id) AS SectionCount,
            (SELECT COUNT(*) FROM dbo.FormQuestion q JOIN dbo.FormSection s ON s.fs_id = q.fs_id WHERE s.ft_id = p.ft_id) AS QuestionCount,
            (SELECT COUNT(*) FROM dbo.FormRule r WHERE r.ft_id = p.ft_id) AS RuleCount
        FROM dbo.FormTemplate p
        JOIN dbo.Trade t ON t.t_id = p.t_id";

    private const string SectionSelect = @"
        SELECT
            s.fs_id            AS FsId,
            s.ft_id            AS FtId,
            s.fs_name          AS Name,
            s.fs_phase         AS Phase,
            s.fs_order         AS [Order],
            s.fs_repeatperunit AS RepeatPerUnit,
            s.fs_condition     AS Condition,
            s.fs_active        AS Active
        FROM dbo.FormSection s";

    private const string QuestionSelect = @"
        SELECT
            q.fq_id            AS FqId,
            q.fs_id            AS FsId,
            q.fq_code          AS Code,
            q.clat_id          AS ClatId,
            at.clat_type       AS AnswerType,
            q.fq_question      AS Question,
            q.fq_order         AS [Order],
            q.fq_requirement   AS Requirement,
            q.fq_condition     AS Condition,
            q.fal_id           AS FalId,
            al.fal_name        AS AnswerListName,
            al.fal_values      AS AnswerListValues,
            q.fq_answervalues  AS AnswerValues,
            q.fq_datatype      AS DataType,
            q.fq_unit          AS Unit,
            q.fq_min           AS Min,
            q.fq_max           AS Max,
            q.fq_repeatkey     AS RepeatKey,
            q.fq_calcformula   AS CalcFormula,
            q.fq_writesto      AS WritesTo,
            q.fq_photorequired AS PhotoRequired,
            q.fq_phototiming   AS PhotoTiming,
            q.fq_linkedcode    AS LinkedCode,
            q.fq_estminutes    AS EstMinutes,
            q.fq_seasons       AS Seasons,
            q.fq_skip_answer   AS SkipAnswer,
            q.fq_skip_to_order AS SkipToOrder,
            q.fq_triggernote   AS TriggerNote,
            q.fq_active        AS Active
        FROM dbo.FormQuestion q
        JOIN dbo.FormSection s ON s.fs_id = q.fs_id
        LEFT JOIN dbo.CheckListAnswerType at ON at.clat_id = q.clat_id
        LEFT JOIN dbo.FormAnswerList al ON al.fal_id = q.fal_id";

    private const string RuleSelect = @"
        SELECT
            r.fr_id               AS FrId,
            r.xccc_id             AS XcccId,
            r.t_id                AS TId,
            t.t_trade             AS TradeName,
            r.ft_id               AS FtId,
            p.ft_name             AS TemplateName,
            p.ft_version          AS TemplateVersion,
            r.fr_active           AS Active,
            r.fr_note             AS Note,
            r.fr_insertdatetime   AS InsertDateTime,
            r.fr_modifieddatetime AS ModifiedDateTime
        FROM dbo.FormRule r
        JOIN dbo.Trade t ON t.t_id = r.t_id
        LEFT JOIN dbo.FormTemplate p ON p.ft_id = r.ft_id";

    private const string PmRuleSelect = @"
        SELECT
            m.pmr_id                        AS PmrId,
            m.fr_id                         AS FrId,
            m.pmbm_id                       AS PmbmId,
            bm.pmbm_mode                    AS BillingMode,
            m.pmr_nteguideline              AS NteGuideline,
            m.pmr_firsttimerule             AS FirstTimeRule,
            m.pmr_hourlyrate                AS HourlyRate,
            m.pmr_hourcapperunit            AS HourCapPerUnit,
            m.pmr_hourcappervisit           AS HourCapPerVisit,
            m.pmr_tripcharge                AS TripCharge,
            m.att_id_pricingcontract        AS AttIdPricingContract,
            m.pmr_pricingnote               AS PricingNote,
            m.pmr_filtersincluded           AS FiltersIncluded,
            m.pmr_nofilterchange            AS NoFilterChange,
            m.pmr_freefilters               AS FreeFilters,
            m.pmr_freebeltchangesperyear    AS FreeBeltChangesPerYear,
            m.pmr_beltschargeable           AS BeltsChargeable,
            m.pmr_customerform              AS CustomerForm,
            m.pmr_customerformurl           AS CustomerFormUrl,
            m.att_id_customerform           AS AttIdCustomerForm,
            m.pmr_customerformnote          AS CustomerFormNote,
            CAST(m.pmr_coolingsetpoint AS INT) AS CoolingSetpoint,
            CAST(m.pmr_heatingsetpoint AS INT) AS HeatingSetpoint,
            m.pmr_thermostatschedule        AS ThermostatSchedule,
            m.pmr_thermostatschedulenote    AS ThermostatScheduleNote,
            m.pmr_thermostatlock            AS ThermostatLock,
            m.pmr_antialgae                 AS AntiAlgae,
            m.pmr_phototimestamp            AS PhotoTimestamp,
            m.pmr_managerseesoldfilters     AS ManagerSeesOldFilters,
            m.pmr_pricingdiscussallowed     AS PricingDiscussAllowed,
            m.pmr_immediatequoterequired    AS ImmediateQuoteRequired,
            m.pmr_immediatecallifincomplete AS ImmediateCallIfIncomplete,
            m.pmr_submissiondeadline        AS SubmissionDeadline,
            m.pmr_submissiondeadlinenote    AS SubmissionDeadlineNote,
            m.pmr_closeoutdocs              AS CloseoutDocs,
            m.pmr_submissiondestination     AS SubmissionDestination,
            m.pmr_ivrrequired               AS IvrRequired,
            m.pmr_contactname               AS ContactName,
            m.pmr_contactemail              AS ContactEmail,
            m.pmr_notifyemail               AS NotifyEmail
        FROM dbo.PMRule m
        LEFT JOIN dbo.PMBillingMode bm ON bm.pmbm_id = m.pmbm_id";

    private const string TierSelect = @"
        SELECT
            rt.pmrt_id                  AS PmrtId,
            rt.pmr_id                   AS PmrId,
            rt.asc_id                   AS AscId,
            LTRIM(RTRIM(ac.asc_category)) AS EquipmentType,
            rt.pmrt_label               AS Label,
            rt.pmrt_mintons             AS MinTons,
            rt.pmrt_maxtons             AS MaxTons,
            rt.pmrt_firstunitprice      AS FirstUnitPrice,
            rt.pmrt_additionalunitprice AS AdditionalUnitPrice,
            rt.pmrt_addon               AS AddOn,
            rt.pmrt_addonlabel          AS AddOnLabel,
            rt.pmrt_order               AS [Order],
            rt.pmrt_active              AS Active
        FROM dbo.PMRateTier rt
        LEFT JOIN dbo.AssetCategory ac ON ac.asc_id = rt.asc_id";

    private const string SeasonSelect = @"
        SELECT
            s.pms_id            AS PmsId,
            s.pmr_id            AS PmrId,
            s.pms_name          AS Name,
            s.t_id_season       AS TIdSeason,
            t.t_trade           AS SeasonTradeName,
            s.pms_visittype     AS VisitType,
            s.pms_startmonthday AS StartMonthDay,
            s.pms_endmonthday   AS EndMonthDay,
            s.pms_visitsperyear AS VisitsPerYear,
            s.pms_intervaldays  AS IntervalDays,
            s.pms_order         AS [Order],
            s.pms_active        AS Active
        FROM dbo.PMSeason s
        LEFT JOIN dbo.Trade t ON t.t_id = s.t_id_season";

    private const string OverrideSelect = @"
        SELECT
            x.xfrq_id                  AS XfrqId,
            x.fr_id                    AS FrId,
            x.fq_id                    AS FqId,
            x.xfrq_enabled             AS Enabled,
            x.xfrq_requirementoverride AS RequirementOverride,
            x.xfrq_questionoverride    AS QuestionOverride,
            x.xfrq_photooverride       AS PhotoOverride
        FROM dbo.xrefFormRuleQuestion x";

    public FormRepository(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("DefaultConnection")
            ?? throw new ArgumentNullException(nameof(configuration), "Connection string not found");
    }

    #region lookups

    public async Task<bool> IsFeatureEnabledAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        var value = await connection.ExecuteScalarAsync<string?>(
            "SELECT TOP 1 cs_value FROM dbo.ConfigSetting WHERE cs_type = 'featureflag' AND cs_identifier = 'PreventativeMaintenance'");
        var v = value?.Trim();
        return string.Equals(v, "1") || string.Equals(v, "true", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Parent trades that take part in PM: those with an active sub-trade named "PM - ..." (PM - HVAC,
    /// PM - Spring, PM - Electrical ...) plus any parent that already owns a template. Templated trades
    /// sort first, then by name.
    /// </summary>
    public async Task<List<FormParentTradeDto>> GetParentTradesAsync()
    {
        const string sql = @"
            SELECT
                t.t_id    AS TId,
                t.t_trade AS Trade,
                (SELECT TOP 1 p.ft_id      FROM dbo.FormTemplate p WHERE p.t_id = t.t_id AND p.ft_active = 1 ORDER BY p.ft_version DESC) AS ActiveTemplateId,
                (SELECT TOP 1 p.ft_version FROM dbo.FormTemplate p WHERE p.t_id = t.t_id AND p.ft_active = 1 ORDER BY p.ft_version DESC) AS ActiveVersion,
                (SELECT COUNT(*) FROM dbo.FormTemplate p WHERE p.t_id = t.t_id) AS TemplateCount
            FROM dbo.Trade t
            WHERE t.t_id_parent IS NULL AND t.t_active = 1
              AND (EXISTS (SELECT 1 FROM dbo.Trade c WHERE c.t_id_parent = t.t_id AND c.t_active = 1 AND c.t_trade LIKE 'PM -%')
                   OR EXISTS (SELECT 1 FROM dbo.FormTemplate p WHERE p.t_id = t.t_id))
            ORDER BY CASE WHEN EXISTS (SELECT 1 FROM dbo.FormTemplate p WHERE p.t_id = t.t_id) THEN 0 ELSE 1 END, t.t_trade;

            SELECT c.t_id_parent AS ParentTId, c.t_trade AS Trade
            FROM dbo.Trade c
            WHERE c.t_id_parent IS NOT NULL AND c.t_active = 1 AND c.t_trade LIKE 'PM -%'
            ORDER BY c.t_trade;";
        using var connection = new SqlConnection(_connectionString);
        using var multi = await connection.QueryMultipleAsync(sql);
        var parents = (await multi.ReadAsync<FormParentTradeDto>()).ToList();
        var subTrades = (await multi.ReadAsync<(int ParentTId, string Trade)>()).ToList();
        foreach (var parent in parents)
            parent.PmTrades = subTrades.Where(s => s.ParentTId == parent.TId).Select(s => s.Trade).ToList();
        return parents;
    }

    public async Task<List<CheckListAnswerTypeDto>> GetAnswerTypesAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<CheckListAnswerTypeDto>(
            "SELECT clat_id AS ClatId, clat_type AS ClatType FROM dbo.CheckListAnswerType ORDER BY clat_id")).ToList();
    }

    public async Task<List<PmBillingModeDto>> GetBillingModesAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<PmBillingModeDto>(
            "SELECT pmbm_id AS PmbmId, pmbm_mode AS Mode, pmbm_description AS Description, pmbm_order AS [Order] FROM dbo.PMBillingMode ORDER BY pmbm_order, pmbm_id")).ToList();
    }

    public async Task<List<FormTradeOptionDto>> GetPmSubTradesAsync(int parentTId)
    {
        var sql = $"SELECT t.t_id AS TId, t.t_trade AS Trade FROM dbo.Trade t WHERE t.t_id_parent = @TId AND t.t_active = 1 AND {PmTradeNamePredicate} ORDER BY t.t_trade";
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<FormTradeOptionDto>(sql, new { TId = parentTId })).ToList();
    }

    public async Task<List<FormAssetCategoryDto>> GetAssetCategoriesAsync(int tId)
    {
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<FormAssetCategoryDto>(
            "SELECT asc_id AS AscId, LTRIM(RTRIM(asc_category)) AS Category FROM dbo.AssetCategory WHERE t_id = @TId ORDER BY asc_category", new { TId = tId })).ToList();
    }

    #endregion

    #region templates

    public async Task<List<FormTemplateDto>> GetTemplatesAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<FormTemplateDto>(TemplateSelect + " ORDER BY t.t_trade, p.ft_version DESC")).ToList();
    }

    public async Task<FormTemplateDetailDto?> GetTemplateAsync(int ftId)
    {
        var sql = TemplateSelect + " WHERE p.ft_id = @FtId;"
                + SectionSelect + " WHERE s.ft_id = @FtId ORDER BY s.fs_order, s.fs_id;"
                + QuestionSelect + " WHERE s.ft_id = @FtId ORDER BY s.fs_order, q.fq_order, q.fq_id;";
        using var connection = new SqlConnection(_connectionString);
        using var multi = await connection.QueryMultipleAsync(sql, new { FtId = ftId });
        var template = await multi.ReadFirstOrDefaultAsync<FormTemplateDetailDto>();
        if (template == null) return null;
        var sections = (await multi.ReadAsync<FormSectionDto>()).ToList();
        var questions = (await multi.ReadAsync<FormQuestionDto>()).ToList();
        var bySection = sections.ToDictionary(s => s.FsId);
        foreach (var q in questions)
            if (bySection.TryGetValue(q.FsId, out var section)) section.Questions.Add(q);
        template.Sections = sections;
        return template;
    }

    public async Task<int> GetNextVersionAsync(int tId)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int>("SELECT ISNULL(MAX(ft_version), 0) + 1 FROM dbo.FormTemplate WHERE t_id = @TId", new { TId = tId });
    }

    public async Task<int> CreateTemplateAsync(SaveFormTemplateRequest request, int version)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();
        if (request.Active)
            await connection.ExecuteAsync("UPDATE dbo.FormTemplate SET ft_active = 0, ft_modifieddatetime = GETDATE() WHERE t_id = @TId AND ft_active = 1", new { request.TId }, transaction);
        var id = await connection.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.FormTemplate (t_id, ft_name, ft_version, ft_active, ft_note)
            VALUES (@TId, @Name, @Version, @Active, @Note);
            SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { request.TId, Name = request.Name.Trim(), Version = version, request.Active, request.Note }, transaction);
        transaction.Commit();
        return id;
    }

    public async Task<bool> UpdateTemplateAsync(int ftId, SaveFormTemplateRequest request)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();
        if (request.Active)
            await connection.ExecuteAsync("UPDATE dbo.FormTemplate SET ft_active = 0, ft_modifieddatetime = GETDATE() WHERE t_id = (SELECT t_id FROM dbo.FormTemplate WHERE ft_id = @FtId) AND ft_id <> @FtId AND ft_active = 1",
                new { FtId = ftId }, transaction);
        var rows = await connection.ExecuteAsync(@"
            UPDATE dbo.FormTemplate
               SET ft_name = @Name, ft_note = @Note, ft_active = @Active, ft_modifieddatetime = GETDATE()
             WHERE ft_id = @FtId",
            new { FtId = ftId, Name = request.Name.Trim(), request.Note, request.Active }, transaction);
        transaction.Commit();
        return rows > 0;
    }

    public async Task<int> CloneTemplateAsync(int ftId, string? name)
    {
        var source = await GetTemplateAsync(ftId) ?? throw new InvalidOperationException($"Template {ftId} not found");
        var version = await GetNextVersionAsync(source.TId);

        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        var newId = await connection.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.FormTemplate (t_id, ft_name, ft_version, ft_active, ft_note)
            VALUES (@TId, @Name, @Version, 0, @Note);
            SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { source.TId, Name = string.IsNullOrWhiteSpace(name) ? source.Name : name.Trim(), Version = version, Note = $"Cloned from v{source.Version} on {DateTime.Now:yyyy-MM-dd}" },
            transaction);

        const string insertSection = @"
            INSERT INTO dbo.FormSection (ft_id, fs_name, fs_phase, fs_order, fs_repeatperunit, fs_condition, fs_active)
            VALUES (@FtId, @Name, @Phase, @Order, @RepeatPerUnit, @Condition, @Active);
            SELECT CAST(SCOPE_IDENTITY() AS INT);";
        const string insertQuestion = @"
            INSERT INTO dbo.FormQuestion (fs_id, fq_code, clat_id, fq_question, fq_order, fq_requirement, fq_condition, fal_id, fq_answervalues,
                fq_datatype, fq_unit, fq_min, fq_max, fq_repeatkey, fq_calcformula, fq_writesto, fq_photorequired, fq_phototiming, fq_linkedcode,
                fq_estminutes, fq_seasons, fq_skip_answer, fq_skip_to_order, fq_triggernote, fq_active)
            VALUES (@FsId, @Code, @ClatId, @Question, @Order, @Requirement, @Condition, @FalId, @AnswerValues,
                @DataType, @Unit, @Min, @Max, @RepeatKey, @CalcFormula, @WritesTo, @PhotoRequired, @PhotoTiming, @LinkedCode,
                @EstMinutes, @Seasons, @SkipAnswer, @SkipToOrder, @TriggerNote, @Active)";

        foreach (var s in source.Sections)
        {
            var newSectionId = await connection.ExecuteScalarAsync<int>(insertSection,
                new { FtId = newId, s.Name, s.Phase, s.Order, s.RepeatPerUnit, s.Condition, s.Active }, transaction);
            foreach (var q in s.Questions)
            {
                await connection.ExecuteAsync(insertQuestion, new
                {
                    FsId = newSectionId, q.Code, q.ClatId, q.Question, q.Order, q.Requirement, q.Condition, q.FalId, q.AnswerValues,
                    q.DataType, q.Unit, q.Min, q.Max, q.RepeatKey, q.CalcFormula, q.WritesTo, q.PhotoRequired, q.PhotoTiming, q.LinkedCode,
                    q.EstMinutes, q.Seasons, q.SkipAnswer, q.SkipToOrder, q.TriggerNote, q.Active
                }, transaction);
            }
        }

        transaction.Commit();
        return newId;
    }

    #endregion

    #region sections

    public async Task<FormSectionDto?> GetSectionAsync(int fsId)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.QueryFirstOrDefaultAsync<FormSectionDto>(SectionSelect + " WHERE s.fs_id = @FsId", new { FsId = fsId });
    }

    public async Task<int> CreateSectionAsync(int ftId, SaveFormSectionRequest request)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.FormSection (ft_id, fs_name, fs_phase, fs_order, fs_repeatperunit, fs_condition, fs_active)
            VALUES (@FtId, @Name, @Phase, @Order, @RepeatPerUnit, @Condition, @Active);
            SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { FtId = ftId, Name = request.Name.Trim(), request.Phase, request.Order, request.RepeatPerUnit, Condition = NullIfBlank(request.Condition), request.Active });
    }

    public async Task<bool> UpdateSectionAsync(int fsId, SaveFormSectionRequest request)
    {
        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.ExecuteAsync(@"
            UPDATE dbo.FormSection
               SET fs_name = @Name, fs_phase = @Phase, fs_order = @Order, fs_repeatperunit = @RepeatPerUnit,
                   fs_condition = @Condition, fs_active = @Active, fs_modifieddatetime = GETDATE()
             WHERE fs_id = @FsId",
            new { FsId = fsId, Name = request.Name.Trim(), request.Phase, request.Order, request.RepeatPerUnit, Condition = NullIfBlank(request.Condition), request.Active });
        return rows > 0;
    }

    public async Task<FormDeleteResult> DeleteSectionAsync(int fsId)
    {
        using var connection = new SqlConnection(_connectionString);
        var exists = await connection.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM dbo.FormSection WHERE fs_id = @FsId", new { FsId = fsId });
        if (exists == 0) return FormDeleteResult.NotFound;
        var questions = await connection.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM dbo.FormQuestion WHERE fs_id = @FsId", new { FsId = fsId });
        if (questions > 0) return FormDeleteResult.Referenced;
        await connection.ExecuteAsync("DELETE FROM dbo.FormSection WHERE fs_id = @FsId", new { FsId = fsId });
        return FormDeleteResult.Deleted;
    }

    #endregion

    #region questions

    public async Task<FormQuestionDto?> GetQuestionAsync(int fqId)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.QueryFirstOrDefaultAsync<FormQuestionDto>(QuestionSelect + " WHERE q.fq_id = @FqId", new { FqId = fqId });
    }

    public async Task<bool> CodeExistsInTemplateAsync(int ftId, string code, int? excludeFqId)
    {
        using var connection = new SqlConnection(_connectionString);
        var n = await connection.ExecuteScalarAsync<int>(@"
            SELECT COUNT(*) FROM dbo.FormQuestion q
            JOIN dbo.FormSection s ON s.fs_id = q.fs_id
            WHERE s.ft_id = @FtId AND q.fq_code = @Code AND (@Exclude IS NULL OR q.fq_id <> @Exclude)",
            new { FtId = ftId, Code = code.Trim(), Exclude = excludeFqId });
        return n > 0;
    }

    private static object QuestionParams(int fsId, SaveFormQuestionRequest r, int? fqId = null) => new
    {
        FqId = fqId,
        FsId = fsId,
        Code = r.Code.Trim(),
        r.ClatId,
        Question = r.Question.Trim(),
        r.Order,
        r.Requirement,
        Condition = NullIfBlank(r.Condition),
        r.FalId,
        AnswerValues = NullIfBlank(r.AnswerValues),
        DataType = NullIfBlank(r.DataType),
        Unit = NullIfBlank(r.Unit),
        r.Min,
        r.Max,
        RepeatKey = NullIfBlank(r.RepeatKey),
        CalcFormula = NullIfBlank(r.CalcFormula),
        WritesTo = string.IsNullOrWhiteSpace(r.WritesTo) ? "Visit" : r.WritesTo,
        PhotoRequired = string.IsNullOrWhiteSpace(r.PhotoRequired) ? "No" : r.PhotoRequired,
        PhotoTiming = NullIfBlank(r.PhotoTiming),
        LinkedCode = NullIfBlank(r.LinkedCode),
        r.EstMinutes,
        Seasons = NullIfBlank(r.Seasons),
        SkipAnswer = NullIfBlank(r.SkipAnswer),
        r.SkipToOrder,
        TriggerNote = NullIfBlank(r.TriggerNote),
        r.Active
    };

    public async Task<int> CreateQuestionAsync(int fsId, SaveFormQuestionRequest request)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.FormQuestion (fs_id, fq_code, clat_id, fq_question, fq_order, fq_requirement, fq_condition, fal_id, fq_answervalues,
                fq_datatype, fq_unit, fq_min, fq_max, fq_repeatkey, fq_calcformula, fq_writesto, fq_photorequired, fq_phototiming, fq_linkedcode,
                fq_estminutes, fq_seasons, fq_skip_answer, fq_skip_to_order, fq_triggernote, fq_active)
            VALUES (@FsId, @Code, @ClatId, @Question, @Order, @Requirement, @Condition, @FalId, @AnswerValues,
                @DataType, @Unit, @Min, @Max, @RepeatKey, @CalcFormula, @WritesTo, @PhotoRequired, @PhotoTiming, @LinkedCode,
                @EstMinutes, @Seasons, @SkipAnswer, @SkipToOrder, @TriggerNote, @Active);
            SELECT CAST(SCOPE_IDENTITY() AS INT);", QuestionParams(fsId, request));
    }

    public async Task<bool> UpdateQuestionAsync(int fqId, SaveFormQuestionRequest request)
    {
        using var connection = new SqlConnection(_connectionString);
        var currentSection = await connection.ExecuteScalarAsync<int?>("SELECT fs_id FROM dbo.FormQuestion WHERE fq_id = @FqId", new { FqId = fqId });
        if (currentSection == null) return false;
        var rows = await connection.ExecuteAsync(@"
            UPDATE dbo.FormQuestion
               SET fs_id = @FsId, fq_code = @Code, clat_id = @ClatId, fq_question = @Question, fq_order = @Order, fq_requirement = @Requirement,
                   fq_condition = @Condition, fal_id = @FalId, fq_answervalues = @AnswerValues, fq_datatype = @DataType, fq_unit = @Unit,
                   fq_min = @Min, fq_max = @Max, fq_repeatkey = @RepeatKey, fq_calcformula = @CalcFormula, fq_writesto = @WritesTo,
                   fq_photorequired = @PhotoRequired, fq_phototiming = @PhotoTiming, fq_linkedcode = @LinkedCode, fq_estminutes = @EstMinutes,
                   fq_seasons = @Seasons, fq_skip_answer = @SkipAnswer, fq_skip_to_order = @SkipToOrder, fq_triggernote = @TriggerNote,
                   fq_active = @Active, fq_modifieddatetime = GETDATE()
             WHERE fq_id = @FqId", QuestionParams(request.FsId ?? currentSection.Value, request, fqId));
        return rows > 0;
    }

    public async Task<FormDeleteResult> DeleteQuestionAsync(int fqId)
    {
        using var connection = new SqlConnection(_connectionString);
        var exists = await connection.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM dbo.FormQuestion WHERE fq_id = @FqId", new { FqId = fqId });
        if (exists == 0) return FormDeleteResult.NotFound;
        var refs = await connection.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM dbo.xrefFormRuleQuestion WHERE fq_id = @FqId", new { FqId = fqId });
        if (refs > 0) return FormDeleteResult.Referenced;
        await connection.ExecuteAsync("DELETE FROM dbo.FormQuestion WHERE fq_id = @FqId", new { FqId = fqId });
        return FormDeleteResult.Deleted;
    }

    #endregion

    #region answer lists

    public async Task<List<FormAnswerListDto>> GetAnswerListsAsync()
    {
        using var connection = new SqlConnection(_connectionString);
        return (await connection.QueryAsync<FormAnswerListDto>(@"
            SELECT
                al.fal_id         AS FalId,
                al.fal_name       AS Name,
                al.fal_values     AS [Values],
                al.fal_failvalues AS FailValues,
                al.fal_active     AS Active,
                (SELECT COUNT(*) FROM dbo.FormQuestion q WHERE q.fal_id = al.fal_id) AS QuestionCount
            FROM dbo.FormAnswerList al
            ORDER BY al.fal_name")).ToList();
    }

    public async Task<int> CreateAnswerListAsync(SaveFormAnswerListRequest request)
    {
        using var connection = new SqlConnection(_connectionString);
        return await connection.ExecuteScalarAsync<int>(@"
            INSERT INTO dbo.FormAnswerList (fal_name, fal_values, fal_failvalues, fal_active)
            VALUES (@Name, @Values, @FailValues, @Active);
            SELECT CAST(SCOPE_IDENTITY() AS INT);",
            new { Name = request.Name.Trim(), Values = request.Values.Trim(), FailValues = NullIfBlank(request.FailValues), request.Active });
    }

    public async Task<bool> UpdateAnswerListAsync(int falId, SaveFormAnswerListRequest request)
    {
        using var connection = new SqlConnection(_connectionString);
        var rows = await connection.ExecuteAsync(@"
            UPDATE dbo.FormAnswerList
               SET fal_name = @Name, fal_values = @Values, fal_failvalues = @FailValues, fal_active = @Active, fal_modifieddatetime = GETDATE()
             WHERE fal_id = @FalId",
            new { FalId = falId, Name = request.Name.Trim(), Values = request.Values.Trim(), FailValues = NullIfBlank(request.FailValues), request.Active });
        return rows > 0;
    }

    #endregion

    #region rules

    public async Task<List<FormCompanyTradeDto>> GetCompanyFormTradesAsync(int xcccId)
    {
        var sql = $@"
            SELECT tp.t_id AS TId, tp.t_trade AS Trade, t.t_trade AS PmTrade
            FROM dbo.LaborRate lr
            JOIN dbo.Trade t ON t.t_id = lr.t_id
            JOIN dbo.Trade tp ON tp.t_id = t.t_id_parent
            WHERE lr.xccc_id = @XcccId AND {PmTradeNamePredicate}
            ORDER BY tp.t_trade, t.t_trade;

            SELECT r.fr_id AS FrId, r.t_id AS TId, t.t_trade AS Trade
            FROM dbo.FormRule r
            JOIN dbo.Trade t ON t.t_id = r.t_id
            WHERE r.xccc_id = @XcccId;

            SELECT t.t_id AS TId, p.ft_id AS ActiveTemplateId
            FROM dbo.Trade t
            JOIN dbo.FormTemplate p ON p.t_id = t.t_id AND p.ft_active = 1
            WHERE t.t_id_parent IS NULL;";

        using var connection = new SqlConnection(_connectionString);
        using var multi = await connection.QueryMultipleAsync(sql, new { XcccId = xcccId });
        var assigned = (await multi.ReadAsync<(int TId, string Trade, string PmTrade)>()).ToList();
        var rules = (await multi.ReadAsync<(int FrId, int TId, string Trade)>()).ToList();
        var active = (await multi.ReadAsync<(int TId, int ActiveTemplateId)>()).ToDictionary(x => x.TId, x => x.ActiveTemplateId);

        var result = new Dictionary<int, FormCompanyTradeDto>();
        foreach (var a in assigned)
        {
            if (!result.TryGetValue(a.TId, out var dto))
                result[a.TId] = dto = new FormCompanyTradeDto { TId = a.TId, Trade = a.Trade };
            dto.PmTrades.Add(a.PmTrade);
        }
        foreach (var r in rules)
        {
            if (!result.TryGetValue(r.TId, out var dto))
                result[r.TId] = dto = new FormCompanyTradeDto { TId = r.TId, Trade = r.Trade };
            dto.HasRule = true;
            dto.FrId = r.FrId;
        }
        foreach (var dto in result.Values)
            if (active.TryGetValue(dto.TId, out var ftId)) dto.ActiveTemplateId = ftId;
        return result.Values.OrderBy(d => d.Trade).ToList();
    }

    public async Task<FormRuleDto?> GetRuleAsync(int xcccId, int tId)
    {
        const string ruleIdSub = "(SELECT fr_id FROM dbo.FormRule WHERE xccc_id = @XcccId AND t_id = @TId)";
        const string pmIdSub = "(SELECT pmr_id FROM dbo.PMRule WHERE fr_id = " + ruleIdSub + ")";
        var sql = RuleSelect + " WHERE r.xccc_id = @XcccId AND r.t_id = @TId;"
                + PmRuleSelect + " WHERE m.fr_id = " + ruleIdSub + ";"
                + TierSelect + " WHERE rt.pmr_id = " + pmIdSub + " ORDER BY rt.pmrt_order, rt.pmrt_id;"
                + SeasonSelect + " WHERE s.pmr_id = " + pmIdSub + " ORDER BY s.pms_order, s.pms_id;"
                + OverrideSelect + " WHERE x.fr_id = " + ruleIdSub + ";";
        using var connection = new SqlConnection(_connectionString);
        using var multi = await connection.QueryMultipleAsync(sql, new { XcccId = xcccId, TId = tId });
        var rule = await multi.ReadFirstOrDefaultAsync<FormRuleDto>();
        if (rule == null) return null;
        rule.Pm = await multi.ReadFirstOrDefaultAsync<PmRuleDto>();
        rule.Tiers = (await multi.ReadAsync<PmRateTierDto>()).ToList();
        rule.Seasons = (await multi.ReadAsync<PmSeasonDto>()).ToList();
        rule.QuestionOverrides = (await multi.ReadAsync<FormRuleQuestionDto>()).ToList();
        return rule;
    }

    public async Task<int> UpsertRuleAsync(int xcccId, int tId, SaveFormRuleRequest request)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();

        var frId = await connection.ExecuteScalarAsync<int?>("SELECT fr_id FROM dbo.FormRule WHERE xccc_id = @XcccId AND t_id = @TId", new { XcccId = xcccId, TId = tId }, transaction);
        if (frId.HasValue)
        {
            await connection.ExecuteAsync(@"
                UPDATE dbo.FormRule SET ft_id = @FtId, fr_active = @Active, fr_note = @Note, fr_modifieddatetime = GETDATE() WHERE fr_id = @FrId",
                new { FrId = frId.Value, request.FtId, request.Active, Note = NullIfBlank(request.Note) }, transaction);
        }
        else
        {
            frId = await connection.ExecuteScalarAsync<int>(@"
                INSERT INTO dbo.FormRule (xccc_id, t_id, ft_id, fr_active, fr_note) VALUES (@XcccId, @TId, @FtId, @Active, @Note);
                SELECT CAST(SCOPE_IDENTITY() AS INT);",
                new { XcccId = xcccId, TId = tId, request.FtId, request.Active, Note = NullIfBlank(request.Note) }, transaction);
        }

        if (request.Pm != null)
        {
            var r = request.Pm;
            var p = new
            {
                FrId = frId.Value,
                r.PmbmId, NteGuideline = NullIfBlank(r.NteGuideline), FirstTimeRule = NullIfBlank(r.FirstTimeRule),
                r.HourlyRate, r.HourCapPerUnit, r.HourCapPerVisit, r.TripCharge, r.AttIdPricingContract, PricingNote = NullIfBlank(r.PricingNote),
                r.FiltersIncluded, r.NoFilterChange, r.FreeFilters, r.FreeBeltChangesPerYear, r.BeltsChargeable,
                CustomerForm = NullIfBlank(r.CustomerForm), CustomerFormUrl = NullIfBlank(r.CustomerFormUrl), r.AttIdCustomerForm, CustomerFormNote = NullIfBlank(r.CustomerFormNote),
                r.CoolingSetpoint, r.HeatingSetpoint, ThermostatSchedule = NullIfBlank(r.ThermostatSchedule), ThermostatScheduleNote = NullIfBlank(r.ThermostatScheduleNote),
                r.ThermostatLock, r.AntiAlgae, r.PhotoTimestamp, r.ManagerSeesOldFilters, r.PricingDiscussAllowed, r.ImmediateQuoteRequired, r.ImmediateCallIfIncomplete,
                SubmissionDeadline = NullIfBlank(r.SubmissionDeadline), SubmissionDeadlineNote = NullIfBlank(r.SubmissionDeadlineNote), CloseoutDocs = NullIfBlank(r.CloseoutDocs),
                SubmissionDestination = NullIfBlank(r.SubmissionDestination), r.IvrRequired,
                ContactName = NullIfBlank(r.ContactName), ContactEmail = NullIfBlank(r.ContactEmail), NotifyEmail = NullIfBlank(r.NotifyEmail)
            };
            var pmrId = await connection.ExecuteScalarAsync<int?>("SELECT pmr_id FROM dbo.PMRule WHERE fr_id = @FrId", new { FrId = frId.Value }, transaction);
            if (pmrId.HasValue)
            {
                await connection.ExecuteAsync(@"
                    UPDATE dbo.PMRule SET
                        pmbm_id = @PmbmId, pmr_nteguideline = @NteGuideline, pmr_firsttimerule = @FirstTimeRule,
                        pmr_hourlyrate = @HourlyRate, pmr_hourcapperunit = @HourCapPerUnit, pmr_hourcappervisit = @HourCapPerVisit, pmr_tripcharge = @TripCharge,
                        att_id_pricingcontract = @AttIdPricingContract, pmr_pricingnote = @PricingNote,
                        pmr_filtersincluded = @FiltersIncluded, pmr_nofilterchange = @NoFilterChange, pmr_freefilters = @FreeFilters,
                        pmr_freebeltchangesperyear = @FreeBeltChangesPerYear, pmr_beltschargeable = @BeltsChargeable,
                        pmr_customerform = @CustomerForm, pmr_customerformurl = @CustomerFormUrl, att_id_customerform = @AttIdCustomerForm, pmr_customerformnote = @CustomerFormNote,
                        pmr_coolingsetpoint = @CoolingSetpoint, pmr_heatingsetpoint = @HeatingSetpoint, pmr_thermostatschedule = @ThermostatSchedule,
                        pmr_thermostatschedulenote = @ThermostatScheduleNote, pmr_thermostatlock = @ThermostatLock, pmr_antialgae = @AntiAlgae,
                        pmr_phototimestamp = @PhotoTimestamp, pmr_managerseesoldfilters = @ManagerSeesOldFilters, pmr_pricingdiscussallowed = @PricingDiscussAllowed,
                        pmr_immediatequoterequired = @ImmediateQuoteRequired, pmr_immediatecallifincomplete = @ImmediateCallIfIncomplete,
                        pmr_submissiondeadline = @SubmissionDeadline, pmr_submissiondeadlinenote = @SubmissionDeadlineNote, pmr_closeoutdocs = @CloseoutDocs,
                        pmr_submissiondestination = @SubmissionDestination, pmr_ivrrequired = @IvrRequired,
                        pmr_contactname = @ContactName, pmr_contactemail = @ContactEmail, pmr_notifyemail = @NotifyEmail,
                        pmr_modifieddatetime = GETDATE()
                    WHERE fr_id = @FrId", p, transaction);
            }
            else
            {
                await connection.ExecuteAsync(@"
                    INSERT INTO dbo.PMRule (fr_id,
                        pmbm_id, pmr_nteguideline, pmr_firsttimerule, pmr_hourlyrate, pmr_hourcapperunit, pmr_hourcappervisit, pmr_tripcharge, att_id_pricingcontract, pmr_pricingnote,
                        pmr_filtersincluded, pmr_nofilterchange, pmr_freefilters, pmr_freebeltchangesperyear, pmr_beltschargeable,
                        pmr_customerform, pmr_customerformurl, att_id_customerform, pmr_customerformnote,
                        pmr_coolingsetpoint, pmr_heatingsetpoint, pmr_thermostatschedule, pmr_thermostatschedulenote, pmr_thermostatlock, pmr_antialgae,
                        pmr_phototimestamp, pmr_managerseesoldfilters, pmr_pricingdiscussallowed, pmr_immediatequoterequired, pmr_immediatecallifincomplete,
                        pmr_submissiondeadline, pmr_submissiondeadlinenote, pmr_closeoutdocs, pmr_submissiondestination, pmr_ivrrequired,
                        pmr_contactname, pmr_contactemail, pmr_notifyemail)
                    VALUES (@FrId,
                        @PmbmId, @NteGuideline, @FirstTimeRule, @HourlyRate, @HourCapPerUnit, @HourCapPerVisit, @TripCharge, @AttIdPricingContract, @PricingNote,
                        @FiltersIncluded, @NoFilterChange, @FreeFilters, @FreeBeltChangesPerYear, @BeltsChargeable,
                        @CustomerForm, @CustomerFormUrl, @AttIdCustomerForm, @CustomerFormNote,
                        @CoolingSetpoint, @HeatingSetpoint, @ThermostatSchedule, @ThermostatScheduleNote, @ThermostatLock, @AntiAlgae,
                        @PhotoTimestamp, @ManagerSeesOldFilters, @PricingDiscussAllowed, @ImmediateQuoteRequired, @ImmediateCallIfIncomplete,
                        @SubmissionDeadline, @SubmissionDeadlineNote, @CloseoutDocs, @SubmissionDestination, @IvrRequired,
                        @ContactName, @ContactEmail, @NotifyEmail)", p, transaction);
            }
        }

        transaction.Commit();
        return frId.Value;
    }

    /// <summary>Tiers and seasons hang off the PMRule row; create an empty one if the rule has none yet.</summary>
    private static async Task<int> EnsurePmRuleAsync(SqlConnection connection, SqlTransaction transaction, int frId)
    {
        var pmrId = await connection.ExecuteScalarAsync<int?>("SELECT pmr_id FROM dbo.PMRule WHERE fr_id = @FrId", new { FrId = frId }, transaction);
        if (pmrId.HasValue) return pmrId.Value;
        return await connection.ExecuteScalarAsync<int>("INSERT INTO dbo.PMRule (fr_id) VALUES (@FrId); SELECT CAST(SCOPE_IDENTITY() AS INT);", new { FrId = frId }, transaction);
    }

    public async Task<int> ReplaceTiersAsync(int frId, List<PmRateTierDto> tiers)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();
        var pmrId = await EnsurePmRuleAsync(connection, transaction, frId);
        await connection.ExecuteAsync("DELETE FROM dbo.PMRateTier WHERE pmr_id = @PmrId", new { PmrId = pmrId }, transaction);
        var order = 0;
        foreach (var t in tiers)
        {
            order++;
            await connection.ExecuteAsync(@"
                INSERT INTO dbo.PMRateTier (pmr_id, asc_id, pmrt_label, pmrt_mintons, pmrt_maxtons, pmrt_firstunitprice, pmrt_additionalunitprice, pmrt_addon, pmrt_addonlabel, pmrt_order, pmrt_active)
                VALUES (@PmrId, @AscId, @Label, @MinTons, @MaxTons, @FirstUnitPrice, @AdditionalUnitPrice, @AddOn, @AddOnLabel, @Order, @Active)",
                new { PmrId = pmrId, t.AscId, Label = NullIfBlank(t.Label), t.MinTons, t.MaxTons, t.FirstUnitPrice, t.AdditionalUnitPrice, t.AddOn, AddOnLabel = NullIfBlank(t.AddOnLabel), Order = t.Order > 0 ? t.Order : order, t.Active },
                transaction);
        }
        transaction.Commit();
        return tiers.Count;
    }

    public async Task<int> ReplaceSeasonsAsync(int frId, List<PmSeasonDto> seasons)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();
        var pmrId = await EnsurePmRuleAsync(connection, transaction, frId);
        await connection.ExecuteAsync("DELETE FROM dbo.PMSeason WHERE pmr_id = @PmrId", new { PmrId = pmrId }, transaction);
        var order = 0;
        foreach (var s in seasons)
        {
            order++;
            await connection.ExecuteAsync(@"
                INSERT INTO dbo.PMSeason (pmr_id, pms_name, t_id_season, pms_visittype, pms_startmonthday, pms_endmonthday, pms_visitsperyear, pms_intervaldays, pms_order, pms_active)
                VALUES (@PmrId, @Name, @TIdSeason, @VisitType, @StartMonthDay, @EndMonthDay, @VisitsPerYear, @IntervalDays, @Order, @Active)",
                new { PmrId = pmrId, Name = s.Name.Trim(), s.TIdSeason, VisitType = NullIfBlank(s.VisitType), StartMonthDay = NullIfBlank(s.StartMonthDay), EndMonthDay = NullIfBlank(s.EndMonthDay), s.VisitsPerYear, s.IntervalDays, Order = s.Order > 0 ? s.Order : order, s.Active },
                transaction);
        }
        transaction.Commit();
        return seasons.Count;
    }

    public async Task<int> ReplaceQuestionOverridesAsync(int frId, List<FormRuleQuestionDto> overrides)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        using var transaction = connection.BeginTransaction();
        await connection.ExecuteAsync("DELETE FROM dbo.xrefFormRuleQuestion WHERE fr_id = @FrId", new { FrId = frId }, transaction);
        var inserted = 0;
        foreach (var o in overrides.GroupBy(o => o.FqId).Select(g => g.Last()))
        {
            inserted += await connection.ExecuteAsync(@"
                INSERT INTO dbo.xrefFormRuleQuestion (fr_id, fq_id, xfrq_enabled, xfrq_requirementoverride, xfrq_questionoverride, xfrq_photooverride)
                VALUES (@FrId, @FqId, @Enabled, @RequirementOverride, @QuestionOverride, @PhotoOverride)",
                new { FrId = frId, o.FqId, o.Enabled, RequirementOverride = NullIfBlank(o.RequirementOverride), QuestionOverride = NullIfBlank(o.QuestionOverride), PhotoOverride = NullIfBlank(o.PhotoOverride) },
                transaction);
        }
        transaction.Commit();
        return inserted;
    }

    #endregion

    private static string? NullIfBlank(string? s) => string.IsNullOrWhiteSpace(s) ? null : s.Trim();
}
