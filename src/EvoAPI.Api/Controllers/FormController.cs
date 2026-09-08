using System.Diagnostics;
using EvoAPI.Core.Interfaces;
using EvoAPI.Core.Services;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using Microsoft.AspNetCore.Mvc;

namespace EvoAPI.Api.Controllers
{
    /// <summary>
    /// Forms engine (PM build slice 1): form templates (org-level question bank per parent trade),
    /// form rules (which template and questions a company pairing + parent trade uses, with the PM
    /// terms nested under Pm), and the "preview questions for this customer" resolver.
    ///
    /// Everything here is admin configuration, so the class-level policy is AdminOnly and every
    /// action repeats it as documentation. Rule changes are written to the critical audit log
    /// with before/after values, the same way company trade rates are.
    ///
    /// Template routes live under EvoApi/forms; rule routes use the absolute
    /// /EvoApi/companies/{xcccId}/form-rules/... form so they sit next to the other company endpoints.
    /// The engine is gated by the PreventativeMaintenance feature flag while PM is its only consumer.
    /// </summary>
    [ApiController]
    [Route("EvoApi/forms")]
    [AdminOnly]
    public class FormController : BaseController
    {
        private static readonly string[] Phases = { "Site", "Asset", "Unit", "Findings", "Checkout" };
        private static readonly string[] Requirements = { "Always", "Recommended", "Optional", "Conditional", "Configured" };
        private static readonly string[] PhotoRules = { "Always", "CustomerConfigured", "IfIssue", "Recommended", "No" };

        private readonly IFormRepository _repo;
        private readonly IAssetRepository _assets;
        private readonly IAuditCriticalService _auditCriticalService;
        private readonly ILogger<FormController> _logger;

        public FormController(IFormRepository repo, IAssetRepository assets, IAuditService auditService, IAuditCriticalService auditCriticalService, ILogger<FormController> logger)
        {
            _repo = repo;
            _assets = assets;
            _auditCriticalService = auditCriticalService;
            _logger = logger;
            InitializeAuditService(auditService);
        }

        private void SetAuditCriticalUserContext()
        {
            _auditCriticalService.Username = Username;
            _auditCriticalService.UserFullName = UserFullName;
            _auditCriticalService.IPAddress = ClientIPAddress;
            _auditCriticalService.UserAgent = UserAgent;
        }

        private static ActionResult<ApiResponse<T>> Fail<T>(int status, string message) =>
            new ObjectResult(new ApiResponse<T> { Success = false, Message = message }) { StatusCode = status };

        private static ApiResponse<T> Ok<T>(T data, string message, int? count = null) =>
            new() { Success = true, Message = message, Data = data, Count = count ?? (data is System.Collections.ICollection c ? c.Count : 1) };

        #region lookups

        [HttpGet("enabled")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<bool>>> IsEnabled()
        {
            try
            {
                var enabled = await _repo.IsFeatureEnabledAsync();
                return Ok(Ok(enabled, enabled ? "Forms engine is enabled" : "Forms engine is disabled"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormIsEnabled", ex);
                return Fail<bool>(500, "Failed to read the feature flag");
            }
        }

        [HttpGet("parent-trades")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<FormParentTradeDto>>>> GetParentTrades()
        {
            try
            {
                var trades = await _repo.GetParentTradesAsync();
                return Ok(Ok(trades, $"Retrieved {trades.Count} parent trades"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormGetParentTrades", ex);
                return Fail<List<FormParentTradeDto>>(500, "Failed to retrieve parent trades");
            }
        }

        [HttpGet("answer-types")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<CheckListAnswerTypeDto>>>> GetAnswerTypes()
        {
            try
            {
                var types = await _repo.GetAnswerTypesAsync();
                return Ok(Ok(types, $"Retrieved {types.Count} answer types"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormGetAnswerTypes", ex);
                return Fail<List<CheckListAnswerTypeDto>>(500, "Failed to retrieve answer types");
            }
        }

        [HttpGet("pm/billing-modes")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<PmBillingModeDto>>>> GetBillingModes()
        {
            try
            {
                var modes = await _repo.GetBillingModesAsync();
                return Ok(Ok(modes, $"Retrieved {modes.Count} billing modes"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormGetBillingModes", ex);
                return Fail<List<PmBillingModeDto>>(500, "Failed to retrieve billing modes");
            }
        }

        [HttpGet("trades/{tId:int}/sub-trades")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<FormTradeOptionDto>>>> GetPmSubTrades(int tId)
        {
            try
            {
                var trades = await _repo.GetPmSubTradesAsync(tId);
                return Ok(Ok(trades, $"Retrieved {trades.Count} PM sub-trades"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormGetSubTrades", ex);
                return Fail<List<FormTradeOptionDto>>(500, "Failed to retrieve PM sub-trades");
            }
        }

        [HttpGet("trades/{tId:int}/asset-categories")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<FormAssetCategoryDto>>>> GetAssetCategories(int tId)
        {
            try
            {
                var categories = await _repo.GetAssetCategoriesAsync(tId);
                return Ok(Ok(categories, $"Retrieved {categories.Count} asset categories"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormGetAssetCategories", ex);
                return Fail<List<FormAssetCategoryDto>>(500, "Failed to retrieve asset categories");
            }
        }

        #endregion

        #region templates

        [HttpGet("templates")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<FormTemplateDto>>>> GetTemplates()
        {
            try
            {
                var templates = await _repo.GetTemplatesAsync();
                return Ok(Ok(templates, $"Retrieved {templates.Count} templates"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormGetTemplates", ex);
                return Fail<List<FormTemplateDto>>(500, "Failed to retrieve form templates");
            }
        }

        [HttpGet("templates/{ftId:int}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<FormTemplateDetailDto>>> GetTemplate(int ftId)
        {
            try
            {
                var template = await _repo.GetTemplateAsync(ftId);
                if (template == null) return Fail<FormTemplateDetailDto>(404, "Template not found");
                return Ok(Ok(template, $"Retrieved template {template.Name} v{template.Version}"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormGetTemplate", ex, new { ftId });
                return Fail<FormTemplateDetailDto>(500, "Failed to retrieve the form template");
            }
        }

        [HttpPost("templates")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<FormTemplateDetailDto>>> CreateTemplate([FromBody] SaveFormTemplateRequest request)
        {
            try
            {
                if (request == null || request.TId <= 0 || string.IsNullOrWhiteSpace(request.Name))
                    return Fail<FormTemplateDetailDto>(400, "Trade and name are required");
                var version = await _repo.GetNextVersionAsync(request.TId);
                var id = await _repo.CreateTemplateAsync(request, version);
                var template = await _repo.GetTemplateAsync(id);
                await LogAuditAsync("FormCreateTemplate", new { id, request.TId, request.Name, version });
                return Ok(Ok(template!, $"Template created as version {version}"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormCreateTemplate", ex, request);
                return Fail<FormTemplateDetailDto>(500, "Failed to create the form template");
            }
        }

        [HttpPut("templates/{ftId:int}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<FormTemplateDetailDto>>> UpdateTemplate(int ftId, [FromBody] SaveFormTemplateRequest request)
        {
            try
            {
                if (request == null || string.IsNullOrWhiteSpace(request.Name))
                    return Fail<FormTemplateDetailDto>(400, "Name is required");
                var updated = await _repo.UpdateTemplateAsync(ftId, request);
                if (!updated) return Fail<FormTemplateDetailDto>(404, "Template not found");
                var template = await _repo.GetTemplateAsync(ftId);
                await LogAuditAsync("FormUpdateTemplate", new { ftId, request.Name, request.Active });
                return Ok(Ok(template!, "Template updated"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormUpdateTemplate", ex, new { ftId, request });
                return Fail<FormTemplateDetailDto>(500, "Failed to update the form template");
            }
        }

        [HttpPost("templates/{ftId:int}/clone")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<FormTemplateDetailDto>>> CloneTemplate(int ftId, [FromBody] CloneFormTemplateRequest? request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var source = await _repo.GetTemplateAsync(ftId);
                if (source == null) return Fail<FormTemplateDetailDto>(404, "Template not found");
                var newId = await _repo.CloneTemplateAsync(ftId, request?.Name);
                var template = await _repo.GetTemplateAsync(newId);
                await LogAuditAsync("FormCloneTemplate", new { ftId, newId, questions = template?.QuestionCount }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));
                return Ok(Ok(template!, $"Cloned {source.Name} v{source.Version} to v{template!.Version} ({template.QuestionCount} questions). The new version is inactive until you activate it."));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormCloneTemplate", ex, new { ftId });
                return Fail<FormTemplateDetailDto>(500, "Failed to clone the form template");
            }
        }

        #endregion

        #region sections

        [HttpPost("templates/{ftId:int}/sections")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<FormSectionDto>>> CreateSection(int ftId, [FromBody] SaveFormSectionRequest request)
        {
            try
            {
                var error = ValidateSection(request);
                if (error != null) return Fail<FormSectionDto>(400, error);
                if (await _repo.GetTemplateAsync(ftId) == null) return Fail<FormSectionDto>(404, "Template not found");
                var id = await _repo.CreateSectionAsync(ftId, request);
                var section = await _repo.GetSectionAsync(id);
                await LogAuditAsync("FormCreateSection", new { ftId, id, request.Name, request.Phase });
                return Ok(Ok(section!, "Section created"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormCreateSection", ex, new { ftId, request });
                return Fail<FormSectionDto>(500, "Failed to create the section");
            }
        }

        [HttpPut("sections/{fsId:int}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<FormSectionDto>>> UpdateSection(int fsId, [FromBody] SaveFormSectionRequest request)
        {
            try
            {
                var error = ValidateSection(request);
                if (error != null) return Fail<FormSectionDto>(400, error);
                var updated = await _repo.UpdateSectionAsync(fsId, request);
                if (!updated) return Fail<FormSectionDto>(404, "Section not found");
                var section = await _repo.GetSectionAsync(fsId);
                await LogAuditAsync("FormUpdateSection", new { fsId, request.Name, request.Phase, request.Active });
                return Ok(Ok(section!, "Section updated"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormUpdateSection", ex, new { fsId, request });
                return Fail<FormSectionDto>(500, "Failed to update the section");
            }
        }

        [HttpDelete("sections/{fsId:int}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<bool>>> DeleteSection(int fsId)
        {
            try
            {
                var result = await _repo.DeleteSectionAsync(fsId);
                if (result == FormDeleteResult.NotFound) return Fail<bool>(404, "Section not found");
                if (result == FormDeleteResult.Referenced) return Fail<bool>(409, "The section still has questions. Move or delete them first, or mark the section inactive.");
                await LogAuditAsync("FormDeleteSection", new { fsId });
                return Ok(Ok(true, "Section deleted"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormDeleteSection", ex, new { fsId });
                return Fail<bool>(500, "Failed to delete the section");
            }
        }

        private static string? ValidateSection(SaveFormSectionRequest? request)
        {
            if (request == null || string.IsNullOrWhiteSpace(request.Name)) return "Section name is required";
            if (!Phases.Contains(request.Phase)) return $"Phase must be one of {string.Join(", ", Phases)}";
            if (!string.IsNullOrWhiteSpace(request.Condition) && !IsJsonObject(request.Condition)) return "Condition must be a JSON object";
            return null;
        }

        #endregion

        #region questions

        [HttpPost("sections/{fsId:int}/questions")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<FormQuestionDto>>> CreateQuestion(int fsId, [FromBody] SaveFormQuestionRequest request)
        {
            try
            {
                var error = ValidateQuestion(request);
                if (error != null) return Fail<FormQuestionDto>(400, error);
                var section = await _repo.GetSectionAsync(fsId);
                if (section == null) return Fail<FormQuestionDto>(404, "Section not found");
                if (await _repo.CodeExistsInTemplateAsync(section.FtId, request.Code, null))
                    return Fail<FormQuestionDto>(409, $"Code {request.Code.Trim()} is already used in this template");
                var id = await _repo.CreateQuestionAsync(fsId, request);
                var question = await _repo.GetQuestionAsync(id);
                await LogAuditAsync("FormCreateQuestion", new { fsId, id, request.Code, request.Question });
                return Ok(Ok(question!, "Question created"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormCreateQuestion", ex, new { fsId, request });
                return Fail<FormQuestionDto>(500, "Failed to create the question");
            }
        }

        [HttpPut("questions/{fqId:int}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<FormQuestionDto>>> UpdateQuestion(int fqId, [FromBody] SaveFormQuestionRequest request)
        {
            try
            {
                var error = ValidateQuestion(request);
                if (error != null) return Fail<FormQuestionDto>(400, error);
                var existing = await _repo.GetQuestionAsync(fqId);
                if (existing == null) return Fail<FormQuestionDto>(404, "Question not found");
                var section = await _repo.GetSectionAsync(existing.FsId);
                if (request.FsId.HasValue && request.FsId != existing.FsId)
                {
                    var target = await _repo.GetSectionAsync(request.FsId.Value);
                    if (target == null || section == null || target.FtId != section.FtId)
                        return Fail<FormQuestionDto>(400, "A question can only move to a section of the same template");
                }
                if (section != null && await _repo.CodeExistsInTemplateAsync(section.FtId, request.Code, fqId))
                    return Fail<FormQuestionDto>(409, $"Code {request.Code.Trim()} is already used in this template");
                var updated = await _repo.UpdateQuestionAsync(fqId, request);
                if (!updated) return Fail<FormQuestionDto>(404, "Question not found");
                var question = await _repo.GetQuestionAsync(fqId);
                await LogAuditAsync("FormUpdateQuestion", new { fqId, request.Code, request.Question, request.Requirement });
                return Ok(Ok(question!, "Question updated"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormUpdateQuestion", ex, new { fqId, request });
                return Fail<FormQuestionDto>(500, "Failed to update the question");
            }
        }

        [HttpDelete("questions/{fqId:int}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<bool>>> DeleteQuestion(int fqId)
        {
            try
            {
                var result = await _repo.DeleteQuestionAsync(fqId);
                if (result == FormDeleteResult.NotFound) return Fail<bool>(404, "Question not found");
                if (result == FormDeleteResult.Referenced) return Fail<bool>(409, "Form rules reference this question. Mark it inactive instead of deleting it.");
                await LogAuditAsync("FormDeleteQuestion", new { fqId });
                return Ok(Ok(true, "Question deleted"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormDeleteQuestion", ex, new { fqId });
                return Fail<bool>(500, "Failed to delete the question");
            }
        }

        private static string? ValidateQuestion(SaveFormQuestionRequest? request)
        {
            if (request == null) return "Request body is required";
            if (string.IsNullOrWhiteSpace(request.Code)) return "Code is required";
            if (request.Code.Trim().Length > 12) return "Code must be 12 characters or fewer";
            if (string.IsNullOrWhiteSpace(request.Question)) return "Question text is required";
            if (request.ClatId <= 0) return "Answer type is required";
            if (!Requirements.Contains(request.Requirement)) return $"Requirement must be one of {string.Join(", ", Requirements)}";
            if (!PhotoRules.Contains(request.PhotoRequired)) return $"Photo rule must be one of {string.Join(", ", PhotoRules)}";
            if (!string.IsNullOrWhiteSpace(request.Condition) && !IsJsonObject(request.Condition)) return "Condition must be a JSON object";
            if (request.WritesTo != "Visit" && request.WritesTo != "Asset") return "Writes-to must be Visit or Asset";
            return null;
        }

        private static bool IsJsonObject(string json)
        {
            try
            {
                using var doc = System.Text.Json.JsonDocument.Parse(json);
                return doc.RootElement.ValueKind == System.Text.Json.JsonValueKind.Object;
            }
            catch (System.Text.Json.JsonException)
            {
                return false;
            }
        }

        #endregion

        #region answer lists

        [HttpGet("answer-lists")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<FormAnswerListDto>>>> GetAnswerLists()
        {
            try
            {
                var lists = await _repo.GetAnswerListsAsync();
                return Ok(Ok(lists, $"Retrieved {lists.Count} answer lists"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormGetAnswerLists", ex);
                return Fail<List<FormAnswerListDto>>(500, "Failed to retrieve answer lists");
            }
        }

        [HttpPost("answer-lists")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<int>>> CreateAnswerList([FromBody] SaveFormAnswerListRequest request)
        {
            try
            {
                if (request == null || string.IsNullOrWhiteSpace(request.Name) || string.IsNullOrWhiteSpace(request.Values))
                    return Fail<int>(400, "Name and values are required");
                var id = await _repo.CreateAnswerListAsync(request);
                await LogAuditAsync("FormCreateAnswerList", new { id, request.Name });
                return Ok(Ok(id, "Answer list created"));
            }
            catch (System.Data.SqlClient.SqlException ex) when (ex.Number is 2627 or 2601)
            {
                return Fail<int>(409, "An answer list with that name already exists");
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormCreateAnswerList", ex, request);
                return Fail<int>(500, "Failed to create the answer list");
            }
        }

        [HttpPut("answer-lists/{falId:int}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<bool>>> UpdateAnswerList(int falId, [FromBody] SaveFormAnswerListRequest request)
        {
            try
            {
                if (request == null || string.IsNullOrWhiteSpace(request.Name) || string.IsNullOrWhiteSpace(request.Values))
                    return Fail<bool>(400, "Name and values are required");
                var updated = await _repo.UpdateAnswerListAsync(falId, request);
                if (!updated) return Fail<bool>(404, "Answer list not found");
                await LogAuditAsync("FormUpdateAnswerList", new { falId, request.Name });
                return Ok(Ok(true, "Answer list updated"));
            }
            catch (System.Data.SqlClient.SqlException ex) when (ex.Number is 2627 or 2601)
            {
                return Fail<bool>(409, "An answer list with that name already exists");
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormUpdateAnswerList", ex, new { falId, request });
                return Fail<bool>(500, "Failed to update the answer list");
            }
        }

        #endregion

        #region rules

        [HttpGet("/EvoApi/companies/{xcccId:int}/form-trades")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<FormCompanyTradeDto>>>> GetCompanyFormTrades(int xcccId)
        {
            try
            {
                var trades = await _repo.GetCompanyFormTradesAsync(xcccId);
                return Ok(Ok(trades, $"Retrieved {trades.Count} PM parent trades"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormGetCompanyTrades", ex, new { xcccId });
                return Fail<List<FormCompanyTradeDto>>(500, "Failed to retrieve the company's PM trades");
            }
        }

        [HttpGet("/EvoApi/companies/{xcccId:int}/form-rules/{tId:int}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<FormRuleDto>>> GetRule(int xcccId, int tId)
        {
            try
            {
                var rule = await _repo.GetRuleAsync(xcccId, tId);
                if (rule == null) return Fail<FormRuleDto>(404, "No form rule for this company and trade yet");
                return Ok(Ok(rule, "Retrieved form rule"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormGetRule", ex, new { xcccId, tId });
                return Fail<FormRuleDto>(500, "Failed to retrieve the form rule");
            }
        }

        [HttpPut("/EvoApi/companies/{xcccId:int}/form-rules/{tId:int}")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<FormRuleDto>>> SaveRule(int xcccId, int tId, [FromBody] SaveFormRuleRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (request == null) return Fail<FormRuleDto>(400, "Request body is required");
                if (request.Pm?.CoolingSetpoint is < 50 or > 90 || request.Pm?.HeatingSetpoint is < 50 or > 90)
                    return Fail<FormRuleDto>(400, "Setpoints must be between 50 and 90 °F");

                var before = await _repo.GetRuleAsync(xcccId, tId);
                var frId = await _repo.UpsertRuleAsync(xcccId, tId, request);
                var after = await _repo.GetRuleAsync(xcccId, tId);
                stopwatch.Stop();

                var (oldValues, newValues) = Diff(before, request);
                if (request.Pm != null)
                {
                    var (oldPm, newPm) = Diff(before?.Pm, request.Pm);
                    foreach (var kv in oldPm) oldValues["Pm." + kv.Key] = kv.Value;
                    foreach (var kv in newPm) newValues["Pm." + kv.Key] = kv.Value;
                }
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync(
                    $"Form Rule {(before == null ? "Created" : "Updated")} - ID: {frId} - {after?.TradeName} - xccc {xcccId}",
                    oldValues, newValues, stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                await LogAuditAsync("FormSaveRule", new { xcccId, tId, frId, changed = newValues.Keys });

                return Ok(Ok(after!, before == null ? "Form rule created" : "Form rule saved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormSaveRule", ex, new { xcccId, tId, request });
                return Fail<FormRuleDto>(500, "Failed to save the form rule");
            }
        }

        [HttpPut("/EvoApi/companies/{xcccId:int}/form-rules/{tId:int}/tiers")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<PmRateTierDto>>>> SaveTiers(int xcccId, int tId, [FromBody] List<PmRateTierDto> tiers)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var rule = await _repo.GetRuleAsync(xcccId, tId);
                if (rule == null) return Fail<List<PmRateTierDto>>(404, "Save the rule before adding price tiers");
                tiers ??= new List<PmRateTierDto>();
                await _repo.ReplaceTiersAsync(rule.FrId, tiers);
                var after = await _repo.GetRuleAsync(xcccId, tId);
                stopwatch.Stop();
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync(
                    $"Form Rule Tiers Updated - ID: {rule.FrId} - {rule.TradeName} - xccc {xcccId}",
                    new Dictionary<string, object?> { { "Tiers", rule.Tiers } },
                    new Dictionary<string, object?> { { "Tiers", after!.Tiers } },
                    stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                await LogAuditAsync("FormSaveTiers", new { xcccId, tId, count = tiers.Count });
                return Ok(Ok(after.Tiers, $"{after.Tiers.Count} price tier(s) saved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormSaveTiers", ex, new { xcccId, tId });
                return Fail<List<PmRateTierDto>>(500, "Failed to save the price tiers");
            }
        }

        [HttpPut("/EvoApi/companies/{xcccId:int}/form-rules/{tId:int}/seasons")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<PmSeasonDto>>>> SaveSeasons(int xcccId, int tId, [FromBody] List<PmSeasonDto> seasons)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var rule = await _repo.GetRuleAsync(xcccId, tId);
                if (rule == null) return Fail<List<PmSeasonDto>>(404, "Save the rule before adding seasons");
                seasons ??= new List<PmSeasonDto>();
                if (seasons.Any(s => string.IsNullOrWhiteSpace(s.Name))) return Fail<List<PmSeasonDto>>(400, "Every season needs a name");
                await _repo.ReplaceSeasonsAsync(rule.FrId, seasons);
                var after = await _repo.GetRuleAsync(xcccId, tId);
                stopwatch.Stop();
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync(
                    $"Form Rule Seasons Updated - ID: {rule.FrId} - {rule.TradeName} - xccc {xcccId}",
                    new Dictionary<string, object?> { { "Seasons", rule.Seasons } },
                    new Dictionary<string, object?> { { "Seasons", after!.Seasons } },
                    stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                await LogAuditAsync("FormSaveSeasons", new { xcccId, tId, count = seasons.Count });
                return Ok(Ok(after.Seasons, $"{after.Seasons.Count} season(s) saved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormSaveSeasons", ex, new { xcccId, tId });
                return Fail<List<PmSeasonDto>>(500, "Failed to save the seasons");
            }
        }

        [HttpPut("/EvoApi/companies/{xcccId:int}/form-rules/{tId:int}/questions")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<List<FormRuleQuestionDto>>>> SaveQuestionOverrides(int xcccId, int tId, [FromBody] List<FormRuleQuestionDto> overrides)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var rule = await _repo.GetRuleAsync(xcccId, tId);
                if (rule == null) return Fail<List<FormRuleQuestionDto>>(404, "Save the rule before choosing questions");
                overrides ??= new List<FormRuleQuestionDto>();
                await _repo.ReplaceQuestionOverridesAsync(rule.FrId, overrides);
                var after = await _repo.GetRuleAsync(xcccId, tId);
                stopwatch.Stop();
                SetAuditCriticalUserContext();
                await _auditCriticalService.LogChangeAsync(
                    $"Form Rule Questions Updated - ID: {rule.FrId} - {rule.TradeName} - xccc {xcccId}",
                    new Dictionary<string, object?> { { "QuestionOverrides", rule.QuestionOverrides } },
                    new Dictionary<string, object?> { { "QuestionOverrides", after!.QuestionOverrides } },
                    stopwatch.Elapsed.TotalSeconds.ToString("F3"));
                await LogAuditAsync("FormSaveQuestionOverrides", new { xcccId, tId, count = overrides.Count });
                return Ok(Ok(after.QuestionOverrides, $"{after.QuestionOverrides.Count} question override(s) saved"));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormSaveQuestionOverrides", ex, new { xcccId, tId });
                return Fail<List<FormRuleQuestionDto>>(500, "Failed to save the question overrides");
            }
        }

        /// <summary>
        /// Resolved question list for this customer: template ∩ rule ∩ season ∩ equipment / heating type.
        /// Works before a rule exists (template defaults only) so the tab can show what a new customer would get.
        /// </summary>
        [HttpGet("/EvoApi/companies/{xcccId:int}/form-rules/{tId:int}/preview")]
        [AdminOnly]
        public async Task<ActionResult<ApiResponse<FormPreviewDto>>> Preview(int xcccId, int tId, [FromQuery] string? season, [FromQuery] string? equipmentType, [FromQuery] string? heatingType, [FromQuery] int? asId)
        {
            try
            {
                // a real unit supplies equipment type, heating type, mounting, attributes and repeat counts;
                // anything typed into the preview filters still wins over the stored value
                var facts = new FormResolveFacts { Season = season, EquipmentType = equipmentType, HeatingType = heatingType };
                if (asId is > 0)
                {
                    var assetFacts = await _assets.GetAssetFactsAsync(asId.Value);
                    if (assetFacts == null) return Fail<FormPreviewDto>(404, "Asset not found");
                    facts.AsId = assetFacts.AsId;
                    facts.AssetLabel = assetFacts.Label;
                    facts.EquipmentType = string.IsNullOrWhiteSpace(equipmentType) ? assetFacts.EquipmentType : equipmentType;
                    facts.HeatingType = string.IsNullOrWhiteSpace(heatingType) ? assetFacts.HeatingType : heatingType;
                    facts.Mount = assetFacts.Mount;
                    facts.Attributes = assetFacts.Attributes;
                    facts.RepeatCounts = assetFacts.RepeatCounts;
                }

                var rule = await _repo.GetRuleAsync(xcccId, tId);
                var ftId = rule?.FtId;
                if (ftId == null)
                {
                    var trade = (await _repo.GetParentTradesAsync()).FirstOrDefault(t => t.TId == tId);
                    ftId = trade?.ActiveTemplateId;
                }
                if (ftId == null) return Fail<FormPreviewDto>(404, "No form template exists for this trade yet");
                var template = await _repo.GetTemplateAsync(ftId.Value);
                if (template == null) return Fail<FormPreviewDto>(404, "The rule points at a template that no longer exists");

                var preview = FormTemplateResolver.Resolve(template, rule, facts);
                return Ok(Ok(preview, $"{preview.IncludedQuestions} of {preview.TotalQuestions} questions apply", preview.IncludedQuestions));
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("FormPreview", ex, new { xcccId, tId, season, equipmentType, heatingType, asId });
                return Fail<FormPreviewDto>(500, "Failed to build the preview");
            }
        }

        #endregion

        /// <summary>
        /// Property-by-property comparison of the saved object against the incoming request (same
        /// property names on both types), so the critical audit only carries what changed.
        /// Nested objects (Pm) are diffed by the caller.
        /// </summary>
        private static (Dictionary<string, object?> oldValues, Dictionary<string, object?> newValues) Diff(object? before, object request)
        {
            var oldValues = new Dictionary<string, object?>();
            var newValues = new Dictionary<string, object?>();
            foreach (var prop in request.GetType().GetProperties())
            {
                if (prop.PropertyType.IsClass && prop.PropertyType != typeof(string)) continue;   // nested objects diffed separately
                var newVal = prop.GetValue(request);
                object? oldVal = null;
                if (before != null)
                {
                    var op = before.GetType().GetProperty(prop.Name);
                    if (op != null) oldVal = op.GetValue(before);
                }
                if (!Equals(Normalize(oldVal), Normalize(newVal)))
                {
                    oldValues[prop.Name] = oldVal;
                    newValues[prop.Name] = newVal;
                }
            }
            return (oldValues, newValues);
        }

        private static object? Normalize(object? v) => v switch
        {
            null => null,
            string s => string.IsNullOrWhiteSpace(s) ? null : s.Trim(),
            decimal d => d.ToString("0.####"),
            _ => v.ToString()
        };
    }

    public class CloneFormTemplateRequest
    {
        public string? Name { get; set; }
    }
}
