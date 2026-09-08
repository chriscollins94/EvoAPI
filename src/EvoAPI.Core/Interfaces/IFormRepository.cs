using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public enum FormDeleteResult { Deleted, NotFound, Referenced }

public interface IFormRepository
{
    // lookups
    Task<bool> IsFeatureEnabledAsync();
    Task<List<FormParentTradeDto>> GetParentTradesAsync();
    Task<List<CheckListAnswerTypeDto>> GetAnswerTypesAsync();
    Task<List<PmBillingModeDto>> GetBillingModesAsync();
    Task<List<FormTradeOptionDto>> GetPmSubTradesAsync(int parentTId);
    Task<List<FormAssetCategoryDto>> GetAssetCategoriesAsync(int tId);

    // templates
    Task<List<FormTemplateDto>> GetTemplatesAsync();
    Task<FormTemplateDetailDto?> GetTemplateAsync(int ftId);
    Task<int> CreateTemplateAsync(SaveFormTemplateRequest request, int version);
    Task<int> GetNextVersionAsync(int tId);
    Task<bool> UpdateTemplateAsync(int ftId, SaveFormTemplateRequest request);
    Task<int> CloneTemplateAsync(int ftId, string? name);

    // sections
    Task<FormSectionDto?> GetSectionAsync(int fsId);
    Task<int> CreateSectionAsync(int ftId, SaveFormSectionRequest request);
    Task<bool> UpdateSectionAsync(int fsId, SaveFormSectionRequest request);
    Task<FormDeleteResult> DeleteSectionAsync(int fsId);

    // questions
    Task<FormQuestionDto?> GetQuestionAsync(int fqId);
    Task<bool> CodeExistsInTemplateAsync(int ftId, string code, int? excludeFqId);
    Task<int> CreateQuestionAsync(int fsId, SaveFormQuestionRequest request);
    Task<bool> UpdateQuestionAsync(int fqId, SaveFormQuestionRequest request);
    Task<FormDeleteResult> DeleteQuestionAsync(int fqId);

    // answer lists
    Task<List<FormAnswerListDto>> GetAnswerListsAsync();
    Task<int> CreateAnswerListAsync(SaveFormAnswerListRequest request);
    Task<bool> UpdateAnswerListAsync(int falId, SaveFormAnswerListRequest request);

    // rules
    Task<List<FormCompanyTradeDto>> GetCompanyFormTradesAsync(int xcccId);
    Task<FormRuleDto?> GetRuleAsync(int xcccId, int tId);
    Task<int> UpsertRuleAsync(int xcccId, int tId, SaveFormRuleRequest request);
    Task<int> ReplaceTiersAsync(int frId, List<PmRateTierDto> tiers);
    Task<int> ReplaceSeasonsAsync(int frId, List<PmSeasonDto> seasons);
    Task<int> ReplaceQuestionOverridesAsync(int frId, List<FormRuleQuestionDto> overrides);
}
