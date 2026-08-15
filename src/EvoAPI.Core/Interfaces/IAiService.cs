using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

// Generic dispatcher for AI use cases. Pulls the use-case config from
// ConfigSetting (cs_type='AI', cs_identifier=<identifier>), runs file
// extraction, calls the configured chat-completions endpoint, and logs
// the round trip to the shared dbo.AI table.
public interface IAiService
{
    Task<AiServiceResult> ProcessAsync(
        string identifier,
        string? text,
        Stream? fileContent,
        string? fileName,
        long? fileSize,
        int? srId,
        int userId,
        string? systemPromptAddendum = null,
        CancellationToken ct = default);
}

public class AiServiceResult
{
    public AiConfigDto Config         { get; set; } = new();
    public string      RawOutput      { get; set; } = string.Empty;
    public string      Model          { get; set; } = string.Empty;
    public double      ResponseTimeMs { get; set; }
    public int         InputWordCount { get; set; }
    public int         OutputWordCount{ get; set; }
}
