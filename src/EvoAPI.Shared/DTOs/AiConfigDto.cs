using System.Text.Json.Serialization;

namespace EvoAPI.Shared.DTOs;

// Mirrors the JSON stored in ConfigSetting.cs_value where cs_type='AI'.
// One row per use case (cs_identifier = "QuoteAI", future "FooAI", etc.).
// Admins retune by editing the JSON in the DB — no redeploy.
public class AiConfigDto
{
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; } = true;

    [JsonPropertyName("endpoint")]
    public string Endpoint { get; set; } = string.Empty;

    [JsonPropertyName("token")]
    public string Token { get; set; } = string.Empty;

    [JsonPropertyName("model")]
    public string Model { get; set; } = string.Empty;

    [JsonPropertyName("maxTokens")]
    public int MaxTokens { get; set; } = 8192;

    [JsonPropertyName("temperature")]
    public double Temperature { get; set; } = 0.2;

    [JsonPropertyName("maxFileSizeMb")]
    public int MaxFileSizeMb { get; set; } = 20;

    [JsonPropertyName("allowedFileExtensions")]
    public List<string> AllowedFileExtensions { get; set; } = new();

    [JsonPropertyName("systemPrompt")]
    public string SystemPrompt { get; set; } = string.Empty;

    // OpenAI "json_schema" Structured Outputs payload. Passed through verbatim
    // as response_format.json_schema, so its shape must match OpenAI's spec:
    // { name, strict, schema: { ... } }.
    [JsonPropertyName("outputSchema")]
    public System.Text.Json.JsonElement? OutputSchema { get; set; }
}
