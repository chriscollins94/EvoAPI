using System.Data.SqlClient;
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EvoAPI.Infrastructure.Services;

// Generic AI dispatcher. One row in ConfigSetting per use case
// (cs_type='AI', cs_identifier='QuoteAI' today) carries endpoint, token,
// model, prompts and OpenAI Structured-Output schema. Every call is logged
// to the shared dbo.AI table.
public class AiService : IAiService
{
    private readonly IDataService _dataService;
    private readonly IFileExtractionService _fileExtractor;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly IConfiguration _configuration;
    private readonly ILogger<AiService> _logger;

    public AiService(
        IDataService dataService,
        IFileExtractionService fileExtractor,
        IHttpClientFactory httpClientFactory,
        IConfiguration configuration,
        ILogger<AiService> logger)
    {
        _dataService = dataService;
        _fileExtractor = fileExtractor;
        _httpClientFactory = httpClientFactory;
        _configuration = configuration;
        _logger = logger;
    }

    public async Task<AiServiceResult> ProcessAsync(
        string identifier,
        string? text,
        Stream? fileContent,
        string? fileName,
        long? fileSize,
        int? srId,
        int userId,
        string? systemPromptAddendum = null,
        CancellationToken ct = default)
    {
        var config = await LoadConfigAsync(identifier);
        if (!config.Enabled)
            throw new InvalidOperationException($"AI use case '{identifier}' is disabled in ConfigSetting.");

        ValidateFile(fileName, fileSize, config);

        FileExtractionResult? extracted = null;
        if (fileContent != null && !string.IsNullOrEmpty(fileName))
            extracted = await _fileExtractor.ExtractAsync(fileContent, fileName, ct);

        // Compose the effective system prompt = config prompt + per-request
        // addendum (e.g. labor rate for the selected SR). The composed prompt
        // is what gets sent AND what we log to dbo.AI.ai_prompt.
        var effectivePrompt = string.IsNullOrWhiteSpace(systemPromptAddendum)
            ? config.SystemPrompt
            : config.SystemPrompt + "\n\n" + systemPromptAddendum.Trim();

        // Build the OpenAI chat-completions request.
        var userContent = BuildUserContent(text, extracted);
        var requestBody = BuildRequestBody(config, effectivePrompt, userContent);
        var jsonPayload = JsonSerializer.Serialize(requestBody);

        var http = _httpClientFactory.CreateClient();
        http.Timeout = TimeSpan.FromMinutes(2);
        using var req = new HttpRequestMessage(HttpMethod.Post, config.Endpoint)
        {
            Content = new StringContent(jsonPayload, Encoding.UTF8, "application/json")
        };
        req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", config.Token);

        var sw = Stopwatch.StartNew();
        string? rawOutput = null;
        string? errorDetail = null;
        try
        {
            using var response = await http.SendAsync(req, ct);
            sw.Stop();

            var body = await response.Content.ReadAsStringAsync(ct);
            if (!response.IsSuccessStatusCode)
            {
                errorDetail = $"OpenAI {(int)response.StatusCode}: {Truncate(body, 1800)}";
                throw new HttpRequestException(errorDetail);
            }

            rawOutput = ExtractAssistantContent(body);

            var inputForLog = ComposeInputForLog(text, extracted);
            var result = new AiServiceResult
            {
                Config         = config,
                RawOutput      = rawOutput,
                Model          = config.Model,
                ResponseTimeMs = sw.Elapsed.TotalMilliseconds,
                InputWordCount = CountWords(inputForLog),
                OutputWordCount= CountWords(rawOutput)
            };

            await LogAsync(identifier, srId, userId, config, effectivePrompt, inputForLog, rawOutput, fileName, fileSize, sw.Elapsed, null);
            return result;
        }
        catch (Exception ex)
        {
            if (sw.IsRunning) sw.Stop();
            errorDetail ??= ex.Message;
            var inputForLog = ComposeInputForLog(text, extracted);
            try
            {
                await LogAsync(identifier, srId, userId, config, effectivePrompt, inputForLog, rawOutput ?? string.Empty,
                    fileName, fileSize, sw.Elapsed, errorDetail);
            }
            catch (Exception logEx)
            {
                _logger.LogError(logEx, "Failed to log AI error to dbo.AI for identifier {Identifier}", identifier);
            }
            throw;
        }
    }

    // ---- ConfigSetting load ------------------------------------------------

    private async Task<AiConfigDto> LoadConfigAsync(string identifier)
    {
        var setting = await _dataService.GetConfigSettingAsync(identifier);
        if (setting == null || string.IsNullOrWhiteSpace(setting.CsValue))
            throw new InvalidOperationException($"ConfigSetting '{identifier}' not found or empty.");

        try
        {
            var config = JsonSerializer.Deserialize<AiConfigDto>(setting.CsValue)
                         ?? throw new InvalidOperationException("Deserialized config was null.");

            if (string.IsNullOrWhiteSpace(config.Endpoint))
                throw new InvalidOperationException("AI config 'endpoint' is required.");
            if (string.IsNullOrWhiteSpace(config.Token))
                throw new InvalidOperationException("AI config 'token' is required.");
            if (string.IsNullOrWhiteSpace(config.Model))
                throw new InvalidOperationException("AI config 'model' is required.");

            return config;
        }
        catch (JsonException ex)
        {
            throw new InvalidOperationException($"ConfigSetting '{identifier}' is not valid JSON: {ex.Message}", ex);
        }
    }

    private static void ValidateFile(string? fileName, long? fileSize, AiConfigDto config)
    {
        if (string.IsNullOrEmpty(fileName)) return;

        var ext = Path.GetExtension(fileName).ToLowerInvariant();
        if (config.AllowedFileExtensions.Count > 0 &&
            !config.AllowedFileExtensions.Contains(ext))
        {
            throw new InvalidOperationException(
                $"File extension '{ext}' is not allowed. Allowed: {string.Join(", ", config.AllowedFileExtensions)}");
        }

        if (fileSize.HasValue)
        {
            var maxBytes = (long)config.MaxFileSizeMb * 1024 * 1024;
            if (fileSize.Value > maxBytes)
                throw new InvalidOperationException(
                    $"File is {fileSize.Value / 1024 / 1024} MB, exceeds limit of {config.MaxFileSizeMb} MB.");
        }
    }

    // ---- Request construction ---------------------------------------------

    // Builds the value of messages[1].content. When a vision payload is
    // present this is the OpenAI mixed-content array; otherwise a plain string.
    private static object BuildUserContent(string? text, FileExtractionResult? extracted)
    {
        var typedText = (text ?? string.Empty).Trim();
        var extractedText = extracted?.Text?.Trim() ?? string.Empty;
        var combinedText = string.Join("\n\n",
            new[] { typedText, extractedText }.Where(s => !string.IsNullOrEmpty(s)));

        if (extracted?.HasImage == true)
        {
            var parts = new List<object>();
            if (!string.IsNullOrEmpty(combinedText))
                parts.Add(new { type = "text", text = combinedText });
            else
                parts.Add(new { type = "text", text = "Read the attached document and produce a quote." });

            parts.Add(new
            {
                type = "image_url",
                image_url = new { url = extracted.ImageDataUrl }
            });
            return parts;
        }

        return string.IsNullOrEmpty(combinedText)
            ? "Produce a quote from the supplied information."
            : combinedText;
    }

    private static object BuildRequestBody(AiConfigDto config, string systemPrompt, object userContent)
    {
        // GPT-5 / o-series models renamed max_tokens -> max_completion_tokens
        // and lock temperature to 1.0. Detect by model name and emit the
        // right shape so config can stay generic across model generations.
        var modelLower = config.Model.ToLowerInvariant();
        var isNewGeneration =
            modelLower.StartsWith("gpt-5") ||
            modelLower.StartsWith("o1") ||
            modelLower.StartsWith("o3") ||
            modelLower.StartsWith("o4");

        var body = new Dictionary<string, object>
        {
            ["model"] = config.Model,
            ["messages"] = new object[]
            {
                new { role = "system", content = systemPrompt },
                new { role = "user",   content = userContent }
            }
        };

        if (isNewGeneration)
        {
            body["max_completion_tokens"] = config.MaxTokens;
            // temperature is locked to 1.0 on these models — omit so the
            // request isn't rejected for "unsupported_value".
        }
        else
        {
            body["max_tokens"]  = config.MaxTokens;
            body["temperature"] = config.Temperature;
        }

        // OpenAI Structured Outputs: pass-through whatever schema admin
        // configured. Shape is { name, strict, schema: { ... } }.
        if (config.OutputSchema.HasValue && config.OutputSchema.Value.ValueKind == JsonValueKind.Object)
        {
            body["response_format"] = new
            {
                type = "json_schema",
                json_schema = config.OutputSchema.Value
            };
        }

        return body;
    }

    private static string ExtractAssistantContent(string responseBody)
    {
        using var doc = JsonDocument.Parse(responseBody);
        var content = doc.RootElement
            .GetProperty("choices")[0]
            .GetProperty("message")
            .GetProperty("content")
            .GetString();
        return content ?? string.Empty;
    }

    // ---- Logging to dbo.AI -------------------------------------------------

    private async Task LogAsync(
        string identifier, int? srId, int userId, AiConfigDto config,
        string promptSent, string input, string output, string? fileName, long? fileSize,
        TimeSpan elapsed, string? error)
    {
        var connStr = _configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException("No connection string found");

        const string sql = @"
            INSERT INTO dbo.AI
                (sr_id, u_id, ai_type, ai_input, ai_output, ai_responsetime,
                 ai_prompt, ai_inputwordcount, ai_outputwordcount,
                 ai_filename, ai_filesize, ai_model, ai_error, ai_insertdatetime)
            VALUES
                (@sr_id, @u_id, @ai_type, @ai_input, @ai_output, @ai_responsetime,
                 @ai_prompt, @ai_inputwordcount, @ai_outputwordcount,
                 @ai_filename, @ai_filesize, @ai_model, @ai_error, SYSUTCDATETIME());";

        using var conn = new SqlConnection(connStr);
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@sr_id",   (object?)srId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@u_id",    userId);
        cmd.Parameters.AddWithValue("@ai_type", Truncate(identifier, 50));
        cmd.Parameters.AddWithValue("@ai_input",  (object?)input  ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ai_output", (object?)output ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ai_responsetime", elapsed.TotalSeconds.ToString("0.00"));
        cmd.Parameters.AddWithValue("@ai_prompt", (object?)promptSent ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ai_inputwordcount",  CountWords(input));
        cmd.Parameters.AddWithValue("@ai_outputwordcount", CountWords(output));
        cmd.Parameters.AddWithValue("@ai_filename", (object?)fileName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ai_filesize", (object?)fileSize ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ai_model",    (object?)config.Model ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@ai_error",    (object?)Truncate(error, 2000) ?? DBNull.Value);

        await conn.OpenAsync();
        await cmd.ExecuteNonQueryAsync();
    }

    private static string ComposeInputForLog(string? text, FileExtractionResult? extracted)
    {
        var typed = text ?? string.Empty;
        var fromFile = extracted?.Text ?? string.Empty;
        var note = extracted?.HasImage == true ? $"[binary {extracted.MimeType} sent as vision input]" : string.Empty;
        return string.Join("\n\n",
            new[] { typed, fromFile, note }.Where(s => !string.IsNullOrEmpty(s)));
    }

    private static int CountWords(string s)
    {
        if (string.IsNullOrWhiteSpace(s)) return 0;
        return s.Split(new[] { ' ', '\t', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries).Length;
    }

    private static string? Truncate(string? s, int len)
    {
        if (s == null) return null;
        return s.Length <= len ? s : s.Substring(0, len);
    }
}
