using System.Diagnostics;
using System.Text.Json;
using EvoAPI.Core.Interfaces;
using EvoAPI.Infrastructure.Pdf;
using EvoAPI.Infrastructure.Pricing;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using Microsoft.AspNetCore.Mvc;

namespace EvoAPI.Api.Controllers;

// Generic AI endpoint + a Quote-AI-specific wrapper that renders the AI
// response into a branded PDF. The use-case identifier (e.g. "QuoteAI") is
// what picks the config row in ConfigSetting and therefore the model / prompt
// / output schema. Add new use cases by inserting a new ConfigSetting row and
// (optionally) a new wrapper endpoint with its own renderer.
[ApiController]
[Route("EvoApi/ai")]
[EvoAuthorize]
public class AiController : BaseController
{
    private readonly IAiService _aiService;
    private readonly QuotePdfRenderer _quotePdfRenderer;
    private readonly IMarkupConfigLoader _markupConfigLoader;

    public AiController(
        IAiService aiService,
        QuotePdfRenderer quotePdfRenderer,
        IMarkupConfigLoader markupConfigLoader,
        IAuditService auditService)
    {
        _aiService = aiService;
        _quotePdfRenderer = quotePdfRenderer;
        _markupConfigLoader = markupConfigLoader;
        InitializeAuditService(auditService);
    }

    // Generic: returns whatever the AI produced as a string. Callers can parse
    // it themselves (e.g. JSON.parse on the client) when the configured use
    // case uses Structured Outputs.
    [HttpPost("process")]
    [Consumes("multipart/form-data")]
    public async Task<ActionResult<ApiResponse<AiProcessResultDto>>> Process(
        [FromForm] string identifier,
        [FromForm] string? text,
        [FromForm] int? sr_id,
        [FromForm] string? srContextJson,
        IFormFile? file,
        CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            if (string.IsNullOrWhiteSpace(identifier))
                return BadRequest(new ApiResponse<AiProcessResultDto>
                {
                    Success = false,
                    Message = "identifier is required"
                });

            var addendum = BuildSrContextAddendum(ParseSrContext(srContextJson));

            using var stream = file?.OpenReadStream();
            var result = await _aiService.ProcessAsync(
                identifier,
                text,
                stream,
                file?.FileName,
                file?.Length,
                sr_id,
                UserId,
                addendum,
                ct);

            stopwatch.Stop();
            await LogAuditAsync(
                $"AiProcess:{identifier}",
                new { sr_id, fileName = file?.FileName, fileSize = file?.Length, textLen = text?.Length ?? 0, result.Model, result.ResponseTimeMs },
                stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<AiProcessResultDto>
            {
                Success = true,
                Message = "AI request processed",
                Data = new AiProcessResultDto
                {
                    Output          = result.RawOutput,
                    Model           = result.Model,
                    ResponseTimeMs  = result.ResponseTimeMs,
                    InputWordCount  = result.InputWordCount,
                    OutputWordCount = result.OutputWordCount
                },
                Count = 1
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync($"AiProcess:{identifier}", ex,
                new { sr_id, fileName = file?.FileName, fileSize = file?.Length });
            return StatusCode(500, new ApiResponse<AiProcessResultDto>
            {
                Success = false,
                Message = $"AI request failed: {ex.Message}"
            });
        }
    }

    // Quote-AI-specific: runs Process under identifier "QuoteAI", deserializes
    // the AI's structured output into a QuoteJsonDto, renders to PDF, streams
    // back application/pdf.
    [HttpPost("quote-pdf")]
    [Consumes("multipart/form-data")]
    public async Task<IActionResult> QuotePdf(
        [FromForm] string? text,
        [FromForm] int? sr_id,
        [FromForm] string? srContextJson,
        IFormFile? file,
        CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        const string identifier = "QuoteAI";
        try
        {
            var srContext = ParseSrContext(srContextJson);
            var addendum  = BuildSrContextAddendum(srContext);

            using var stream = file?.OpenReadStream();
            var result = await _aiService.ProcessAsync(
                identifier,
                text,
                stream,
                file?.FileName,
                file?.Length,
                sr_id,
                UserId,
                addendum,
                ct);

            QuoteJsonDto quote;
            try
            {
                quote = JsonSerializer.Deserialize<QuoteJsonDto>(result.RawOutput,
                            new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                    ?? throw new InvalidOperationException("Quote JSON deserialized to null.");
            }
            catch (JsonException ex)
            {
                throw new InvalidOperationException(
                    "AI response was not valid quote JSON. Check the QuoteAI ConfigSetting outputSchema. Raw output: "
                    + (result.RawOutput.Length > 500 ? result.RawOutput.Substring(0, 500) + "…" : result.RawOutput),
                    ex);
            }

            // Server is the single source of truth for markup. Pull the
            // company/trade markup config once for this SR and apply it to
            // every material lineItem and every serviceItem. Labor and
            // "other" lines are left alone (labor rate is already final;
            // tripcharges / permits aren't marked up). When no SR is selected
            // or the SR has no markup config, the calls below no-op.
            MarkupConfigDto? markupCfg = null;
            if (sr_id.HasValue && sr_id.Value > 0)
            {
                try
                {
                    markupCfg = await _markupConfigLoader.LoadAsync(sr_id.Value, ct);
                }
                catch (Exception ex)
                {
                    // Don't block PDF generation if markup load fails — log and continue.
                    await LogAuditErrorAsync("AiQuotePdf:MarkupLoad", ex, new { sr_id });
                }
            }
            if (markupCfg != null)
                ApplyMarkup(quote, markupCfg);

            // Prepend the configured trip charge as the first line item
            // (matches evo's invoice/quote layout). type=other so it doesn't
            // get marked up or taxed — evo treats trip charge as a flat add.
            if (srContext?.HasTripCharge == true)
            {
                var tripAmount = srContext.TripChargeAmount!.Value;
                quote.LineItems.Insert(0, new QuoteLineItemDto
                {
                    Description   = "Trip Charge",
                    Quantity      = 1,
                    UnitPrice     = tripAmount,
                    Total         = tripAmount,
                    Type          = "other",
                    MarkupPercent = 0,
                    TaxAmount     = 0
                });
                // Bump quote-level Subtotal/Total — Tax is unchanged since
                // trip charge isn't taxed.
                quote.Subtotal += tripAmount;
                quote.Total    += tripAmount;
            }

            var pdfBytes = _quotePdfRenderer.Render(quote, srContext, markupCfg);

            stopwatch.Stop();
            await LogAuditAsync(
                "AiQuotePdf",
                new { sr_id, fileName = file?.FileName, fileSize = file?.Length, pdfBytes = pdfBytes.Length, result.Model, result.ResponseTimeMs },
                stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            var downloadName = string.IsNullOrWhiteSpace(quote.QuoteNumber)
                ? $"Quote_{DateTime.Now:yyyyMMddHHmmss}.pdf"
                : $"Quote_{Sanitize(quote.QuoteNumber)}.pdf";

            return File(pdfBytes, "application/pdf", downloadName);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("AiQuotePdf", ex,
                new { sr_id, fileName = file?.FileName, fileSize = file?.Length });
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = $"Quote PDF generation failed: {ex.Message}"
            });
        }
    }

    private static string Sanitize(string s) =>
        new string(s.Where(c => char.IsLetterOrDigit(c) || c == '-' || c == '_').ToArray());

    // Walks the AI-returned quote and stamps the server-computed markup +
    // tax on every material lineItem and every serviceItem. Recomputes line
    // totals and breaks the quote-level subtotal/tax/total apart for display.
    //
    // Lines tagged "labor" or "other" pass through with markupPercent = 0 and
    // tax = 0 — labor's unitPrice is already the configured rate (final
    // billing), and "other" covers trip charges / permits / fees which evo
    // doesn't mark up or tax.
    //
    // Tax math mirrors evo's invoice formula:
    //   line subtotal (marked-up)  = baseCost * qty * (1 + markup/100)
    //   line tax      (marked-up)  = baseCost * qty * (taxRate/100) * (1 + markup/100)
    //   line total                 = subtotal + tax
    // Sum across lines for quote-level Subtotal / Tax / Total — Subtotal + Tax
    // matches Total to the cent because rounded line components add up.
    private static void ApplyMarkup(QuoteJsonDto quote, MarkupConfigDto config)
    {
        // Service items (materials list on page 1)
        foreach (var si in quote.ServiceItems)
        {
            var calc = MarkupCalculator.Calculate(config, si.EstimatedUnitCost, si.EstimatedQuantity, taxable: true);
            si.MarkupPercent      = calc.EffectiveMarkup;
            si.EstimatedTotalCost = calc.LineTotal;
        }

        // Line items (billable breakdown). Only material lines get markup + tax.
        decimal subtotalAccum = 0m;
        decimal taxAccum      = 0m;
        foreach (var li in quote.LineItems)
        {
            var type = (li.Type ?? string.Empty).Trim().ToLowerInvariant();
            if (type == "material")
            {
                var calc      = MarkupCalculator.Calculate(config, li.UnitPrice, li.Quantity, taxable: true);
                var markupMul = 1m + (decimal)calc.EffectiveMarkup / 100m;
                var preTax    = Math.Round(li.UnitPrice * li.Quantity * markupMul, 2, MidpointRounding.AwayFromZero);
                var lineTax   = Math.Round(calc.TaxAmount * markupMul,             2, MidpointRounding.AwayFromZero);

                li.MarkupPercent = calc.EffectiveMarkup;
                li.TaxAmount     = lineTax;
                li.Total         = preTax + lineTax;

                subtotalAccum += preTax;
                taxAccum      += lineTax;
            }
            else
            {
                li.MarkupPercent = 0;
                li.TaxAmount     = 0;
                li.Total         = Math.Round(li.Quantity * li.UnitPrice, 2, MidpointRounding.AwayFromZero);
                subtotalAccum   += li.Total;
            }
        }

        quote.Subtotal = subtotalAccum;
        quote.Tax      = taxAccum;
        quote.Total    = subtotalAccum + taxAccum;
    }

    // Best-effort deserialize of the SR context blob the UI POSTed. Returns
    // null on missing/empty/malformed JSON so callers can degrade gracefully.
    private static SrLaborContextDto? ParseSrContext(string? srContextJson)
    {
        if (string.IsNullOrWhiteSpace(srContextJson)) return null;
        try
        {
            return JsonSerializer.Deserialize<SrLaborContextDto>(srContextJson,
                new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        }
        catch (JsonException)
        {
            return null;
        }
    }

    // Turns the SR labor-context payload (from the UI) into a short prose
    // appendix that gets glued onto the system prompt for this one call.
    // The AI uses this to fill in customer info and labor unitPrice on the
    // returned quote JSON. Returns null when there's no usable context.
    private static string? BuildSrContextAddendum(SrLaborContextDto? ctx)
    {
        if (ctx == null) return null;

        var lines = new List<string> { "Context for this specific quote:" };
        if (!string.IsNullOrWhiteSpace(ctx.RequestNumber)) lines.Add($"- Service Request: {ctx.RequestNumber}");
        if (!string.IsNullOrWhiteSpace(ctx.Company))      lines.Add($"- Company: {ctx.Company}");
        if (!string.IsNullOrWhiteSpace(ctx.CallCenter))   lines.Add($"- Call Center: {ctx.CallCenter}");
        if (!string.IsNullOrWhiteSpace(ctx.Trade))        lines.Add($"- Trade: {ctx.Trade}");

        if (ctx.HasRate)
        {
            var rateType = string.IsNullOrWhiteSpace(ctx.RateType) ? "standard" : ctx.RateType;
            lines.Add($"- Hourly labor rate: ${ctx.RatePerHour:0.00}/hr ({rateType}).");
            lines.Add($"  Use this rate as the unitPrice on any labor lineItems (quantity = totalLaborHours, unitPrice = {ctx.RatePerHour:0.00}). Recompute total = quantity * unitPrice and update subtotal/total accordingly. This overrides the earlier instruction to leave unitPrice at 0 for labor.");
        }
        else
        {
            lines.Add("- Hourly labor rate is not configured for this trade/customer. Leave labor unitPrice at 0 for the estimator to fill in.");
        }

        // Trip charge note — the server prepends a Trip Charge line item
        // itself after the AI responds, so we explicitly tell the AI to skip it.
        if (ctx.HasTripCharge)
        {
            lines.Add($"- A trip charge of ${ctx.TripChargeAmount:0.00} ({ctx.TripChargeSource}) will be applied. DO NOT include any trip-charge, mobilization, or show-up-fee line — the server will add a single \"Trip Charge\" line item at the top automatically.");
        }
        else
        {
            lines.Add("- No trip charge is configured for this customer/trade. DO NOT add a trip-charge line on your own.");
        }

        return lines.Count > 1 ? string.Join("\n", lines) : null;
    }
}
