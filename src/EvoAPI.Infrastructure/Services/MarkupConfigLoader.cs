using System.Data.SqlClient;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EvoAPI.Infrastructure.Services;

// Pulls the per-SR markup/tax inputs that evo's invoice flow uses, in one
// round trip (one combined query for the scalars + a separate query for the
// multi-row MaterialsMarkup ranges).
public class MarkupConfigLoader : IMarkupConfigLoader
{
    private readonly IConfiguration _configuration;
    private readonly IDataService _dataService;
    private readonly ILogger<MarkupConfigLoader> _logger;

    public MarkupConfigLoader(IConfiguration configuration, IDataService dataService, ILogger<MarkupConfigLoader> logger)
    {
        _configuration = configuration;
        _dataService = dataService;
        _logger = logger;
    }

    public async Task<MarkupConfigDto?> LoadAsync(int srId, CancellationToken ct = default)
    {
        if (srId <= 0) return null;

        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        if (string.IsNullOrEmpty(connectionString))
            throw new InvalidOperationException("No connection string found");

        // ---- Pull the scalar markup inputs --------------------------------
        // lr_markup is INT NULL — coerced via ISNULL because the column may be
        // missing for trades that don't override at the trade level.
        const string scalarSql = @"
            SELECT TOP 1
                sr.xccc_id,
                sr.t_id,
                xccc.xccc_markuppercentage         AS CompanyDefault,
                xccc.xccc_markuppercentagesupplier AS CompanySupplier,
                xccc.xccc_markuptriggeramount      AS TriggerAmount,
                xccc.xccc_taxexempt                AS TaxExempt,
                ISNULL(lr.lr_markup, 0)            AS TradeMarkup
            FROM ServiceRequest sr
            LEFT JOIN xrefCompanyCallCenter xccc
                ON xccc.xccc_id = sr.xccc_id
            LEFT JOIN LaborRate lr
                ON lr.xccc_id = sr.xccc_id
               AND lr.t_id    = sr.t_id
               AND lr.lr_flatorhourly = 'Hourly'
            WHERE sr.sr_id = @SrId;";

        int xcccId = 0;
        decimal? companyDefault = null;
        decimal? companySupplier = null;
        decimal? triggerAmount = null;
        bool taxExempt = false;
        decimal tradeMarkup = 0;

        using (var conn = new SqlConnection(connectionString))
        using (var cmd = new SqlCommand(scalarSql, conn))
        {
            cmd.Parameters.AddWithValue("@SrId", srId);
            await conn.OpenAsync(ct);
            using var reader = await cmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct)) return null;

            xcccId          = reader["xccc_id"]         is DBNull ? 0    : Convert.ToInt32(reader["xccc_id"]);
            companyDefault  = reader["CompanyDefault"]  is DBNull ? null : (decimal?)Convert.ToDecimal(reader["CompanyDefault"]);
            companySupplier = reader["CompanySupplier"] is DBNull ? null : (decimal?)Convert.ToDecimal(reader["CompanySupplier"]);
            triggerAmount   = reader["TriggerAmount"]   is DBNull ? null : (decimal?)Convert.ToDecimal(reader["TriggerAmount"]);
            taxExempt       = reader["TaxExempt"]       is not DBNull && Convert.ToBoolean(reader["TaxExempt"]);
            tradeMarkup     = reader["TradeMarkup"]     is DBNull ? 0    : Convert.ToDecimal(reader["TradeMarkup"]);
        }

        // ---- Pull MaterialsMarkup ranges for this company/call center -----
        var ranges = new List<MarkupRangeDto>();
        if (xcccId > 0)
        {
            const string rangeSql = @"
                SELECT mm_from, mm_to, mm_markup, mm_markuphighquantity
                FROM MaterialsMarkup
                WHERE xccc_id = @XcccId
                ORDER BY mm_from;";

            using var conn = new SqlConnection(connectionString);
            using var cmd  = new SqlCommand(rangeSql, conn);
            cmd.Parameters.AddWithValue("@XcccId", xcccId);
            await conn.OpenAsync(ct);
            using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                ranges.Add(new MarkupRangeDto
                {
                    From          = Convert.ToDecimal(reader["mm_from"]),
                    To            = Convert.ToDecimal(reader["mm_to"]),
                    Markup        = Convert.ToDecimal(reader["mm_markup"]),
                    HighQtyMarkup = reader["mm_markuphighquantity"] is DBNull ? 0 : Convert.ToDecimal(reader["mm_markuphighquantity"])
                });
            }
        }

        // ---- Flat tax rate from ConfigSetting -----------------------------
        decimal taxFlatRate = 0m;
        try
        {
            var taxSetting = await _dataService.GetConfigSettingAsync("taxFlatRate");
            if (taxSetting?.CsValue is string s && decimal.TryParse(s, out var parsed))
                taxFlatRate = parsed;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not load taxFlatRate ConfigSetting; defaulting to 0");
        }

        var dto = new MarkupConfigDto
        {
            TradeMarkupPercent     = tradeMarkup > 0 ? tradeMarkup : null,
            CompanyDefaultPercent  = companyDefault,
            CompanySupplierPercent = companySupplier,
            MaterialsRanges        = ranges,
            TaxExempt              = taxExempt,
            TaxFlatRate            = taxFlatRate,
            TriggerAmount          = triggerAmount
        };
        dto.Summary = BuildSummary(dto);
        return dto;
    }

    // One-liner shown in the UI chip and the PDF footer. Reflects which tier
    // WILL be used per evo's cascade (trade > materials > company default).
    private static string BuildSummary(MarkupConfigDto cfg)
    {
        string baseSummary;
        if (cfg.TradeMarkupPercent.HasValue)
            baseSummary = $"Trade-level {cfg.TradeMarkupPercent}%";
        else if (cfg.MaterialsRanges.Count > 0)
        {
            var min = cfg.MaterialsRanges.Min(r => r.Markup);
            var max = cfg.MaterialsRanges.Max(r => Math.Max(r.Markup, r.HighQtyMarkup));
            baseSummary = $"Tiered materials ({cfg.MaterialsRanges.Count} range{(cfg.MaterialsRanges.Count == 1 ? "" : "s")}, {min}-{max}%)";
        }
        else if (cfg.CompanyDefaultPercent.HasValue && cfg.CompanyDefaultPercent > 0)
            baseSummary = $"Company default {cfg.CompanyDefaultPercent}%";
        else
            baseSummary = "No markup configured";

        if (cfg.CompanySupplierPercent.HasValue && cfg.CompanySupplierPercent > 0)
            baseSummary += $" (+{cfg.CompanySupplierPercent}% supplier)";

        return baseSummary;
    }
}
