using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace EvoAPI.Infrastructure.Services;

public class QuickBooksService : IQuickBooksService
{
    private readonly HttpClient _httpClient;
    private readonly IDataService _dataService;
    private readonly IAuditService _auditService;
    private readonly IConfiguration _configuration;
    private readonly ILogger<QuickBooksService> _logger;

    private const string TokenCsType = "Token";
    private const string AccessTokenIdentifier = "AccessToken";

    public QuickBooksService(
        HttpClient httpClient,
        IDataService dataService,
        IAuditService auditService,
        IConfiguration configuration,
        ILogger<QuickBooksService> logger)
    {
        _httpClient = httpClient;
        _dataService = dataService;
        _auditService = auditService;
        _configuration = configuration;
        _logger = logger;
    }

    public async Task<QuickBooksInvoiceCheckDto> CheckInvoiceByRequestNumberAsync(string requestNumber)
    {
        var sr = await _dataService.GetServiceRequestQbInfoByRequestNumberAsync(requestNumber);
        if (sr == null)
        {
            return new QuickBooksInvoiceCheckDto
            {
                sr_requestnumber = requestNumber,
                error = "Service request not found"
            };
        }

        var result = new QuickBooksInvoiceCheckDto
        {
            sr_id = sr.sr_id,
            sr_requestnumber = sr.sr_requestnumber,
            sr_quickbooks_docnumber = sr.sr_quickbooks_docnumber,
            dbSyncToken = sr.sr_quickbooks_synctoken
        };

        if (string.IsNullOrWhiteSpace(sr.sr_quickbooks_docnumber))
        {
            result.error = "Service request has no QuickBooks doc number";
            return result;
        }

        // QBQL injection guard: docnumber must be safe to interpolate into a single-quoted literal
        if (!IsSafeQbLiteral(sr.sr_quickbooks_docnumber))
        {
            result.error = "Doc number contains invalid characters";
            return result;
        }

        var realmId = await _dataService.GetConfigSettingValueAsync("quickbooks", "RealmId");
        var qbBaseUrl = await _dataService.GetConfigSettingValueAsync("quickbooks", "BaseUrl");
        if (string.IsNullOrWhiteSpace(realmId)) realmId = "735175030";
        if (string.IsNullOrWhiteSpace(qbBaseUrl)) qbBaseUrl = "https://quickbooks.api.intuit.com";

        var accessToken = await _dataService.GetConfigSettingValueAsync(TokenCsType, AccessTokenIdentifier);
        if (string.IsNullOrWhiteSpace(accessToken))
        {
            result.error = "QuickBooks access token not found in ConfigSetting";
            return result;
        }

        var qbResponse = await CallQuickBooksAsync(qbBaseUrl, realmId, sr.sr_quickbooks_docnumber, accessToken);
        var initialBody = await qbResponse.Content.ReadAsStringAsync();
        await LogAuditAsync("QuickBooksCheckInvoice - QB call", new
        {
            requestNumber,
            docNumber = sr.sr_quickbooks_docnumber,
            qbStatus = (int)qbResponse.StatusCode,
            qbStatusName = qbResponse.StatusCode.ToString(),
            qbBodyPreview = Truncate(initialBody, 1000)
        });

        // One-time refresh + retry on 401
        if (qbResponse.StatusCode == HttpStatusCode.Unauthorized)
        {
            _logger.LogInformation("QuickBooks access token rejected (401); attempting refresh via EvoWS");
            var (refreshed, refreshDetail) = await TryRefreshAccessTokenAsync();
            await LogAuditAsync("QuickBooksCheckInvoice - Refresh", new
            {
                requestNumber,
                refreshed,
                refreshDetail
            });

            if (!refreshed)
            {
                result.error = $"QuickBooks token expired and refresh failed: {refreshDetail}";
                return result;
            }

            accessToken = await _dataService.GetConfigSettingValueAsync(TokenCsType, AccessTokenIdentifier);
            if (string.IsNullOrWhiteSpace(accessToken))
            {
                result.error = "QuickBooks token refreshed but new value not readable from ConfigSetting";
                return result;
            }

            qbResponse.Dispose();
            qbResponse = await CallQuickBooksAsync(qbBaseUrl, realmId, sr.sr_quickbooks_docnumber, accessToken);
            var retryBody = await qbResponse.Content.ReadAsStringAsync();
            await LogAuditAsync("QuickBooksCheckInvoice - QB retry", new
            {
                requestNumber,
                qbStatus = (int)qbResponse.StatusCode,
                qbStatusName = qbResponse.StatusCode.ToString(),
                qbBodyPreview = Truncate(retryBody, 1000)
            });

            if (qbResponse.StatusCode == HttpStatusCode.Unauthorized)
            {
                result.error = "QuickBooks still returned 401 after token refresh";
                return result;
            }
            initialBody = retryBody;
        }

        var body = initialBody;

        if (!qbResponse.IsSuccessStatusCode)
        {
            result.error = $"QuickBooks API error ({(int)qbResponse.StatusCode}): {Truncate(body, 500)}";
            return result;
        }

        QuickBooksInvoiceQueryResponse? parsed;
        try
        {
            parsed = JsonSerializer.Deserialize<QuickBooksInvoiceQueryResponse>(body, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to parse QuickBooks response for docnumber {DocNumber}", sr.sr_quickbooks_docnumber);
            result.error = "Could not parse QuickBooks response";
            return result;
        }

        var invoices = parsed?.QueryResponse?.Invoice ?? new List<QuickBooksInvoice>();
        result.qbInvoiceMatches = invoices.Count;

        if (invoices.Count == 0)
        {
            result.error = "No invoices found in QuickBooks for that doc number";
            return result;
        }

        var invoice = invoices[0];
        result.qbId = invoice.Id;
        result.qbSyncToken = invoice.SyncToken;
        result.qbCreateTime = invoice.MetaData?.CreateTime;
        result.qbInvoice = invoice;
        result.qbRawResponse = PrettyPrintJson(body);
        result.isMismatch = !string.Equals(
            (sr.sr_quickbooks_synctoken ?? string.Empty).Trim(),
            (invoice.SyncToken ?? string.Empty).Trim(),
            StringComparison.Ordinal);

        if (invoices.Count > 1)
        {
            result.error = $"QuickBooks returned {invoices.Count} invoices for that doc number; showing first";
        }

        return result;
    }

    private static string PrettyPrintJson(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            return JsonSerializer.Serialize(doc.RootElement, new JsonSerializerOptions { WriteIndented = true });
        }
        catch
        {
            return json;
        }
    }

    private async Task<HttpResponseMessage> CallQuickBooksAsync(string baseUrl, string realmId, string docNumber, string accessToken)
    {
        var query = $"Select * from invoice where docnumber = '{docNumber}'";
        var url = $"{baseUrl.TrimEnd('/')}/v3/company/{realmId}/query?query={Uri.EscapeDataString(query)}";

        var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        return await _httpClient.SendAsync(request);
    }

    private async Task<(bool refreshed, string detail)> TryRefreshAccessTokenAsync()
    {
        var evoWsBaseUrl = _configuration["EvoWS:BaseUrl"];
        if (string.IsNullOrWhiteSpace(evoWsBaseUrl))
        {
            _logger.LogError("EvoWS:BaseUrl not configured; cannot refresh QuickBooks token");
            return (false, "EvoWS:BaseUrl not configured in EvoAPI appsettings/env");
        }

        var refreshUrl = $"{evoWsBaseUrl.TrimEnd('/')}/ws/api/Accounting/RefreshToken";
        try
        {
            using var refreshRequest = new HttpRequestMessage(HttpMethod.Post, refreshUrl);
            using var refreshResponse = await _httpClient.SendAsync(refreshRequest);
            var body = await refreshResponse.Content.ReadAsStringAsync();
            if (!refreshResponse.IsSuccessStatusCode)
            {
                _logger.LogError("EvoWS RefreshToken returned {Status}: {Body}", refreshResponse.StatusCode, Truncate(body, 500));
                return (false, $"EvoWS POST {refreshUrl} returned {(int)refreshResponse.StatusCode} {refreshResponse.StatusCode}: {Truncate(body, 400)}");
            }
            return (true, $"EvoWS POST {refreshUrl} returned 200");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error calling EvoWS RefreshToken at {Url}", refreshUrl);
            return (false, $"Exception calling {refreshUrl}: {ex.GetType().Name}: {ex.Message}");
        }
    }

    private async Task LogAuditAsync(string description, object detail)
    {
        try
        {
            await _auditService.LogAsync(new AuditEntry
            {
                Name = "QuickBooksService",
                Description = description,
                Detail = JsonSerializer.Serialize(detail),
                MachineName = Environment.MachineName
            });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to write audit entry for {Description}", description);
        }
    }

    private static bool IsSafeQbLiteral(string s)
    {
        foreach (var c in s)
        {
            if (!(char.IsLetterOrDigit(c) || c == '-' || c == '_' || c == '.'))
                return false;
        }
        return true;
    }

    private static string Truncate(string s, int max) => s.Length <= max ? s : s.Substring(0, max);
}
