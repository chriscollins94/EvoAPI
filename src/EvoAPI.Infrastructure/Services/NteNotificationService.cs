using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Logging;

namespace EvoAPI.Infrastructure.Services;

// One scan = one recipient query + one spend query, then per (service request,
// checked-in tech) row: text the tech at the highest crossed threshold and email the
// zone at thresholds >= 100. Decisions D1-D9 in NTE_NOTIFICATION_PLAN.md.
//
// Every text / email (and every explanatory skip) is one dbo.AuditNTE row, which is also
// the dedup lock (unique on sr_id, channel, key):
//   SMS   key = "{threshold}@{nte}|u{techUserId}"   one text per tech per threshold per NTE amount
//   Email key = "{threshold}@{nte}|z{zoneId}"       one email per zone per threshold per NTE amount
// Including the NTE amount re-arms both legs when the office changes the NTE (D4).
// A Skipped row (no phone, no zone) also occupies its key, so the same tech/zone is not
// retried every 5 minutes; fixing the data and raising/lowering the NTE re-arms it.
//
// Rollout controls (all ConfigSetting NteConfig rows, read every scan):
//   SmokeTestOverrideMobile / SmokeTestOverrideEmail - semicolon lists; when set, every send goes
//       there instead of the tech / zone and the audit row is flagged an_overridden = 1.
//   EnabledTechIds - comma list of user ids; when set, rows for other techs are skipped WITHOUT an
//       audit row, so they alert normally once the list is cleared. Empty = everyone.
public class NteNotificationService : INteNotificationService
{
    private const string ChannelSms = "Sms";
    private const string ChannelEmail = "Email";

    private const string CsTypeConfig = "NteConfig";
    private const string CsTypeTemplate = "NteTemplate";

    private readonly IDataService _dataService;
    private readonly INteQueryRepository _queryRepository;
    private readonly INteSpendCalculator _spendCalculator;
    private readonly IAuditNteRepository _auditRepository;
    private readonly ISmsService _smsService;
    private readonly IEmailService _emailService;
    private readonly ILogger<NteNotificationService> _logger;

    public NteNotificationService(
        IDataService dataService,
        INteQueryRepository queryRepository,
        INteSpendCalculator spendCalculator,
        IAuditNteRepository auditRepository,
        ISmsService smsService,
        IEmailService emailService,
        ILogger<NteNotificationService> logger)
    {
        _dataService = dataService;
        _queryRepository = queryRepository;
        _spendCalculator = spendCalculator;
        _auditRepository = auditRepository;
        _smsService = smsService;
        _emailService = emailService;
        _logger = logger;
    }

    // Everything read from ConfigSetting once per scan.
    private sealed class ScanConfig
    {
        public List<int> Thresholds { get; init; } = new();
        public int MaxOpenCheckInHours { get; init; } = INteSpendCalculator.DefaultMaxOpenCheckInHours;
        public string SmsFrom { get; init; } = string.Empty;
        public string EmailFrom { get; init; } = string.Empty;
        public List<string> OverrideMobiles { get; init; } = new();   // raw values as typed
        public List<string> OverrideEmails { get; init; } = new();
        public HashSet<int> EnabledTechIds { get; init; } = new();    // empty = all techs
        public Dictionary<string, string> Templates { get; init; } = new();
    }

    public async Task<NteScanResult> RunScanAsync(CancellationToken ct)
    {
        var result = new NteScanResult();

        var enabledRaw = await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "Enabled");
        if (!string.Equals(enabledRaw, "true", StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogInformation("NTE notification scan skipped - NteConfig/Enabled is not 'true'");
            return result;
        }

        var config = await LoadConfigAsync();
        if (config.Thresholds.Count == 0)
        {
            _logger.LogWarning("NTE notification scan skipped - no thresholds configured");
            return result;
        }

        // A previous scan that died mid-send (process stopped, SMTP hang) leaves rows in
        // 'Sending'. Release them so this scan can retry; the send timeout is 60 s, so
        // anything older than 15 minutes is definitely not still in flight.
        var abandoned = await _auditRepository.AbandonStaleSendingAsync(TimeSpan.FromMinutes(15), ct);
        if (abandoned > 0)
        {
            _logger.LogWarning("NTE scan: {Count} notification(s) stuck in Sending were marked Failed for retry", abandoned);
        }

        // Only SRs with a tech checked in right now (D9); one row per checked-in tech.
        var rows = await _queryRepository.GetActiveServiceRequestsAsync(config.MaxOpenCheckInHours, ct);
        result.RowsScanned = rows.Count;
        if (rows.Count == 0)
        {
            _logger.LogInformation("NTE scan: no service request with an NTE has a tech checked in; nothing to do");
            return result;
        }

        // One set-based spend query per scan; rows without a spend entry are
        // SRs that dropped out between the two queries (closed, NTE cleared).
        var spendBySr = await _spendCalculator.CalculateActiveAsync(config.MaxOpenCheckInHours, ct);

        // Fired-key lookups are per SR; cache them so two recipients on one SR share a lookup.
        var firedBySr = new Dictionary<int, HashSet<(string Key, string Channel)>>();

        foreach (var row in rows)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                if (!spendBySr.TryGetValue(row.SrId, out var spend))
                {
                    _logger.LogDebug("NTE scan: no spend row for SR {SrId}; skipping", row.SrId);
                    continue;
                }

                // Nothing crossed: skip before touching the audit table.
                if (!config.Thresholds.Any(t => spend.Percent >= t)) continue;

                // Rollout valve: techs not on the list are left alone entirely (no audit row),
                // so they get the real alert once the list is cleared.
                if (config.EnabledTechIds.Count > 0 &&
                    (row.TechUserId == null || !config.EnabledTechIds.Contains(row.TechUserId.Value)))
                {
                    _logger.LogDebug("NTE scan: SR {SrId} at {Percent}% but tech {TechId} is not in NteConfig/EnabledTechIds; skipping",
                        row.SrId, spend.Percent, row.TechUserId);
                    continue;
                }

                if (!firedBySr.TryGetValue(row.SrId, out var fired))
                {
                    fired = await _auditRepository.GetFiredAsync(row.SrId, ct);
                    firedBySr[row.SrId] = fired;
                }

                await ProcessRowAsync(row, spend, fired, config, result, ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "NTE scan failed for SR {SrId}", row.SrId);
                result.Failed++;
                result.Errors.Add($"SR {row.SrId}: {ex.Message}");
            }
        }

        _logger.LogInformation(
            "NTE scan complete. Scanned={Scanned}, Sms={Sms}, Email={Email}, Skipped={Skipped}, Failed={Failed}",
            result.RowsScanned, result.SmsSent, result.EmailsSent, result.Skipped, result.Failed);

        return result;
    }

    private async Task ProcessRowAsync(
        NteServiceRequestRow row,
        NteSpendResult spend,
        HashSet<(string Key, string Channel)> fired,
        ScanConfig config,
        NteScanResult result,
        CancellationToken ct)
    {
        var nteKey = row.Nte.ToString("0.00", CultureInfo.InvariantCulture);
        var crossed = config.Thresholds.Where(t => spend.Percent >= t).OrderBy(t => t).ToList();

        // Only the highest crossed threshold is sent per channel. Lower ones that were never
        // sent (the SR was first seen already past them) are logged as Skipped/superseded so
        // they occupy their key and do not fire later. A 75% text that already went out in an
        // earlier scan is untouched.
        var highestSms = crossed.Last();
        var highestEmail = crossed.Where(t => t >= 100).DefaultIfEmpty(0).Max();

        foreach (var threshold in crossed)
        {
            // SMS leg: every threshold, once per tech.
            var smsKey = $"{threshold}@{nteKey}|u{row.TechUserId ?? 0}";
            if (!fired.Contains((smsKey, ChannelSms)))
            {
                if (threshold == highestSms)
                    await SendSmsAsync(row, threshold, spend, smsKey, config, result, ct);
                else
                    await LogSkippedAsync(NewEntry(row, spend, ChannelSms, threshold, smsKey, config),
                        $"Superseded by the {highestSms}% alert sent in the same scan", ct);
                fired.Add((smsKey, ChannelSms));
            }

            // Email leg: thresholds >= 100, once per zone.
            if (threshold < 100) continue;
            var emailKey = $"{threshold}@{nteKey}|z{row.ZoneId ?? 0}";
            if (!fired.Contains((emailKey, ChannelEmail)))
            {
                if (threshold == highestEmail)
                    await SendEmailAsync(row, threshold, spend, emailKey, config, result, ct);
                else
                    await LogSkippedAsync(NewEntry(row, spend, ChannelEmail, threshold, emailKey, config),
                        $"Superseded by the {highestEmail}% alert sent in the same scan", ct);
                fired.Add((emailKey, ChannelEmail));
            }
        }
    }

    private async Task SendSmsAsync(
        NteServiceRequestRow row, int threshold, NteSpendResult spend, string key,
        ScanConfig config, NteScanResult result, CancellationToken ct)
    {
        var entry = NewEntry(row, spend, ChannelSms, threshold, key, config);

        // Where it should go, regardless of overrides: this is what the audit shows as intended.
        string? techNumber = null;
        string? skipReason = null;
        if (row.TechUserId == null)
        {
            skipReason = "Checked-in tech could not be resolved to a user";
        }
        else
        {
            techNumber = NormalizeToE164(row.TechMobile);
            if (techNumber == null)
            {
                skipReason = string.IsNullOrWhiteSpace(row.TechMobile)
                    ? $"{row.TechFullName} has no Evo Cell or Personal Cell on file"
                    : $"{row.TechFullName} phone '{row.TechMobile}' is not a valid number";
            }
        }
        entry.IntendedRecipient = techNumber ?? row.TechMobile;

        // Where it actually goes.
        List<string> recipients;
        if (config.OverrideMobiles.Count > 0)
        {
            recipients = config.OverrideMobiles.Select(NormalizeToE164).Where(n => n != null).Select(n => n!).ToList();
            if (recipients.Count == 0)
            {
                await LogSkippedAsync(entry, $"SmokeTestOverrideMobile '{string.Join(";", config.OverrideMobiles)}' holds no valid phone number", ct);
                result.Skipped++;
                return;
            }
            entry.Overridden = true;
        }
        else if (techNumber == null)
        {
            await LogSkippedAsync(entry, skipReason!, ct);
            result.Skipped++;
            return;
        }
        else
        {
            recipients = new List<string> { techNumber };
        }

        if (!config.Templates.TryGetValue($"Sms{threshold}Body", out var bodyTemplate))
        {
            _logger.LogWarning("Missing template NteTemplate/Sms{Threshold}Body - skipping SMS for SR {SrId}", threshold, row.SrId);
            return;
        }

        if (string.IsNullOrWhiteSpace(config.SmsFrom))
        {
            await LogSkippedAsync(entry, "NteConfig/AcsSmsFrom is empty", ct);
            result.Skipped++;
            return;
        }

        entry.Body = RenderTemplate(bodyTemplate, row, spend, threshold);
        entry.Recipient = string.Join(";", recipients);
        entry.Status = "Sending";

        var anId = await _auditRepository.TryInsertSendingAsync(entry, ct);
        if (anId == null) return; // race - another instance got it first

        // One text per override number; the audit row lists them all. A failure on any number
        // marks the row Failed (and names the number) so the next scan retries the whole row.
        try
        {
            foreach (var to in recipients)
            {
                try
                {
                    await _smsService.SendSmsAsync(config.SmsFrom, to, entry.Body, ct);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"{to}: {ex.Message}", ex);
                }
            }
            await _auditRepository.UpdateStatusAsync(anId.Value, "Sent", null, ct);
            result.SmsSent++;
        }
        catch (Exception ex)
        {
            await _auditRepository.UpdateStatusAsync(anId.Value, "Failed", ex.Message, ct);
            result.Failed++;
            result.Errors.Add($"SR {row.SrId} SMS@{threshold}: {ex.Message}");
        }
    }

    private async Task SendEmailAsync(
        NteServiceRequestRow row, int threshold, NteSpendResult spend, string key,
        ScanConfig config, NteScanResult result, CancellationToken ct)
    {
        var entry = NewEntry(row, spend, ChannelEmail, threshold, key, config);

        string? zoneEmail = null;
        string? skipReason = null;
        if (row.TechUserId == null)
            skipReason = "Checked-in tech could not be resolved to a user, so no zone to email";
        else if (row.ZoneId == null)
            skipReason = $"{row.TechFullName} has no zone in the Employee Directory";
        else if (string.IsNullOrWhiteSpace(row.ZoneEmail))
            skipReason = $"Zone '{row.ZoneName}' has no email address";
        else
            zoneEmail = row.ZoneEmail!.Trim();
        entry.IntendedRecipient = zoneEmail;

        string recipient;
        if (config.OverrideEmails.Count > 0)
        {
            recipient = string.Join(";", config.OverrideEmails);
            entry.Overridden = true;
        }
        else if (zoneEmail == null)
        {
            await LogSkippedAsync(entry, skipReason!, ct);
            result.Skipped++;
            return;
        }
        else
        {
            recipient = zoneEmail;
        }

        if (!config.Templates.TryGetValue($"Email{threshold}Subject", out var subjectTemplate) ||
            !config.Templates.TryGetValue($"Email{threshold}Body", out var bodyTemplate))
        {
            _logger.LogWarning(
                "Missing template NteTemplate/Email{Threshold}Subject or Body - skipping email for SR {SrId}",
                threshold, row.SrId);
            return;
        }

        if (string.IsNullOrWhiteSpace(config.EmailFrom))
        {
            await LogSkippedAsync(entry, "NteConfig/EmailFrom is empty", ct);
            result.Skipped++;
            return;
        }

        entry.Subject = RenderTemplate(subjectTemplate, row, spend, threshold);
        entry.Body = RenderTemplate(bodyTemplate, row, spend, threshold);
        entry.Recipient = recipient;
        entry.Status = "Sending";

        var anId = await _auditRepository.TryInsertSendingAsync(entry, ct);
        if (anId == null) return;

        // The SMTP sender accepts the semicolon list directly.
        var (sent, error) = await _emailService.TrySendEmailAsync(config.EmailFrom, recipient, entry.Subject, entry.Body);
        if (sent)
        {
            await _auditRepository.UpdateStatusAsync(anId.Value, "Sent", null, ct);
            result.EmailsSent++;
        }
        else
        {
            await _auditRepository.UpdateStatusAsync(anId.Value, "Failed", error, ct);
            result.Failed++;
            result.Errors.Add($"SR {row.SrId} Email@{threshold}: {error}");
        }
    }

    private async Task LogSkippedAsync(AuditNteEntry entry, string reason, CancellationToken ct)
    {
        _logger.LogWarning("NTE {Channel} skipped for SR {SrId} ({Key}): {Reason}", entry.Channel, entry.SrId, entry.Key, reason);
        entry.Status = "Skipped";
        entry.Recipient = null;
        entry.ErrorMessage = reason;
        entry.Description += " - skipped: " + reason;
        await _auditRepository.TryInsertSendingAsync(entry, ct);
    }

    // The audit row before we know where it went or how it ended.
    private static AuditNteEntry NewEntry(
        NteServiceRequestRow row, NteSpendResult spend, string channel, int threshold, string key, ScanConfig config)
    {
        var what = channel == ChannelSms ? "SMS" : "Email";
        var who = channel == ChannelSms
            ? (row.TechUserId == null ? "unknown tech" : row.TechFullName)
            : (row.ZoneName ?? "unknown zone") + " zone";

        return new AuditNteEntry
        {
            Channel = channel,
            Threshold = threshold,
            Key = key,
            SrId = row.SrId,
            SrNumber = row.SrNumber,
            TechUserId = row.TechUserId,
            TechName = row.TechUserId == null ? null : row.TechFullName,
            ZoneId = row.ZoneId,
            ZoneName = row.ZoneName,
            Nte = row.Nte,
            Spent = spend.Spent,
            Percent = spend.Percent,
            ServerName = Environment.MachineName,
            Description = $"{what} {threshold}% to {who} for SR {row.SrNumber} ({spend.Percent:0.#}% of NTE)",
            Detail = JsonSerializer.Serialize(new
            {
                nte = row.Nte,
                spent = spend.Spent,
                percent = spend.Percent,
                labor = spend.Labor,
                serviceItems = spend.ServiceItems,
                tripCharge = spend.TripCharge,
                openCheckIns = spend.OpenCheckIns,
                staleCheckIns = spend.StaleCheckIns,
                techMobileSource = row.TechMobileSource,
                customer = row.CustomerName,
                location = row.LocationName,
                enabledTechIds = config.EnabledTechIds.Count == 0 ? null : string.Join(",", config.EnabledTechIds)
            })
        };
    }

    private async Task<ScanConfig> LoadConfigAsync()
    {
        var thresholds = ParseThresholds(await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "Thresholds"));

        var templates = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var t in thresholds)
        {
            foreach (var name in new[] { $"Sms{t}Body", $"Email{t}Subject", $"Email{t}Body" })
            {
                var value = await _dataService.GetConfigSettingValueAsync(CsTypeTemplate, name);
                if (!string.IsNullOrWhiteSpace(value)) templates[name] = value;
            }
        }

        var maxOpenRaw = await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "MaxOpenCheckInHours");
        var maxOpenHours = int.TryParse(maxOpenRaw, out var parsedHours) && parsedHours > 0
            ? parsedHours
            : INteSpendCalculator.DefaultMaxOpenCheckInHours;

        return new ScanConfig
        {
            Thresholds = thresholds,
            MaxOpenCheckInHours = maxOpenHours,
            SmsFrom = (await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "AcsSmsFrom"))?.Trim() ?? string.Empty,
            EmailFrom = (await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "EmailFrom"))?.Trim() ?? string.Empty,
            OverrideMobiles = SplitList(await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "SmokeTestOverrideMobile")),
            OverrideEmails = SplitList(await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "SmokeTestOverrideEmail")),
            EnabledTechIds = ParseIds(await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "EnabledTechIds")),
            Templates = templates
        };
    }

    // Semicolon or comma separated, trimmed, blanks dropped.
    private static List<string> SplitList(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return new List<string>();
        return raw.Split(new[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => s.Length > 0)
            .ToList();
    }

    private static HashSet<int> ParseIds(string? raw)
    {
        var ids = new HashSet<int>();
        foreach (var s in SplitList(raw))
        {
            if (int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var id) && id > 0) ids.Add(id);
        }
        return ids;
    }

    private static List<int> ParseThresholds(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return new List<int>();

        return raw
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(s => s.Trim())
            .Where(s => int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            .Select(s => int.Parse(s, CultureInfo.InvariantCulture))
            .Where(t => t > 0)
            .Distinct()
            .OrderBy(t => t)
            .ToList();
    }

    // Employee Directory numbers are stored however the office typed them
    // ("6154979537", "(615) 497-9537", "615-497-9537"). ACS needs E.164.
    // US/Canada only: 10 digits, or 11 digits starting with 1. Anything else is invalid.
    public static string? NormalizeToE164(string? phone)
    {
        if (string.IsNullOrWhiteSpace(phone)) return null;
        var digits = Regex.Replace(phone, "[^0-9]", "");
        return digits.Length switch
        {
            10 => "+1" + digits,
            11 when digits[0] == '1' => "+" + digits,
            _ => null
        };
    }

    private static string RenderTemplate(string template, NteServiceRequestRow row, NteSpendResult spend, int threshold)
    {
        return template
            .Replace("{sr_number}", row.SrNumber ?? string.Empty)
            .Replace("{sr_id}", row.SrId.ToString(CultureInfo.InvariantCulture))
            .Replace("{percent}", spend.Percent.ToString("0.#", CultureInfo.InvariantCulture))
            .Replace("{threshold}", threshold.ToString(CultureInfo.InvariantCulture))
            .Replace("{nte}", row.Nte.ToString("0.00", CultureInfo.InvariantCulture))
            .Replace("{spent}", spend.Spent.ToString("0.00", CultureInfo.InvariantCulture))
            .Replace("{labor}", spend.Labor.ToString("0.00", CultureInfo.InvariantCulture))
            .Replace("{service_items}", spend.ServiceItems.ToString("0.00", CultureInfo.InvariantCulture))
            .Replace("{trip_charge}", spend.TripCharge.ToString("0.00", CultureInfo.InvariantCulture))
            .Replace("{tech_firstname}", row.TechFirstName ?? string.Empty)
            .Replace("{tech_name}", row.TechFullName)
            .Replace("{zone}", row.ZoneName ?? string.Empty)
            .Replace("{customer}", row.CustomerName ?? string.Empty)
            .Replace("{location}", row.LocationName ?? string.Empty);
    }
}
