using System.Globalization;
using System.Text.Json;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Logging;

namespace EvoAPI.Infrastructure.Services;

public class NteNotificationService : INteNotificationService
{
    private const string NotificationType = "NteThresholdCrossed";
    private const string EntityType = "ServiceRequest";
    private const string ChannelSms = "Sms";
    private const string ChannelEmail = "Email";

    private const string CsTypeConfig = "NteConfig";
    private const string CsTypeTemplate = "NteTemplate";

    private readonly IDataService _dataService;
    private readonly INteQueryRepository _queryRepository;
    private readonly INteSpendCalculator _spendCalculator;
    private readonly INotificationLogRepository _logRepository;
    private readonly ISmsService _smsService;
    private readonly AcsEmailService _emailService;
    private readonly ILogger<NteNotificationService> _logger;

    public NteNotificationService(
        IDataService dataService,
        INteQueryRepository queryRepository,
        INteSpendCalculator spendCalculator,
        INotificationLogRepository logRepository,
        ISmsService smsService,
        AcsEmailService emailService,
        ILogger<NteNotificationService> logger)
    {
        _dataService = dataService;
        _queryRepository = queryRepository;
        _spendCalculator = spendCalculator;
        _logRepository = logRepository;
        _smsService = smsService;
        _emailService = emailService;
        _logger = logger;
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

        var thresholds = await LoadThresholdsAsync();
        if (thresholds.Count == 0)
        {
            _logger.LogWarning("NTE notification scan skipped - no thresholds configured");
            return result;
        }

        var smsFrom = (await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "AcsSmsFrom")) ?? string.Empty;
        var emailFrom = (await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "AcsEmailFrom")) ?? string.Empty;
        var overrideMobile = await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "SmokeTestOverrideMobile");
        var overrideEmail = await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "SmokeTestOverrideEmail");

        var rows = await _queryRepository.GetActiveServiceRequestsAsync(ct);
        result.RowsScanned = rows.Count;

        foreach (var row in rows)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await ProcessRowAsync(row, thresholds, smsFrom, emailFrom, overrideMobile, overrideEmail, result, ct);
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
        List<int> thresholds,
        string smsFrom,
        string emailFrom,
        string? overrideMobile,
        string? overrideEmail,
        NteScanResult result,
        CancellationToken ct)
    {
        var spend = await _spendCalculator.CalculateAsync(row, ct);
        var fired = await _logRepository.GetFiredAsync(NotificationType, EntityType, row.SrId, ct);

        var metadata = JsonSerializer.Serialize(new
        {
            nte = row.Nte,
            spent = spend.Spent,
            percent = spend.Percent
        });

        foreach (var threshold in thresholds.Where(t => spend.Percent >= t).OrderBy(t => t))
        {
            var key = threshold.ToString(CultureInfo.InvariantCulture);

            // SMS leg fires at every threshold.
            if (!fired.Contains((key, ChannelSms)))
            {
                await SendSmsAsync(row, threshold, spend, key, metadata, smsFrom, overrideMobile, result, ct);
            }

            // Email leg fires at 100% and above (zone email gets the breach notice).
            if (threshold >= 100 && !fired.Contains((key, ChannelEmail)))
            {
                await SendEmailAsync(row, threshold, spend, key, metadata, emailFrom, overrideEmail, result, ct);
            }
        }
    }

    private async Task SendSmsAsync(
        NteServiceRequestRow row,
        int threshold,
        NteSpendResult spend,
        string key,
        string metadata,
        string smsFrom,
        string? overrideMobile,
        NteScanResult result,
        CancellationToken ct)
    {
        var recipient = !string.IsNullOrWhiteSpace(overrideMobile) ? overrideMobile! : row.TechMobile;
        if (string.IsNullOrWhiteSpace(recipient))
        {
            await LogSkippedAsync(row, threshold, key, ChannelSms, "(none)", body: null, subject: null,
                reason: "Tech has no mobile number on file", metadata: metadata, ct);
            result.Skipped++;
            return;
        }

        var bodyTemplate = await _dataService.GetConfigSettingValueAsync(CsTypeTemplate, $"Sms{threshold}Body");
        if (string.IsNullOrWhiteSpace(bodyTemplate))
        {
            _logger.LogWarning("Missing template NteTemplate/Sms{Threshold}Body - skipping SMS for SR {SrId}", threshold, row.SrId);
            return;
        }

        var body = RenderTemplate(bodyTemplate, row, spend, threshold);

        var entry = new NotificationLogEntry
        {
            Type = NotificationType,
            Key = key,
            EntityType = EntityType,
            EntityId = row.SrId,
            Channel = ChannelSms,
            Recipient = recipient,
            Subject = null,
            Body = body,
            Status = "Sending",
            Metadata = metadata
        };

        var nlId = await _logRepository.TryInsertSendingAsync(entry, ct);
        if (nlId == null) return; // race - another instance got it first

        try
        {
            await _smsService.SendSmsAsync(smsFrom, recipient, body, ct);
            await _logRepository.UpdateStatusAsync(nlId.Value, "Sent", null, ct);
            result.SmsSent++;
        }
        catch (Exception ex)
        {
            await _logRepository.UpdateStatusAsync(nlId.Value, "Failed", ex.Message, ct);
            result.Failed++;
            result.Errors.Add($"SR {row.SrId} SMS@{threshold}: {ex.Message}");
        }
    }

    private async Task SendEmailAsync(
        NteServiceRequestRow row,
        int threshold,
        NteSpendResult spend,
        string key,
        string metadata,
        string emailFrom,
        string? overrideEmail,
        NteScanResult result,
        CancellationToken ct)
    {
        var recipient = !string.IsNullOrWhiteSpace(overrideEmail) ? overrideEmail! : row.ZoneEmail;
        if (string.IsNullOrWhiteSpace(recipient))
        {
            await LogSkippedAsync(row, threshold, key, ChannelEmail, "(none)", body: null, subject: null,
                reason: "Zone has no email address on file", metadata: metadata, ct);
            result.Skipped++;
            return;
        }

        var subjectTemplate = await _dataService.GetConfigSettingValueAsync(CsTypeTemplate, $"Email{threshold}Subject");
        var bodyTemplate = await _dataService.GetConfigSettingValueAsync(CsTypeTemplate, $"Email{threshold}Body");
        if (string.IsNullOrWhiteSpace(subjectTemplate) || string.IsNullOrWhiteSpace(bodyTemplate))
        {
            _logger.LogWarning(
                "Missing template NteTemplate/Email{Threshold}Subject or Body - skipping email for SR {SrId}",
                threshold, row.SrId);
            return;
        }

        var subject = RenderTemplate(subjectTemplate, row, spend, threshold);
        var body = RenderTemplate(bodyTemplate, row, spend, threshold);

        var entry = new NotificationLogEntry
        {
            Type = NotificationType,
            Key = key,
            EntityType = EntityType,
            EntityId = row.SrId,
            Channel = ChannelEmail,
            Recipient = recipient,
            Subject = subject,
            Body = body,
            Status = "Sending",
            Metadata = metadata
        };

        var nlId = await _logRepository.TryInsertSendingAsync(entry, ct);
        if (nlId == null) return;

        try
        {
            await _emailService.SendEmailAsync(emailFrom, recipient, subject, body, ct);
            await _logRepository.UpdateStatusAsync(nlId.Value, "Sent", null, ct);
            result.EmailsSent++;
        }
        catch (Exception ex)
        {
            await _logRepository.UpdateStatusAsync(nlId.Value, "Failed", ex.Message, ct);
            result.Failed++;
            result.Errors.Add($"SR {row.SrId} Email@{threshold}: {ex.Message}");
        }
    }

    private async Task LogSkippedAsync(
        NteServiceRequestRow row, int threshold, string key, string channel,
        string recipientPlaceholder, string? body, string? subject, string reason, string metadata,
        CancellationToken ct)
    {
        var entry = new NotificationLogEntry
        {
            Type = NotificationType,
            Key = key,
            EntityType = EntityType,
            EntityId = row.SrId,
            Channel = channel,
            Recipient = recipientPlaceholder,
            Subject = subject,
            Body = body,
            Status = "Skipped",
            Error = reason,
            Metadata = metadata
        };
        await _logRepository.TryInsertSendingAsync(entry, ct);
    }

    private async Task<List<int>> LoadThresholdsAsync()
    {
        var raw = await _dataService.GetConfigSettingValueAsync(CsTypeConfig, "Thresholds");
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

    private static string RenderTemplate(string template, NteServiceRequestRow row, NteSpendResult spend, int threshold)
    {
        return template
            .Replace("{sr_number}", row.SrNumber ?? string.Empty)
            .Replace("{sr_id}", row.SrId.ToString(CultureInfo.InvariantCulture))
            .Replace("{percent}", spend.Percent.ToString("0.#", CultureInfo.InvariantCulture))
            .Replace("{threshold}", threshold.ToString(CultureInfo.InvariantCulture))
            .Replace("{nte}", row.Nte.ToString("0.00", CultureInfo.InvariantCulture))
            .Replace("{spent}", spend.Spent.ToString("0.00", CultureInfo.InvariantCulture))
            .Replace("{tech_firstname}", row.TechFirstName ?? string.Empty)
            .Replace("{tech_name}", row.TechFullName)
            .Replace("{zone}", row.ZoneName ?? string.Empty);
    }
}
