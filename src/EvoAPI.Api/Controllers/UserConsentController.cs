using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("EvoApi/userconsent")]
    [EvoAuthorize]
    public class UserConsentController : BaseController
    {
        private readonly IUserConsentRepository _userConsentRepository;
        private readonly IDataService _dataService;

        public UserConsentController(
            IUserConsentRepository userConsentRepository,
            IDataService dataService,
            IAuditService auditService)
        {
            _userConsentRepository = userConsentRepository;
            _dataService = dataService;
            InitializeAuditService(auditService);
        }

        private bool IsTech => User.FindAll("function").Any(c => c.Value == "TECH");

        [HttpGet("sms/status")]
        public async Task<ActionResult<ApiResponse<SmsConsentStatusResponse>>> GetSmsStatus(CancellationToken ct)
        {
            try
            {
                var response = new SmsConsentStatusResponse { ConsentRequired = false, PhoneNumber = null };

                if (IsTech)
                {
                    response.PhoneNumber = await GetMobileOnFileAsync(UserId);

                    var optedIn = await _userConsentRepository.HasOptedInAsync(
                        UserId, UserConsentConstants.NteSmsAlerts, UserConsentConstants.NteSmsDisclosureVersion, ct);
                    response.ConsentRequired = !optedIn;
                }

                return Ok(new ApiResponse<SmsConsentStatusResponse>
                {
                    Success = true,
                    Message = "Consent status retrieved",
                    Data = response,
                    Count = 1
                });
            }
            catch (Exception ex)
            {
                await LogAuditErrorAsync("GetSmsConsentStatus", ex);
                return StatusCode(500, new ApiResponse<SmsConsentStatusResponse> { Success = false, Message = "Failed to retrieve consent status" });
            }
        }

        [HttpPost("sms/accept")]
        public async Task<ActionResult<ApiResponse<bool>>> AcceptSmsConsent(CancellationToken ct)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                // Number, type and version are all derived server-side so the stored
                // record always reflects what the screen displayed, not client input.
                var phone = await GetMobileOnFileAsync(UserId);
                if (phone == null)
                {
                    return BadRequest(new ApiResponse<bool> { Success = false, Message = "No mobile number on file" });
                }

                var alreadyOptedIn = await _userConsentRepository.HasOptedInAsync(
                    UserId, UserConsentConstants.NteSmsAlerts, UserConsentConstants.NteSmsDisclosureVersion, ct);

                if (!alreadyOptedIn)
                {
                    await _userConsentRepository.InsertAsync(new UserConsentRecord
                    {
                        UserId = UserId,
                        ConsentType = UserConsentConstants.NteSmsAlerts,
                        ContactValue = phone,
                        Status = UserConsentConstants.StatusOptedIn,
                        DisclosureVersion = UserConsentConstants.NteSmsDisclosureVersion,
                        Source = UserConsentConstants.SourceConnectAppOptInScreen,
                        IpAddress = ClientIPAddress,
                        UserAgent = Truncate(UserAgent, 500)
                    }, ct);
                }

                stopwatch.Stop();
                await LogAuditAsync("AcceptSmsConsent",
                    new { userId = UserId, phone, version = UserConsentConstants.NteSmsDisclosureVersion, alreadyOptedIn },
                    stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<bool> { Success = true, Message = "Consent recorded", Data = true, Count = 1 });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("AcceptSmsConsent", ex);
                return StatusCode(500, new ApiResponse<bool> { Success = false, Message = "Failed to record consent" });
            }
        }

        /// Prefer the work-provided cell (u_phonemobile); fall back to the personal
        /// number (u_phonehome) when the mobile is blank or unusable.
        private async Task<string?> GetMobileOnFileAsync(int userId)
        {
            const string sql = "SELECT u_phonemobile, u_phonehome FROM [User] WHERE u_id = @UserId";
            var result = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object> { { "@UserId", userId } });
            if (result.Rows.Count == 0) return null;

            var mobile = NormalizeToE164(result.Rows[0]["u_phonemobile"]?.ToString());
            if (mobile != null) return mobile;
            return NormalizeToE164(result.Rows[0]["u_phonehome"]?.ToString());
        }

        /// u_phonemobile has no enforced format; accept 10-digit US numbers (with or
        /// without punctuation) or 11 digits with a leading 1. Anything else is
        /// treated as no-number-on-file so the tech is told to contact their admin.
        private static string? NormalizeToE164(string? raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return null;
            var digits = new string(raw.Where(char.IsDigit).ToArray());
            if (digits.Length == 11 && digits[0] == '1') digits = digits.Substring(1);
            return digits.Length == 10 ? $"+1{digits}" : null;
        }

        private static string? Truncate(string? value, int maxLength)
            => value != null && value.Length > maxLength ? value.Substring(0, maxLength) : value;
    }
}
