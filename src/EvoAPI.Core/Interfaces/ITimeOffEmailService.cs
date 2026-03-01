using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

/// <summary>
/// Specialized email service for time off workflow notifications.
/// Reads templates from ConfigSetting table and handles all routing logic.
/// </summary>
public interface ITimeOffEmailService
{
    /// <summary>
    /// Send same-day request notification to dispatch/admin (ToSameDay recipients)
    /// </summary>
    Task SendSameDayNotificationAsync(int torId, int userId, int tortdId, bool isTech);

    /// <summary>
    /// Send ZFM review notification when a tech submits a request
    /// </summary>
    Task SendZFMReviewNotificationAsync(int torId, int userId);

    /// <summary>
    /// Send admin escalation notification (from creation or ZFM approval)
    /// </summary>
    /// <param name="exceedingBalance">Whether the request exceeds available balance</param>
    /// <param name="balanceType">"vacation" or "PTO" - only used if exceedingBalance is true</param>
    Task SendAdminEscalationNotificationAsync(int torId, int userId, bool exceedingBalance = false, string? balanceType = null);

    /// <summary>
    /// Send approval notification to the requesting employee
    /// </summary>
    Task SendApprovalNotificationAsync(int torId, int userId);

    /// <summary>
    /// Send rejection notification to the requesting employee
    /// </summary>
    Task SendRejectionNotificationAsync(int torId, int userId, string noteReason);
}
