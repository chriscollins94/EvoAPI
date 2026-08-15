using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

public interface IQuickBooksService
{
    /// <summary>
    /// Looks up an SR by request number, calls QuickBooks for the matching invoice,
    /// and returns the comparison between DB and QB sync tokens. Handles a one-time
    /// access-token refresh on 401.
    /// </summary>
    Task<QuickBooksInvoiceCheckDto> CheckInvoiceByRequestNumberAsync(string requestNumber);
}
