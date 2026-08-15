namespace EvoAPI.Core.Interfaces;

public interface ISmsService
{
    Task SendSmsAsync(string from, string to, string body, CancellationToken ct);
}
