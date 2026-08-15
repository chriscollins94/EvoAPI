namespace EvoAPI.Core.Interfaces;

public interface INteNotificationService
{
    Task<NteScanResult> RunScanAsync(CancellationToken ct);
}

public class NteScanResult
{
    public int RowsScanned { get; set; }
    public int SmsSent { get; set; }
    public int EmailsSent { get; set; }
    public int Skipped { get; set; }
    public int Failed { get; set; }
    public List<string> Errors { get; } = new();
}
