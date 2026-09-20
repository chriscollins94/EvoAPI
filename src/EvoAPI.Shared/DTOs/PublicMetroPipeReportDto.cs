namespace EvoAPI.Shared.DTOs;

/// Payload of GET EvoApi/public/metro-pipe: both halves of the Metro Pipe Program Report in
/// one anonymous, key-protected call. Shapes are identical to the two admin endpoints so the
/// evotech report component renders either source the same way.
public class PublicMetroPipeReportDto
{
    public List<HighVolumeReportDto> HighVolume { get; set; } = new();
    public MetroPipeReportDto MetroPipe { get; set; } = new();
}
