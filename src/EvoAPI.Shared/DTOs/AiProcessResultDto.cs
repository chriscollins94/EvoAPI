namespace EvoAPI.Shared.DTOs;

// Returned by /EvoApi/ai/process. PDF endpoints stream bytes directly
// and don't use this wrapper.
public class AiProcessResultDto
{
    public string  Output         { get; set; } = string.Empty;
    public string  Model          { get; set; } = string.Empty;
    public double  ResponseTimeMs { get; set; }
    public int     InputWordCount { get; set; }
    public int     OutputWordCount{ get; set; }
}
