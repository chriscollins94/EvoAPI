namespace EvoAPI.Shared.DTOs;

/// Metro Pipe Program Report: the XRF half of the office High Volume report plus the
/// summaries both programs share (latest-wave totals and locations by crew). The per-tech
/// High Volume rows still come from GET reports/high-volume; this DTO adds everything else
/// the page needs for its XRF and Side by Side views.
///
/// "Window" below means the last five weekdays including today, Central time, the same
/// window the High Volume table shows. Day names ride along on each row so the client can
/// label columns without knowing the calendar.
public class MetroPipeReportDto
{
    /// Per-tech XRF submissions in the window, one row per tech plus a final 'TOTAL' row.
    public List<XrfReportRowDto> XrfRows { get; set; } = new();

    /// XRF submissions in the window by crew (xrfbd_team), one row per crew.
    public List<CrewReportRowDto> XrfCrews { get; set; } = new();

    /// High Volume completions in the window by crew (hvbd_team), one row per crew.
    public List<CrewReportRowDto> HvCrews { get; set; } = new();

    /// Every loaded wave with its submitted / remaining counts, newest first.
    public List<XrfWaveProgressDto> XrfWaves { get; set; } = new();

    /// Count of each result value across the window. Results with zero rows are omitted.
    public List<XrfResultCountDto> XrfResults { get; set; } = new();

    /// XRF stops loaded but not yet submitted, across all waves.
    public int XrfPending { get; set; }

    /// Most recently loaded XRF wave: its name, how many stops it holds, how many are submitted.
    /// Null name and zero counts when no wave has been loaded.
    public string? XrfWaveName { get; set; }
    public int XrfWaveTotal { get; set; }
    public int XrfDone { get; set; }

    /// Most recently loaded High Volume batch, same shape.
    public string? HvWaveName { get; set; }
    public int HvWaveTotal { get; set; }
    public int HvDone { get; set; }
}

/// Same shape as HighVolumeReportDto plus how many of the tech's window submissions were 'Complete'.
public class XrfReportRowDto
{
    public string Tech { get; set; } = string.Empty;
    public int Today { get; set; }
    public int Previous1 { get; set; }
    public int Previous2 { get; set; }
    public int Previous3 { get; set; }
    public int Previous4 { get; set; }
    public string TodayName { get; set; } = string.Empty;
    public string Previous1Name { get; set; } = string.Empty;
    public string Previous2Name { get; set; } = string.Empty;
    public string Previous3Name { get; set; } = string.Empty;
    public string Previous4Name { get; set; } = string.Empty;
    public int Complete { get; set; }
}

public class CrewReportRowDto
{
    public string Crew { get; set; } = string.Empty;
    public int Today { get; set; }
    public int Previous1 { get; set; }
    public int Previous2 { get; set; }
    public int Previous3 { get; set; }
    public int Previous4 { get; set; }
}

public class XrfWaveProgressDto
{
    public int BatchId { get; set; }
    public string Wave { get; set; } = string.Empty;
    public DateTime LoadedDate { get; set; }
    public int Total { get; set; }
    public int Done { get; set; }
    public int Pending { get; set; }
}

public class XrfResultCountDto
{
    public string Result { get; set; } = string.Empty;
    public int Count { get; set; }
}
