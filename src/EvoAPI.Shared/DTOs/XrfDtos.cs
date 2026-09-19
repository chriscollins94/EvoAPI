namespace EvoAPI.Shared.DTOs;

/// XRF revisits: a second trip to High Volume locations so the tech can shoot the
/// service line with the XRF analyzer. Rows live in XrfBatch / XrfBatchDetail and are
/// keyed by premise number back to HighVolumeBatchDetail. Independent of service
/// requests, work orders and billing.

/// The outcome a tech must pick when submitting a stop. Stored verbatim in xrfbd_result
/// (CHECK constraint mirrors this list). Any result finalizes the stop.
public static class XrfResults
{
    public const string Complete = "Complete";
    public const string Error = "Error";
    public const string Inaccessible = "Inaccessible";
    public const string DirtyWet = "Dirty/Wet";
    public const string NotUsed = "XRF Not Used";

    public static readonly string[] All = { Complete, Error, Inaccessible, DirtyWet, NotUsed };

    /// Returns the canonical spelling for a client value, or null when it is not one of the options.
    public static string? Normalize(string? value)
    {
        var trimmed = value?.Trim();
        if (string.IsNullOrEmpty(trimmed)) return null;
        return All.FirstOrDefault(r => string.Equals(r, trimmed, StringComparison.OrdinalIgnoreCase));
    }
}

/// One team with its submitted / total counts across all XRF waves (drives the team dropdown).
public class XrfTeamDto
{
    public string Team { get; set; } = string.Empty;
    public int TotalCount { get; set; }
    public int CompletedCount { get; set; }
}

/// Which stops the page is asking for. 'today' is measured in Central time on the server.
public static class XrfViews
{
    public const string Incomplete = "incomplete";
    public const string SubmittedToday = "today";
    public const string SubmittedAll = "submitted";

    public static string Normalize(string? value)
    {
        var v = value?.Trim().ToLowerInvariant();
        return v == SubmittedToday || v == SubmittedAll ? v : Incomplete;
    }
}

public class XrfBatchDto
{
    public int XrfbId { get; set; }
    public string Filename { get; set; } = string.Empty;
    public DateTime InsertDateTime { get; set; }
    public int TotalCount { get; set; }
    public int CompletedCount { get; set; }
}

/// One XRF location: the XrfBatchDetail row plus the matched High Volume row and that
/// visit's checklist answers, all read-only on the page except the XRF completion.
public class XrfLocationDto
{
    // XRF row
    public int XrfbdId { get; set; }
    public int XrfbId { get; set; }
    public string BatchName { get; set; } = string.Empty;
    public int? HvbdId { get; set; }
    /// The key back to High Volume (hvbd_premisenumber).
    public string PremiseNumber { get; set; } = string.Empty;
    public string? Team { get; set; }
    public string? Comment { get; set; }
    public string? Result { get; set; }
    public DateTime? CompletedDateTime { get; set; }
    public int? CompletedByUserId { get; set; }
    public string? CompletedByName { get; set; }
    public decimal? Latitude { get; set; }
    public decimal? Longitude { get; set; }
    public int? GeoAccuracy { get; set; }
    /// Optional photo taken at submit (Attachment row); null when none was attached.
    public int? AttId { get; set; }
    public string? AttFilename { get; set; }

    // Matched High Volume row; all null when the meter had no High Volume match
    public int? SrId { get; set; }
    public string? SrRequestNumber { get; set; }
    public string? Address { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? Zip { get; set; }
    /// High Volume's meter number, informational only (the XRF row's own meter is never returned).
    public string? MeterNumber { get; set; }
    public string? MeterLocation { get; set; }
    public string? Stanpar { get; set; }
    public string? Instructions { get; set; }
    public double? PremiseLatitude { get; set; }
    public double? PremiseLongitude { get; set; }
    public string? HvTeam { get; set; }
    public string? HvBatchName { get; set; }
    public DateTime? HvCompletedDateTime { get; set; }
    public string? HvCompletedByName { get; set; }

    /// Latest answer per checklist question from the High Volume visit, ordered by xsrcla_order.
    public List<XrfHvAnswerDto> Answers { get; set; } = new();
}

public class XrfHvAnswerDto
{
    public int SrId { get; set; }
    public int ClqId { get; set; }
    public string Question { get; set; } = string.Empty;
    public string? Answer { get; set; }
    public int? Order { get; set; }
    public int? AttId { get; set; }
    public string? AttFilename { get; set; }
}

public class XrfCompleteRequest
{
    public int XrfbdId { get; set; }
    /// One of XrfResults.All; required.
    public string? Result { get; set; }
    public string? Comment { get; set; }
    public double? Latitude { get; set; }
    public double? Longitude { get; set; }
    public int? GeoAccuracy { get; set; }
    /// Attachment id of the photo already uploaded through the attachment service; optional.
    public int? AttId { get; set; }
}

public class XrfCompleteResult
{
    public int XrfbdId { get; set; }
    public string Result { get; set; } = string.Empty;
    public DateTime CompletedDateTime { get; set; }
    public string CompletedByName { get; set; } = string.Empty;
    public bool LocationCaptured { get; set; }
}

public enum XrfCompleteStatus
{
    Completed,
    NotFound,
    AlreadyCompleted
}

public class XrfCompleteOutcome
{
    public XrfCompleteStatus Status { get; set; }
    public XrfCompleteResult? Result { get; set; }
}
