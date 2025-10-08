namespace EvoAPI.Shared.Models;

public class GoogleMapsDistanceResult
{
    public string Origin { get; set; } = string.Empty;
    public string Destination { get; set; } = string.Empty;
    public int DistanceMeters { get; set; }
    public string DistanceText { get; set; } = string.Empty;
    public int DurationSeconds { get; set; }
    public string DurationText { get; set; } = string.Empty;
    public int DurationInTrafficSeconds { get; set; }
    public string DurationInTrafficText { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public bool IsSuccess => Status == "OK";
}

public class GoogleMapsApiResponse
{
    public string Status { get; set; } = string.Empty;
    public List<GoogleMapsRow> Rows { get; set; } = new List<GoogleMapsRow>();
}

public class GoogleMapsRow
{
    public List<GoogleMapsElement> Elements { get; set; } = new List<GoogleMapsElement>();
}

public class GoogleMapsElement
{
    public GoogleMapsDistance? Distance { get; set; }
    public GoogleMapsDuration? Duration { get; set; }
    public GoogleMapsDuration? Duration_in_traffic { get; set; }
    public string Status { get; set; } = string.Empty;
}

public class GoogleMapsDistance
{
    public string Text { get; set; } = string.Empty;
    public int Value { get; set; }
}

public class GoogleMapsDuration
{
    public string Text { get; set; } = string.Empty;
    public int Value { get; set; }
}
