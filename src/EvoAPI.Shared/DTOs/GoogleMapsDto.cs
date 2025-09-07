using System.Text.Json.Serialization;

namespace EvoAPI.Shared.DTOs;

public class GoogleMapsDistanceDto
{
    public bool Success { get; set; }
    public string Source { get; set; } = string.Empty;
    public DistanceDto Distance { get; set; } = new();
    public DurationDto Duration { get; set; } = new();
    public DurationDto? DurationInTraffic { get; set; }
    public string FromAddress { get; set; } = string.Empty;
    public string ToAddress { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
}

public class DistanceDto
{
    public int Meters { get; set; }
    public double Miles { get; set; }
    public string Text { get; set; } = string.Empty;
}

public class DurationDto
{
    public int Seconds { get; set; }
    public int Minutes { get; set; }
    public string Text { get; set; } = string.Empty;
}

// Google Maps API Response Models
public class GoogleMapsApiResponse
{
    [JsonPropertyName("status")]
    public string Status { get; set; } = string.Empty;

    [JsonPropertyName("error_message")]
    public string? ErrorMessage { get; set; }

    [JsonPropertyName("rows")]
    public List<GoogleMapsRow>? Rows { get; set; }
}

public class GoogleMapsRow
{
    [JsonPropertyName("elements")]
    public List<GoogleMapsElement>? Elements { get; set; }
}

public class GoogleMapsElement
{
    [JsonPropertyName("status")]
    public string Status { get; set; } = string.Empty;

    [JsonPropertyName("distance")]
    public GoogleMapsDistance? Distance { get; set; }

    [JsonPropertyName("duration")]
    public GoogleMapsDuration? Duration { get; set; }

    [JsonPropertyName("duration_in_traffic")]
    public GoogleMapsDuration? DurationInTraffic { get; set; }
}

public class GoogleMapsDistance
{
    [JsonPropertyName("text")]
    public string Text { get; set; } = string.Empty;

    [JsonPropertyName("value")]
    public int Value { get; set; }
}

public class GoogleMapsDuration
{
    [JsonPropertyName("text")]
    public string Text { get; set; } = string.Empty;

    [JsonPropertyName("value")]
    public int Value { get; set; }
}
