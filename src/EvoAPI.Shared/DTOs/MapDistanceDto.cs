namespace EvoAPI.Shared.DTOs;

/// <summary>
/// Data transfer object for cached map distance data
/// </summary>
public class MapDistanceDto
{
    public int md_id { get; set; }
    public string md_address1 { get; set; } = string.Empty;
    public string md_address2 { get; set; } = string.Empty;
    public decimal? md_distance_miles { get; set; }
    public int? md_distance_meters { get; set; }
    public string? md_distance_text { get; set; }
    public int? md_traveltime_minutes { get; set; }
    public int? md_traveltime_seconds { get; set; }
    public string? md_traveltime_text { get; set; }
    public int? md_traveltime_traffic_minutes { get; set; }
    public int? md_traveltime_traffic_seconds { get; set; }
    public string? md_traveltime_traffic_text { get; set; }
    public DateTime md_created_date { get; set; }
    public DateTime md_last_updated { get; set; }
}

/// <summary>
/// Request object for saving map distance data to cache
/// </summary>
public class SaveMapDistanceRequest
{
    public string FromAddress { get; set; } = string.Empty;
    public string ToAddress { get; set; } = string.Empty;
    public decimal? DistanceMiles { get; set; }
    public int? DistanceMeters { get; set; }
    public string? DistanceText { get; set; }
    public int? TravelTimeMinutes { get; set; }
    public int? TravelTimeSeconds { get; set; }
    public string? TravelTimeText { get; set; }
    public int? TravelTimeTrafficMinutes { get; set; }
    public int? TravelTimeTrafficSeconds { get; set; }
    public string? TravelTimeTrafficText { get; set; }
}
