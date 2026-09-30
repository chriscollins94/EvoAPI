using System.Globalization;
using System.Text.Json;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Logging;

namespace EvoAPI.Infrastructure.Services;

/// <summary>
/// Weather at check-in from Open-Meteo (https://open-meteo.com): free, no API key, current conditions by coordinates.
/// Chosen for the PM POC on 2026-09-24 (plan Q A-1). The WMO weather code is turned into a short description.
/// Registered as a typed HttpClient in Program.cs.
/// </summary>
public class OpenMeteoWeatherService : IWeatherService
{
    private const string BaseUrl = "https://api.open-meteo.com/v1/forecast";
    private readonly HttpClient _http;
    private readonly ILogger<OpenMeteoWeatherService> _logger;

    public OpenMeteoWeatherService(HttpClient http, ILogger<OpenMeteoWeatherService> logger)
    {
        _http = http;
        _http.Timeout = TimeSpan.FromSeconds(8);
        _logger = logger;
    }

    public async Task<WeatherResultDto?> GetCurrentAsync(decimal latitude, decimal longitude)
    {
        var url = $"{BaseUrl}?latitude={latitude.ToString(CultureInfo.InvariantCulture)}&longitude={longitude.ToString(CultureInfo.InvariantCulture)}"
                + "&current=temperature_2m,weather_code,wind_speed_10m,relative_humidity_2m&temperature_unit=fahrenheit&wind_speed_unit=mph&timezone=auto";
        try
        {
            using var response = await _http.GetAsync(url);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning("Open-Meteo returned {Status} for {Lat},{Lon}", response.StatusCode, latitude, longitude);
                return null;
            }
            using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            if (!doc.RootElement.TryGetProperty("current", out var current)) return null;

            decimal? temp = current.TryGetProperty("temperature_2m", out var t) && t.ValueKind == JsonValueKind.Number ? Math.Round(t.GetDecimal(), 1) : null;
            var code = current.TryGetProperty("weather_code", out var c) && c.ValueKind == JsonValueKind.Number ? c.GetInt32() : -1;
            var wind = current.TryGetProperty("wind_speed_10m", out var w) && w.ValueKind == JsonValueKind.Number ? Math.Round(w.GetDecimal()) : (decimal?)null;
            var humidity = current.TryGetProperty("relative_humidity_2m", out var h) && h.ValueKind == JsonValueKind.Number ? h.GetInt32() : (int?)null;

            var description = Describe(code);
            if (wind.HasValue) description += $", wind {wind} mph";
            if (humidity.HasValue) description += $", {humidity}% humidity";

            return new WeatherResultDto { Description = description, TemperatureF = temp, Source = "Open-Meteo", ObservedAt = DateTime.Now };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Open-Meteo call failed for {Lat},{Lon}", latitude, longitude);
            return null;
        }
    }

    /// <summary>WMO weather interpretation codes as Open-Meteo documents them.</summary>
    public static string Describe(int code) => code switch
    {
        0 => "Clear sky",
        1 => "Mainly clear",
        2 => "Partly cloudy",
        3 => "Overcast",
        45 or 48 => "Fog",
        51 or 53 or 55 => "Drizzle",
        56 or 57 => "Freezing drizzle",
        61 => "Light rain",
        63 => "Rain",
        65 => "Heavy rain",
        66 or 67 => "Freezing rain",
        71 => "Light snow",
        73 => "Snow",
        75 => "Heavy snow",
        77 => "Snow grains",
        80 or 81 => "Rain showers",
        82 => "Violent rain showers",
        85 or 86 => "Snow showers",
        95 => "Thunderstorm",
        96 or 99 => "Thunderstorm with hail",
        _ => "Conditions unavailable"
    };
}
