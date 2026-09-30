using EvoAPI.Shared.DTOs;

namespace EvoAPI.Core.Interfaces;

/// <summary>Current weather and outdoor temperature at a point, captured when a tech checks in to a PM visit (PM build slice 4a).</summary>
public interface IWeatherService
{
    /// <summary>Null when the provider cannot be reached or returns nothing usable; never throws.</summary>
    Task<WeatherResultDto?> GetCurrentAsync(decimal latitude, decimal longitude);
}
