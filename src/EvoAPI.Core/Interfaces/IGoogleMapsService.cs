using EvoAPI.Shared.Models;

namespace EvoAPI.Core.Interfaces;

public interface IGoogleMapsService
{
    Task<GoogleMapsDistanceResult?> GetDistanceAndDurationAsync(string origin, string destination);
    Task<List<GoogleMapsDistanceResult>> GetDistanceMatrixAsync(List<string> origins, List<string> destinations);
}
