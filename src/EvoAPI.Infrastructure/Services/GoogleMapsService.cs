using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.SqlClient;
using System.Text.Json;
using System.Web;

namespace EvoAPI.Infrastructure.Services;

public class GoogleMapsService : IGoogleMapsService
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<GoogleMapsService> _logger;
    private readonly IConfiguration _configuration;
    private readonly IAuditService _auditService;
    private readonly string _apiKey;
    private const string DISTANCE_MATRIX_BASE_URL = "https://maps.googleapis.com/maps/api/distancematrix/json";

    public GoogleMapsService(HttpClient httpClient, IConfiguration configuration, ILogger<GoogleMapsService> logger, IAuditService auditService)
    {
        _httpClient = httpClient;
        _logger = logger;
        _configuration = configuration;
        _auditService = auditService;
        
        // Try multiple configuration sources to get the API key
        var configApiKey = configuration["GoogleMaps:ApiKey"];
        var envVar1 = configuration["GOOGLE_MAPS_API_KEY"];
        
        // Use the first available key, prioritizing environment variables
        _apiKey = envVar1 ?? configApiKey ?? throw new ArgumentException("Google Maps API key not configured");
    }

    public async Task<GoogleMapsDistanceResult?> GetDistanceAndDurationAsync(string origin, string destination)
    {
        if (string.IsNullOrWhiteSpace(origin) || string.IsNullOrWhiteSpace(destination))
        {
            _logger.LogWarning("Origin or destination is null or empty");
            return null;
        }

        try
        {
            // First check database cache
            var cachedResult = await GetCachedDistanceAsync(origin, destination);
            if (cachedResult != null)
            {
                _logger.LogInformation("Using cached distance data for {Origin} to {Destination}", origin, destination);
                return cachedResult;
            }

            // If not cached, call Google API
            var results = await CallGoogleMapsApiAsync(new List<string> { origin }, new List<string> { destination });
            var result = results.FirstOrDefault();

            // Cache the result if successful
            if (result != null && result.Status == "OK")
            {
                await CacheDistanceResultAsync(result);
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting distance and duration between {Origin} and {Destination}", origin, destination);
            return null;
        }
    }

    public async Task<List<GoogleMapsDistanceResult>> GetDistanceMatrixAsync(List<string> origins, List<string> destinations)
    {
        var results = new List<GoogleMapsDistanceResult>();

        if (!origins.Any() || !destinations.Any())
        {
            _logger.LogWarning("Origins or destinations list is empty");
            
            // Log empty request to audit table
            await _auditService.LogAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "GoogleMapsService",
                Description = "GetDistanceMatrixAsync - Empty Request",
                Detail = $"Origins count: {origins.Count}, Destinations count: {destinations.Count}",
                IPAddress = "System",
                UserAgent = "EvoAPI GoogleMapsService",
                MachineName = Environment.MachineName
            });
            
            return results;
        }

        try
        {
            var uncachedPairs = new List<(string origin, string destination)>();
            var cachedResults = new List<GoogleMapsDistanceResult>();

            // Check database cache for each origin-destination pair
            foreach (var origin in origins)
            {
                foreach (var destination in destinations)
                {
                    var cachedResult = await GetCachedDistanceAsync(origin, destination);
                    if (cachedResult != null)
                    {
                        cachedResults.Add(cachedResult);
                        _logger.LogDebug("Found cached data for {Origin} to {Destination}", origin, destination);
                    }
                    else
                    {
                        uncachedPairs.Add((origin, destination));
                    }
                }
            }

            // Add cached results
            results.AddRange(cachedResults);

            // If we have uncached pairs, call Google API
            if (uncachedPairs.Any())
            {
                var uncachedOrigins = uncachedPairs.Select(p => p.origin).Distinct().ToList();
                var uncachedDestinations = uncachedPairs.Select(p => p.destination).Distinct().ToList();

                _logger.LogInformation("Calling Google Maps API for {UncachedCount} uncached pairs", uncachedPairs.Count);
                var apiResults = await CallGoogleMapsApiAsync(uncachedOrigins, uncachedDestinations);

                // Cache new results
                foreach (var result in apiResults.Where(r => r.Status == "OK"))
                {
                    await CacheDistanceResultAsync(result);
                }

                results.AddRange(apiResults);
            }

            _logger.LogInformation("Returned {TotalCount} results ({CachedCount} cached, {ApiCount} from API)", 
                results.Count, cachedResults.Count, results.Count - cachedResults.Count);
                
            // Log final results summary to audit table
            await _auditService.LogAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "GoogleMapsService",
                Description = "GetDistanceMatrixAsync - Results Summary",
                Detail = $"Total results: {results.Count}, Cached: {cachedResults.Count}, From API: {results.Count - cachedResults.Count}",
                IPAddress = "System",
                UserAgent = "EvoAPI GoogleMapsService",
                MachineName = Environment.MachineName
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing distance matrix request");
        }

        return results;
    }

    private async Task<GoogleMapsDistanceResult?> GetCachedDistanceAsync(string origin, string destination)
    {
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                _logger.LogWarning("No connection string found for caching");
                return null;
            }

            const string sql = @"
                SELECT md_distance_miles, md_distance_text, md_traveltime_minutes, md_traveltime_text,
                       md_traveltime_traffic_minutes, md_traveltime_traffic_text
                FROM dbo.MapDistance 
                WHERE md_address1 = @Origin AND md_address2 = @Destination";

            using var connection = new SqlConnection(connectionString);
            using var command = new SqlCommand(sql, connection);
            command.Parameters.AddWithValue("@Origin", origin);
            command.Parameters.AddWithValue("@Destination", destination);

            await connection.OpenAsync();
            using var reader = await command.ExecuteReaderAsync();

            if (await reader.ReadAsync())
            {
                var distanceMiles = reader.IsDBNull("md_distance_miles") ? 0 : reader.GetDecimal("md_distance_miles");
                var durationMinutes = reader.IsDBNull("md_traveltime_minutes") ? 0 : reader.GetInt32("md_traveltime_minutes");
                var trafficMinutes = reader.IsDBNull("md_traveltime_traffic_minutes") ? durationMinutes : reader.GetInt32("md_traveltime_traffic_minutes");

                return new GoogleMapsDistanceResult
                {
                    Origin = origin,
                    Destination = destination,
                    Status = "OK",
                    DistanceMeters = (int)(distanceMiles * 1609.344m), // Convert miles to meters
                    DistanceText = reader.IsDBNull("md_distance_text") ? $"{distanceMiles:F1} mi" : reader.GetString("md_distance_text"),
                    DurationSeconds = durationMinutes * 60, // Convert minutes to seconds
                    DurationText = reader.IsDBNull("md_traveltime_text") ? $"{durationMinutes} mins" : reader.GetString("md_traveltime_text"),
                    DurationInTrafficSeconds = trafficMinutes * 60,
                    DurationInTrafficText = reader.IsDBNull("md_traveltime_traffic_text") ? $"{trafficMinutes} mins" : reader.GetString("md_traveltime_traffic_text")
                };
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving cached distance data for {Origin} to {Destination}", origin, destination);
        }

        return null;
    }

    private async Task CacheDistanceResultAsync(GoogleMapsDistanceResult result)
    {
        try
        {
            var connectionString = _configuration.GetConnectionString("DefaultConnection");
            if (string.IsNullOrEmpty(connectionString))
            {
                _logger.LogWarning("No connection string found for caching");
                return;
            }

            // Convert meters to miles and seconds to minutes
            var distanceMiles = (decimal)(result.DistanceMeters * 0.000621371m);
            var durationMinutes = result.DurationSeconds / 60;
            var trafficMinutes = result.DurationInTrafficSeconds / 60;

            const string sql = @"
                INSERT INTO dbo.MapDistance 
                (md_address1, md_address2, md_distance_miles, md_distance_text, 
                 md_traveltime_minutes, md_traveltime_text, md_traveltime_traffic_minutes, md_traveltime_traffic_text)
                VALUES 
                (@Address1, @Address2, @DistanceMiles, @DistanceText,
                 @TravelTimeMinutes, @TravelTimeText, @TrafficMinutes, @TrafficText)";

            using var connection = new SqlConnection(connectionString);
            using var command = new SqlCommand(sql, connection);
            
            command.Parameters.AddWithValue("@Address1", result.Origin);
            command.Parameters.AddWithValue("@Address2", result.Destination);
            command.Parameters.AddWithValue("@DistanceMiles", distanceMiles);
            command.Parameters.AddWithValue("@DistanceText", result.DistanceText ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@TravelTimeMinutes", durationMinutes);
            command.Parameters.AddWithValue("@TravelTimeText", result.DurationText ?? (object)DBNull.Value);
            command.Parameters.AddWithValue("@TrafficMinutes", trafficMinutes);
            command.Parameters.AddWithValue("@TrafficText", result.DurationInTrafficText ?? (object)DBNull.Value);

            await connection.OpenAsync();
            await command.ExecuteNonQueryAsync();

            _logger.LogDebug("Cached distance result: {Origin} to {Destination} = {Distance}, {Duration}", 
                result.Origin, result.Destination, result.DistanceText, result.DurationText);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error caching distance result for {Origin} to {Destination}", result.Origin, result.Destination);
        }
    }

    private async Task<List<GoogleMapsDistanceResult>> CallGoogleMapsApiAsync(List<string> origins, List<string> destinations)
    {
        var results = new List<GoogleMapsDistanceResult>();
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            var originParam = string.Join("|", origins.Select(HttpUtility.UrlEncode));
            var destinationParam = string.Join("|", destinations.Select(HttpUtility.UrlEncode));

            var url = $"{DISTANCE_MATRIX_BASE_URL}" +
                     $"?origins={originParam}" +
                     $"&destinations={destinationParam}" +
                     $"&departure_time=now" +
                     $"&traffic_model=best_guess" +
                     $"&units=imperial" +
                     $"&key={_apiKey}";

            _logger.LogInformation("Calling Google Maps API for {OriginCount} origins and {DestinationCount} destinations", 
                origins.Count, destinations.Count);

            // Log API call to audit table
            await _auditService.LogAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "GoogleMapsService",
                Description = "Google Maps Distance Matrix API Call",
                Detail = $"Origins: {origins.Count}, Destinations: {destinations.Count}",
                IPAddress = "System",
                UserAgent = "EvoAPI GoogleMapsService",
                MachineName = Environment.MachineName
            });

            var response = await _httpClient.GetAsync(url);
            response.EnsureSuccessStatusCode();

            var jsonContent = await response.Content.ReadAsStringAsync();
            var apiResponse = JsonSerializer.Deserialize<GoogleMapsApiResponse>(jsonContent, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

            stopwatch.Stop();

            if (apiResponse == null || apiResponse.Status != "OK")
            {
                _logger.LogWarning("Google Maps API returned status: {Status}", apiResponse?.Status ?? "null");
                
                // Log API failure to audit table
                await _auditService.LogErrorAsync(new AuditEntry
                {
                    Username = "SYSTEM",
                    Name = "GoogleMapsService",
                    Description = "Google Maps API Error Response",
                    Detail = $"Status: {apiResponse?.Status ?? "null"}, Response: {jsonContent}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                    IPAddress = "System",
                    UserAgent = "EvoAPI GoogleMapsService",
                    MachineName = Environment.MachineName,
                    IsError = true
                });
                
                return results;
            }

            // Log successful API call
            await _auditService.LogAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "GoogleMapsService",
                Description = "Google Maps API Success",
                Detail = $"Retrieved distance matrix data successfully. Response status: {apiResponse.Status}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI GoogleMapsService",
                MachineName = Environment.MachineName
            });

            // Parse the response matrix
            for (int originIndex = 0; originIndex < origins.Count && originIndex < apiResponse.Rows.Count; originIndex++)
            {
                var row = apiResponse.Rows[originIndex];
                
                for (int destIndex = 0; destIndex < destinations.Count && destIndex < row.Elements.Count; destIndex++)
                {
                    var element = row.Elements[destIndex];
                    
                    var result = new GoogleMapsDistanceResult
                    {
                        Origin = origins[originIndex],
                        Destination = destinations[destIndex],
                        Status = element.Status
                    };

                    if (element.Status == "OK")
                    {
                        result.DistanceMeters = element.Distance?.Value ?? 0;
                        result.DistanceText = element.Distance?.Text ?? "";
                        result.DurationSeconds = element.Duration?.Value ?? 0;
                        result.DurationText = element.Duration?.Text ?? "";
                        result.DurationInTrafficSeconds = element.Duration_in_traffic?.Value ?? element.Duration?.Value ?? 0;
                        result.DurationInTrafficText = element.Duration_in_traffic?.Text ?? element.Duration?.Text ?? "";
                    }

                    results.Add(result);
                }
            }

            _logger.LogInformation("Successfully processed {ResultCount} distance matrix results from Google API", results.Count);
        }
        catch (Exception ex)
        {
            stopwatch?.Stop();
            _logger.LogError(ex, "Error calling Google Maps Distance Matrix API");
            
            // Log API error to audit table
            await _auditService.LogErrorAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "GoogleMapsService",
                Description = "Google Maps API Exception",
                Detail = $"Error calling Distance Matrix API: {ex.Message}. Origins: {origins.Count}, Destinations: {destinations.Count}",
                ResponseTime = stopwatch?.Elapsed.TotalSeconds.ToString("0.00") ?? "0",
                IPAddress = "System",
                UserAgent = "EvoAPI GoogleMapsService",
                MachineName = Environment.MachineName,
                IsError = true
            });
        }

        return results;
    }
}
