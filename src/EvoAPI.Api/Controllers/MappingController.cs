using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.Attributes;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Models;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;
using System.Text.Json;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("EvoApi/[controller]")]
[EvoAuthorize]
public class MappingController : BaseController
{
    private readonly IDataService _dataService;
    private readonly ILogger<MappingController> _logger;
    private readonly IConfiguration _configuration;
    private readonly HttpClient _httpClient;

    public MappingController(IDataService dataService, IAuditService auditService, ILogger<MappingController> logger, IConfiguration configuration, HttpClient httpClient)
    {
        _dataService = dataService;
        _logger = logger;
        _configuration = configuration;
        _httpClient = httpClient;
        InitializeAuditService(auditService);
    }

    /// <summary>
    /// Get cached distance data between two addresses
    /// </summary>
    /// <param name="fromAddress">Origin address</param>
    /// <param name="toAddress">Destination address</param>
    /// <returns>Cached distance data if available</returns>
    [HttpGet("cached-distance")]
    public async Task<ActionResult<ApiResponse<MapDistanceDto>>> GetCachedDistance(
        [FromQuery] string fromAddress, 
        [FromQuery] string toAddress)
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            if (string.IsNullOrWhiteSpace(fromAddress) || string.IsNullOrWhiteSpace(toAddress))
            {
                return BadRequest(new ApiResponse<MapDistanceDto>
                {
                    Success = false,
                    Message = "Both fromAddress and toAddress are required"
                });
            }

            var cachedDistance = await _dataService.GetCachedDistanceAsync(fromAddress.Trim(), toAddress.Trim());
            
            stopwatch.Stop();
            await LogAuditAsync("GetCachedDistance", new { fromAddress, toAddress, found = cachedDistance != null }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            if (cachedDistance == null)
            {
                return Ok(new ApiResponse<MapDistanceDto>
                {
                    Success = true,
                    Message = "No cached distance data found",
                    Data = null
                });
            }

            return Ok(new ApiResponse<MapDistanceDto>
            {
                Success = true,
                Message = "Cached distance retrieved successfully",
                Data = cachedDistance
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("GetCachedDistance", ex, new { fromAddress, toAddress });
            
            _logger.LogError(ex, "Error retrieving cached distance between {FromAddress} and {ToAddress}", fromAddress, toAddress);
            
            return StatusCode(500, new ApiResponse<MapDistanceDto>
            {
                Success = false,
                Message = "Failed to retrieve cached distance data"
            });
        }
    }

    /// <summary>
    /// Save distance data to cache
    /// </summary>
    /// <param name="request">Distance data to cache</param>
    /// <returns>Saved distance data</returns>
    [HttpPost("cached-distance")]
    public async Task<ActionResult<ApiResponse<int>>> SaveCachedDistance([FromBody] SaveMapDistanceRequest request)
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            if (request == null)
            {
                return BadRequest(new ApiResponse<int>
                {
                    Success = false,
                    Message = "Request data is required"
                });
            }

            if (string.IsNullOrWhiteSpace(request.FromAddress) || string.IsNullOrWhiteSpace(request.ToAddress))
            {
                return BadRequest(new ApiResponse<int>
                {
                    Success = false,
                    Message = "Both FromAddress and ToAddress are required"
                });
            }

            var savedId = await _dataService.SaveCachedDistanceAsync(request);
            
            stopwatch.Stop();
            await LogAuditAsync("SaveCachedDistance", request, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<int>
            {
                Success = true,
                Message = "Distance data cached successfully",
                Data = savedId
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("SaveCachedDistance", ex, request);
            
            _logger.LogError(ex, "Error saving cached distance data");
            
            return StatusCode(500, new ApiResponse<int>
            {
                Success = false,
                Message = "Failed to save distance data to cache"
            });
        }
    }

    /// <summary>
    /// Clear old cached distance data (for maintenance)
    /// </summary>
    /// <param name="olderThanDays">Remove cache entries older than this many days</param>
    /// <returns>Number of entries removed</returns>
    [HttpDelete("cached-distance/cleanup")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<int>>> CleanupCachedDistance([FromQuery] int olderThanDays = 90)
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            if (olderThanDays < 1)
            {
                return BadRequest(new ApiResponse<int>
                {
                    Success = false,
                    Message = "olderThanDays must be at least 1"
                });
            }

            var deletedCount = await _dataService.CleanupCachedDistanceAsync(olderThanDays);
            
            stopwatch.Stop();
            await LogAuditAsync("CleanupCachedDistance", new { olderThanDays, deletedCount }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<int>
            {
                Success = true,
                Message = $"Removed {deletedCount} cached distance entries older than {olderThanDays} days",
                Data = deletedCount
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("CleanupCachedDistance", ex, new { olderThanDays });
            
            _logger.LogError(ex, "Error cleaning up cached distance data");
            
            return StatusCode(500, new ApiResponse<int>
            {
                Success = false,
                Message = "Failed to cleanup cached distance data"
            });
        }
    }

    /// <summary>
    /// Calculate distance and travel time using Google Maps API
    /// </summary>
    /// <param name="fromAddress">Origin address</param>
    /// <param name="toAddress">Destination address</param>
    /// <param name="includeTraffic">Whether to include traffic data</param>
    /// <returns>Distance and travel time data</returns>
    [HttpGet("calculate-distance")]
    public async Task<ActionResult<ApiResponse<GoogleMapsDistanceDto>>> CalculateDistance(
        [FromQuery] string fromAddress,
        [FromQuery] string toAddress,
        [FromQuery] bool includeTraffic = false)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            if (string.IsNullOrWhiteSpace(fromAddress) || string.IsNullOrWhiteSpace(toAddress))
            {
                return BadRequest(new ApiResponse<GoogleMapsDistanceDto>
                {
                    Success = false,
                    Message = "Both fromAddress and toAddress are required"
                });
            }

            var apiKey = _configuration["GoogleMaps:ApiKey"];
            if (string.IsNullOrWhiteSpace(apiKey))
            {
                await LogAuditErrorAsync("CalculateDistance", 
                    new Exception("Google Maps API key not configured or empty"));
                
                return StatusCode(500, new ApiResponse<GoogleMapsDistanceDto>
                {
                    Success = false,
                    Message = "Google Maps API key not configured"
                });
            }

            var encodedFrom = Uri.EscapeDataString(fromAddress);
            var encodedTo = Uri.EscapeDataString(toAddress);

            var apiUrl = $"https://maps.googleapis.com/maps/api/distancematrix/json?origins={encodedFrom}&destinations={encodedTo}&units=imperial&key={apiKey}";

            if (includeTraffic)
            {
                apiUrl += "&departure_time=now&traffic_model=best_guess";
            }

            var response = await _httpClient.GetAsync(apiUrl);
            var jsonContent = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
            {
                await LogAuditErrorAsync("CalculateDistance", 
                    new Exception($"Google Maps API HTTP error: {response.StatusCode}"));

                return StatusCode(500, new ApiResponse<GoogleMapsDistanceDto>
                {
                    Success = false,
                    Message = "Failed to call Google Maps API"
                });
            }

            var googleResponse = JsonSerializer.Deserialize<EvoAPI.Shared.DTOs.GoogleMapsApiResponse>(jsonContent, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
            });

            if (googleResponse?.Status != "OK")
            {
                var errorMessage = $"Google Maps API error: {googleResponse?.Status}";
                if (!string.IsNullOrEmpty(googleResponse?.ErrorMessage))
                {
                    errorMessage += $" - {googleResponse.ErrorMessage}";
                }
                
                await LogAuditErrorAsync("CalculateDistance", 
                    new Exception($"{errorMessage}. API Key starts with: {apiKey.Substring(0, Math.Min(8, apiKey.Length))}..."));

                return StatusCode(500, new ApiResponse<GoogleMapsDistanceDto>
                {
                    Success = false,
                    Message = errorMessage
                });
            }

            var element = googleResponse.Rows?.FirstOrDefault()?.Elements?.FirstOrDefault();
            if (element?.Status != "OK")
            {
                return StatusCode(500, new ApiResponse<GoogleMapsDistanceDto>
                {
                    Success = false,
                    Message = $"Unable to calculate distance: {element?.Status}"
                });
            }

            var result = new GoogleMapsDistanceDto
            {
                Success = true,
                Source = "google_maps",
                Distance = new DistanceDto
                {
                    Meters = element.Distance?.Value ?? 0,
                    Miles = Math.Round((element.Distance?.Value ?? 0) * 0.000621371, 2),
                    Text = element.Distance?.Text ?? ""
                },
                Duration = new DurationDto
                {
                    Seconds = element.Duration?.Value ?? 0,
                    Minutes = (int)Math.Ceiling((element.Duration?.Value ?? 0) / 60.0),
                    Text = element.Duration?.Text ?? ""
                },
                DurationInTraffic = element.DurationInTraffic != null ? new DurationDto
                {
                    Seconds = element.DurationInTraffic.Value,
                    Minutes = (int)Math.Ceiling(element.DurationInTraffic.Value / 60.0),
                    Text = element.DurationInTraffic.Text
                } : null,
                FromAddress = fromAddress,
                ToAddress = toAddress,
                Timestamp = DateTime.UtcNow
            };

            stopwatch.Stop();
            await LogAuditAsync("CalculateDistance", 
                $"Calculated distance from '{fromAddress}' to '{toAddress}'", 
                stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<GoogleMapsDistanceDto>
            {
                Success = true,
                Message = "Distance calculated successfully",
                Data = result
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("CalculateDistance", ex);

            return StatusCode(500, new ApiResponse<GoogleMapsDistanceDto>
            {
                Success = false,
                Message = "Failed to calculate distance"
            });
        }
    }
}
