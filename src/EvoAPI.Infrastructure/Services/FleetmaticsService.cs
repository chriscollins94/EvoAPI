using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Text.Json;
using System.Data.SqlClient;
using Dapper;
using System.Diagnostics;

namespace EvoAPI.Infrastructure.Services;

public class FleetmaticsService : IFleetmaticsService
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<FleetmaticsService> _logger;
    private readonly IConfiguration _configuration;
    private readonly IAuditService _auditService;
    private readonly string _connectionString;
    
    private string? _currentToken;
    private DateTime _tokenExpiry = DateTime.MinValue;
    private readonly SemaphoreSlim _tokenSemaphore = new(1, 1);
    
    public FleetmaticsService(
        HttpClient httpClient,
        ILogger<FleetmaticsService> logger,
        IConfiguration configuration,
        IAuditService auditService)
    {
        _httpClient = httpClient;
        _logger = logger;
        _configuration = configuration;
        _auditService = auditService;
        
        _connectionString = _configuration.GetConnectionString("DefaultConnection") 
            ?? throw new ArgumentException("Database connection string not configured");
    }

    public async Task<string> GetAccessTokenAsync()
    {
        await _tokenSemaphore.WaitAsync();
        try
        {
            // Check if current token is still valid (with 2-minute buffer)
            if (!string.IsNullOrEmpty(_currentToken) && DateTime.UtcNow < _tokenExpiry.AddMinutes(-2))
            {
                _logger.LogDebug("Using cached Fleetmatics token, expires at {ExpiryTime}", _tokenExpiry);
                return _currentToken;
            }

            // Get new token from Fleetmatics API using Basic Authentication (like Postman)
            var stopwatch = Stopwatch.StartNew();
            
            var baseUrl = _configuration["Fleetmatics:BaseUrl"]
                ?? throw new ArgumentException("Fleetmatics BaseUrl not configured");
            var username = _configuration["Fleetmatics:Username"]
                ?? throw new ArgumentException("Fleetmatics Username not configured");
            var password = _configuration["Fleetmatics:Password"]
                ?? throw new ArgumentException("Fleetmatics Password not configured");

            // Create Basic Authentication header exactly like Postman
            var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
            
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{baseUrl}/token");
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", credentials);

            _logger.LogInformation("Requesting new Fleetmatics access token using Basic Auth");
            _logger.LogInformation("Request URL: {Url}", $"{baseUrl}/token");
            _logger.LogInformation("Using credentials for user: {Username}", username);

            var response = await _httpClient.SendAsync(request);
            stopwatch.Stop();

            _logger.LogInformation("Fleetmatics API response status: {StatusCode}", response.StatusCode);

            if (!response.IsSuccessStatusCode)
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                _logger.LogError("Failed to get Fleetmatics token: {StatusCode} - {ErrorContent}", 
                    response.StatusCode, errorContent);
                    
                await _auditService.LogErrorAsync(new AuditEntry
                {
                    Username = "SYSTEM",
                    Name = "FleetmaticsService",
                    Description = "Fleetmatics Token Request Failed",
                    Detail = $"Status: {response.StatusCode}, Error: {errorContent}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                    IPAddress = "System",
                    UserAgent = "EvoAPI FleetmaticsService",
                    MachineName = Environment.MachineName,
                    IsError = true
                });
                
                throw new Exception($"Failed to get Fleetmatics token: {response.StatusCode}");
            }

            var tokenResponseJson = await response.Content.ReadAsStringAsync();
            
            // Enhanced logging for token response debugging
            _logger.LogError("Fleetmatics Token Response Debug - Status: {StatusCode}, Content: {Content}, StartsWith: {StartChar}", 
                response.StatusCode, tokenResponseJson, tokenResponseJson.Length > 0 ? tokenResponseJson[0].ToString() : "EMPTY");

            await _auditService.LogAsync(new AuditEntry
            {
                Username = "FleetmaticsService",
                Description = "Fleetmatics Token Response Raw",
                Detail = JsonSerializer.Serialize(new { 
                    StatusCode = response.StatusCode,
                    IsSuccess = response.IsSuccessStatusCode,
                    ContentLength = tokenResponseJson.Length,
                    RawContent = tokenResponseJson,
                    FirstChar = tokenResponseJson.Length > 0 ? tokenResponseJson[0].ToString() : "EMPTY"
                }),
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI FleetmaticsService",
                MachineName = Environment.MachineName
            });
            
            // Fleetmatics returns the JWT token directly, not wrapped in JSON
            string accessToken;
            if (tokenResponseJson.StartsWith("{"))
            {
                // Handle JSON response format
                var tokenResponse = JsonSerializer.Deserialize<FleetmaticsTokenResponse>(tokenResponseJson, 
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                
                if (tokenResponse == null || string.IsNullOrEmpty(tokenResponse.AccessToken))
                {
                    _logger.LogError("Invalid token response from Fleetmatics API. Response: {Response}", tokenResponseJson);
                    throw new Exception("Invalid token response from Fleetmatics API");
                }
                accessToken = tokenResponse.AccessToken;
            }
            else
            {
                // Handle direct JWT token response
                accessToken = tokenResponseJson.Trim();
                _logger.LogInformation("Received direct JWT token from Fleetmatics (length: {Length})", accessToken.Length);
            }

            _currentToken = accessToken;
            // Default to 20 minutes for JWT tokens (since we can't parse expiry easily)
            _tokenExpiry = DateTime.UtcNow.AddMinutes(20);

            _logger.LogInformation("Successfully obtained new Fleetmatics token, expires at {ExpiryTime}", _tokenExpiry);
            
            await _auditService.LogAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "FleetmaticsService",
                Description = "Fleetmatics Token Request Success",
                Detail = "Token obtained successfully, expires in 20 minutes (default)",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI FleetmaticsService",
                MachineName = Environment.MachineName
            });

            return _currentToken;
        }
        finally
        {
            _tokenSemaphore.Release();
        }
    }

    public async Task<string?> GetDriverVehicleAssignmentAsync(string driverIdentifier)
    {
        if (string.IsNullOrWhiteSpace(driverIdentifier))
        {
            _logger.LogWarning("Driver identifier is null or empty");
            return null;
        }

        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            var token = await GetAccessTokenAsync();
            var baseUrl = _configuration["Fleetmatics:BaseUrl"];
            var atmosphereAppId = _configuration["Fleetmatics:AtmosphereAppId"];
            
            if (string.IsNullOrEmpty(atmosphereAppId))
            {
                throw new Exception("Fleetmatics AtmosphereAppId is not configured");
            }

            _httpClient.DefaultRequestHeaders.Clear();
            // Fleetmatics requires special Atmosphere authorization format
            _httpClient.DefaultRequestHeaders.Authorization = 
                new System.Net.Http.Headers.AuthenticationHeaderValue("Atmosphere", 
                    $"atmosphere_app_id={atmosphereAppId}, Bearer {token}");

            // Use the correct Fleetmatics API endpoint structure
            var url = $"{baseUrl}/da/v1/driverassignments/drivers/{Uri.EscapeDataString(driverIdentifier)}/currentassignment";
            
            _logger.LogDebug("Requesting driver assignment for employee number {EmployeeNumber}", driverIdentifier);

            var response = await _httpClient.GetAsync(url);
            stopwatch.Stop();

            if (!response.IsSuccessStatusCode)
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                _logger.LogWarning("Failed to get driver assignment for employee number {EmployeeNumber}: {StatusCode} - {ErrorContent}", 
                    driverIdentifier, response.StatusCode, errorContent);
                    
                await _auditService.LogErrorAsync(new AuditEntry
                {
                    Username = "SYSTEM",
                    Name = "FleetmaticsService",
                    Description = "Driver Assignment Request Failed",
                    Detail = $"EmployeeNumber: {driverIdentifier}, Status: {response.StatusCode}, Error: {errorContent}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                    IPAddress = "System",
                    UserAgent = "EvoAPI FleetmaticsService",
                    MachineName = Environment.MachineName,
                    IsError = true
                });
                
                return null;
            }

            var assignmentJson = await response.Content.ReadAsStringAsync();
            _logger.LogError("FLEETMATICS DEBUG - Employee {EmployeeNumber} Response: {ResponseContent}", 
                driverIdentifier, assignmentJson);
                
            // Also log to audit for visibility
            await _auditService.LogAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "FleetmaticsService",
                Description = $"Fleetmatics Raw Response - Employee {driverIdentifier}",
                Detail = assignmentJson,
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI FleetmaticsService",
                MachineName = Environment.MachineName
            });
                
            // Check if response looks like an error message instead of JSON
            if (string.IsNullOrWhiteSpace(assignmentJson) || 
                (!assignmentJson.TrimStart().StartsWith("{") && !assignmentJson.TrimStart().StartsWith("[")))
            {
                _logger.LogWarning("Fleetmatics API returned non-JSON response for employee {EmployeeNumber}: {Response}", 
                    driverIdentifier, assignmentJson);
                return null;
            }
                
            FleetmaticsDriverAssignment? assignment = null;
            try
            {
                assignment = JsonSerializer.Deserialize<FleetmaticsDriverAssignment>(assignmentJson,
                    new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            }
            catch (JsonException jsonEx)
            {
                _logger.LogError(jsonEx, "Failed to parse Fleetmatics API response as JSON for employee {EmployeeNumber}. Response: {ResponseContent}", 
                    driverIdentifier, assignmentJson);
                return null;
            }

            var vehicleNumber = assignment?.VehicleNumber;
            
            if (!string.IsNullOrEmpty(vehicleNumber))
            {
                _logger.LogDebug("Found vehicle assignment for employee number {EmployeeNumber}: {VehicleNumber}", 
                    driverIdentifier, vehicleNumber);
                    
                await _auditService.LogAsync(new AuditEntry
                {
                    Username = "SYSTEM",
                    Name = "FleetmaticsService",
                    Description = "Driver Assignment Retrieved",
                    Detail = $"EmployeeNumber: {driverIdentifier}, VehicleNumber: {vehicleNumber}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                    IPAddress = "System",
                    UserAgent = "EvoAPI FleetmaticsService",
                    MachineName = Environment.MachineName
                });
            }
            else
            {
                _logger.LogInformation("No vehicle assignment found for employee number {EmployeeNumber}", driverIdentifier);
            }

            return vehicleNumber;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error getting driver assignment for employee number {EmployeeNumber}", driverIdentifier);
            
            await _auditService.LogErrorAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "FleetmaticsService",
                Description = "Driver Assignment Exception",
                Detail = $"EmployeeNumber: {driverIdentifier}, Error: {ex.Message}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI FleetmaticsService",
                MachineName = Environment.MachineName,
                IsError = true
            });
            
            return null;
        }
    }

    public async Task<FleetmaticsSyncResultDto> SyncAllVehicleAssignmentsAsync()
    {
        var stopwatch = Stopwatch.StartNew();
        var result = new FleetmaticsSyncResultDto
        {
            SyncDateTime = DateTime.UtcNow
        };

        try
        {
            _logger.LogInformation("Starting Fleetmatics vehicle assignment sync");

            var users = await GetUsersForSyncAsync();
            result.TotalUsersProcessed = users.Count;

            foreach (var user in users)
            {
                try
                {
                    // Skip users without employee numbers
                    if (string.IsNullOrWhiteSpace(user.EmployeeNumber))
                    {
                        _logger.LogDebug("Skipping user {Username} (ID: {UserId}) - no employee number", 
                            user.Username, user.UserId);
                        continue;
                    }

                    var vehicleNumber = await GetDriverVehicleAssignmentAsync(user.EmployeeNumber);
                    
                    if (!string.IsNullOrEmpty(vehicleNumber))
                    {
                        var updateSuccess = await UpdateUserVehicleNumberAsync(user.UserId, vehicleNumber);
                        if (updateSuccess)
                        {
                            result.SuccessfulUpdates++;
                            _logger.LogDebug("Updated vehicle number for user {Username} (ID: {UserId}, Employee: {EmployeeNumber}): {VehicleNumber}", 
                                user.Username, user.UserId, user.EmployeeNumber, vehicleNumber);
                        }
                        else
                        {
                            result.Errors++;
                            var errorMsg = $"Failed to update database for user {user.Username} (Employee: {user.EmployeeNumber})";
                            result.ErrorMessages.Add(errorMsg);
                            _logger.LogWarning(errorMsg);
                        }
                    }
                    else
                    {
                        _logger.LogDebug("No vehicle assignment found for user {Username} (Employee: {EmployeeNumber})", 
                            user.Username, user.EmployeeNumber);
                    }
                }
                catch (Exception ex)
                {
                    result.Errors++;
                    var errorMsg = $"Error processing user {user.Username} (Employee: {user.EmployeeNumber}): {ex.Message}";
                    result.ErrorMessages.Add(errorMsg);
                    _logger.LogWarning(ex, "Failed to sync vehicle assignment for user {Username} (Employee: {EmployeeNumber})", 
                        user.Username, user.EmployeeNumber);
                }
            }

            stopwatch.Stop();
            result.Duration = stopwatch.Elapsed;

            _logger.LogInformation(
                "Fleetmatics sync completed. Processed: {TotalUsers}, Updated: {SuccessfulUpdates}, Errors: {Errors}, Duration: {Duration}ms",
                result.TotalUsersProcessed, result.SuccessfulUpdates, result.Errors, result.Duration.TotalMilliseconds
            );
            
            await _auditService.LogAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "FleetmaticsService",
                Description = "Vehicle Assignment Sync Completed",
                Detail = JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = false }),
                ResponseTime = result.Duration.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI FleetmaticsService",
                MachineName = Environment.MachineName
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            result.Duration = stopwatch.Elapsed;
            result.Errors++;
            result.ErrorMessages.Add($"Sync failed: {ex.Message}");
            
            _logger.LogError(ex, "Failed to complete Fleetmatics vehicle assignment sync");
            
            await _auditService.LogErrorAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "FleetmaticsService",
                Description = "Vehicle Assignment Sync Failed",
                Detail = $"Error: {ex.Message}, Duration: {result.Duration.TotalSeconds:0.00}s",
                ResponseTime = result.Duration.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI FleetmaticsService",
                MachineName = Environment.MachineName,
                IsError = true
            });
        }

        return result;
    }

    public async Task<List<UserFleetmaticsDto>> GetUsersForSyncAsync()
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            
            var sql = @"
                SELECT u_id as UserId, 
                       u_username as Username, 
                       u_firstname as FirstName, 
                       u_lastname as LastName, 
                       u_employeenumber as EmployeeNumber,
                       u_vehiclenumber as CurrentVehicleNumber,
                       u_active as IsActive
                FROM [user] 
                WHERE u_active = 1 
                AND u_employeenumber IS NOT NULL 
                AND u_employeenumber != ''
                AND LEN(TRIM(u_employeenumber)) > 0
                ORDER BY u_employeenumber";

            var users = await connection.QueryAsync<UserFleetmaticsDto>(sql);
            
            _logger.LogDebug("Found {UserCount} users eligible for Fleetmatics sync (with employee numbers)", users.Count());
            
            return users.ToList();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error retrieving users for Fleetmatics sync");
            throw;
        }
    }

    public async Task<bool> UpdateUserVehicleNumberAsync(int userId, string vehicleNumber)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            
            var sql = @"
                UPDATE [user] 
                SET u_vehiclenumber = @VehicleNumber
                WHERE u_id = @UserId";

            var rowsAffected = await connection.ExecuteAsync(sql, new 
            { 
                UserId = userId, 
                VehicleNumber = vehicleNumber 
            });

            if (rowsAffected > 0)
            {
                _logger.LogDebug("Updated vehicle number for user {UserId}: {VehicleNumber}", userId, vehicleNumber);
                return true;
            }
            else
            {
                _logger.LogWarning("No rows affected when updating vehicle number for user {UserId}", userId);
                return false;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating vehicle number for user {UserId}", userId);
            throw;
        }
    }

    public async Task<List<VehicleLocationDto>> GetVehicleLocationsAsync(List<string> vehicleNumbers)
    {
        var stopwatch = Stopwatch.StartNew();
        var result = new List<VehicleLocationDto>();
        
        try
        {
            var token = await GetAccessTokenAsync();
            var baseUrl = _configuration["Fleetmatics:BaseUrl"];
            var atmosphereAppId = _configuration["Fleetmatics:AtmosphereAppId"];

            // Fleetmatics requires special Atmosphere authorization format
            using var request = new HttpRequestMessage(HttpMethod.Post, $"{baseUrl}/rad/v1/vehicles/locations");
            request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Atmosphere", 
                $"atmosphere_app_id={atmosphereAppId}, Bearer {token}");

            // Set content type and body
            var jsonContent = JsonSerializer.Serialize(vehicleNumbers);
            request.Content = new StringContent(jsonContent, Encoding.UTF8, "application/json");

            _logger.LogInformation("Requesting vehicle locations for {VehicleCount} vehicles from Fleetmatics", vehicleNumbers.Count);
            _logger.LogInformation("DEBUG: Exact JSON being sent to Fleetmatics: {JsonContent}", jsonContent);
            
            // Also write to console for immediate visibility
            Console.WriteLine($"=== FLEETMATICS DEBUG ===");
            Console.WriteLine($"URL: {baseUrl}/rad/v1/vehicles/locations");
            Console.WriteLine($"Vehicle count: {vehicleNumbers.Count}");
            Console.WriteLine($"JSON being sent: {jsonContent}");
            Console.WriteLine($"Content-Type: {request.Content.Headers.ContentType}");
            Console.WriteLine($"Authorization: {request.Headers.Authorization}");
            Console.WriteLine($"========================");

            var response = await _httpClient.SendAsync(request);
            stopwatch.Stop();

            if (!response.IsSuccessStatusCode)
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                _logger.LogError("Failed to get vehicle locations: {StatusCode} - {ErrorContent}", 
                    response.StatusCode, errorContent);
                
                await _auditService.LogErrorAsync(new AuditEntry
                {
                    Username = "SYSTEM",
                    Name = "FleetmaticsService",
                    Description = "Vehicle Locations Request Failed",
                    Detail = $"Status: {response.StatusCode}, Error: {errorContent}, VehicleCount: {vehicleNumbers.Count}",
                    ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                    IPAddress = "System",
                    UserAgent = "EvoAPI FleetmaticsService",
                    MachineName = Environment.MachineName,
                    IsError = true
                });
                
                throw new Exception($"Failed to get vehicle locations: {response.StatusCode}");
            }

            var responseContent = await response.Content.ReadAsStringAsync();
            _logger.LogDebug("Fleetmatics vehicle locations raw response: {ResponseContent}", responseContent);
            _logger.LogInformation("DEBUG: Full Fleetmatics response: {ResponseContent}", responseContent);

            // Parse the response using the correct array structure (not wrapped in Data property)
            var options = new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            };

            var fleetmaticsResponse = JsonSerializer.Deserialize<List<FleetmaticsLocationData>>(responseContent, options);

            if (fleetmaticsResponse != null)
            {
                foreach (var dataItem in fleetmaticsResponse)
                {
                    if (dataItem.ContentResource?.Value == null) continue;

                    var locationValue = dataItem.ContentResource.Value;
                    
                    // Parse the UpdatedUTC timestamp
                    var lastUpdateTime = DateTime.UtcNow; // Default fallback
                    if (!string.IsNullOrEmpty(locationValue.UpdatedUTC))
                    {
                        if (DateTime.TryParse(locationValue.UpdatedUTC, out var parsedTime))
                        {
                            lastUpdateTime = parsedTime;
                        }
                    }

                    // Get address string - prioritize AddressLine1 as it contains the full formatted address
                    var address = string.Empty;
                    if (locationValue.Address != null)
                    {
                        if (!string.IsNullOrEmpty(locationValue.Address.AddressLine1))
                        {
                            // Use the full formatted address from AddressLine1
                            address = locationValue.Address.AddressLine1;
                        }
                        else
                        {
                            // Fallback: build address from components
                            var addressParts = new List<string>();
                            if (!string.IsNullOrEmpty(locationValue.Address.Locality))
                                addressParts.Add(locationValue.Address.Locality);
                            if (!string.IsNullOrEmpty(locationValue.Address.AdministrativeArea))
                                addressParts.Add(locationValue.Address.AdministrativeArea);
                            if (!string.IsNullOrEmpty(locationValue.Address.PostalCode))
                                addressParts.Add(locationValue.Address.PostalCode);
                            address = string.Join(", ", addressParts);
                        }
                    }

                    // Convert string heading to numeric value
                    double? headingDegrees = ConvertHeadingTodegrees(locationValue.Heading);

                    var dto = new VehicleLocationDto
                    {
                        VehicleNumber = dataItem.VehicleNumber,
                        Latitude = locationValue.Latitude,
                        Longitude = locationValue.Longitude,
                        Address = !string.IsNullOrEmpty(address) ? address : "Address unavailable",
                        LastUpdateTime = lastUpdateTime,
                        Speed = locationValue.Speed,
                        Heading = headingDegrees,
                        IsActive = true
                    };

                    result.Add(dto);
                }
            }

            _logger.LogInformation("Successfully retrieved locations for {LocationCount} of {RequestedCount} vehicles", 
                result.Count, vehicleNumbers.Count);

            await _auditService.LogAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "FleetmaticsService",
                Description = "Vehicle Locations Retrieved",
                Detail = $"Requested: {vehicleNumbers.Count}, Retrieved: {result.Count}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI FleetmaticsService",
                MachineName = Environment.MachineName
            });

            return result;
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "Error retrieving vehicle locations from Fleetmatics");
            
            await _auditService.LogErrorAsync(new AuditEntry
            {
                Username = "SYSTEM",
                Name = "FleetmaticsService",
                Description = "Vehicle Locations Retrieval Failed",
                Detail = $"Error: {ex.Message}, VehicleCount: {vehicleNumbers.Count}",
                ResponseTime = stopwatch.Elapsed.TotalSeconds.ToString("0.00"),
                IPAddress = "System",
                UserAgent = "EvoAPI FleetmaticsService",
                MachineName = Environment.MachineName,
                IsError = true
            });
            
            throw;
        }
    }

    /// <summary>
    /// Converts a string heading direction to degrees
    /// </summary>
    /// <param name="heading">Heading direction (e.g., "North", "South", "East", "West", or numeric string)</param>
    /// <returns>Heading in degrees (0-360) or null if conversion fails</returns>
    private static double? ConvertHeadingTodegrees(string heading)
    {
        if (string.IsNullOrEmpty(heading))
            return null;

        // Try to parse as a numeric value first
        if (double.TryParse(heading, out var numericHeading))
        {
            // Normalize to 0-360 range
            while (numericHeading < 0) numericHeading += 360;
            while (numericHeading >= 360) numericHeading -= 360;
            return numericHeading;
        }

        // Convert direction names to degrees
        return heading.ToUpperInvariant() switch
        {
            "N" or "NORTH" => 0,
            "NNE" or "NORTH-NORTHEAST" => 22.5,
            "NE" or "NORTHEAST" => 45,
            "ENE" or "EAST-NORTHEAST" => 67.5,
            "E" or "EAST" => 90,
            "ESE" or "EAST-SOUTHEAST" => 112.5,
            "SE" or "SOUTHEAST" => 135,
            "SSE" or "SOUTH-SOUTHEAST" => 157.5,
            "S" or "SOUTH" => 180,
            "SSW" or "SOUTH-SOUTHWEST" => 202.5,
            "SW" or "SOUTHWEST" => 225,
            "WSW" or "WEST-SOUTHWEST" => 247.5,
            "W" or "WEST" => 270,
            "WNW" or "WEST-NORTHWEST" => 292.5,
            "NW" or "NORTHWEST" => 315,
            "NNW" or "NORTH-NORTHWEST" => 337.5,
            _ => null // Unknown direction
        };
    }
}
