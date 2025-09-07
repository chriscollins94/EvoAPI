namespace EvoAPI.Shared.DTOs;

public class TechDetailReportDto
{
    public int UserId { get; set; }
    public string FirstName { get; set; } = string.Empty;
    public string LastName { get; set; } = string.Empty;
    public string FullName => $"{FirstName} {LastName}".Trim();
    public string TechnicianName => FullName; // Alias for frontend compatibility
    public int PerformanceId { get; set; }
    public DateTime? PerformanceDate { get; set; }
    public string Utilization { get; set; } = string.Empty;
    public string Profitability { get; set; } = string.Empty;
    public string Attendance { get; set; } = string.Empty;
    public string Comment { get; set; } = string.Empty;
    public string Address1 { get; set; } = string.Empty;
    public string Address2 { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string State { get; set; } = string.Empty;
    public string Zip { get; set; } = string.Empty;
    public string FullAddress => BuildFullAddress();
    public int? ZoneId { get; set; }
    public string ZoneNumber { get; set; } = string.Empty;
    public string ZoneDisplay => !string.IsNullOrWhiteSpace(ZoneNumber) ? 
        $"Zone {ZoneNumber}" : 
        "No Zone";
    public string Zone => ZoneDisplay; // Alias for frontend compatibility
    
    // Calculated percentage properties for frontend display
    public double? UtilizationPercentage => ParsePercentage(Utilization);
    public double? ProfitabilityPercentage => ParsePercentage(Profitability);
    public double? AttendancePercentage => ParsePercentage(Attendance);
    
    private double? ParsePercentage(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;
            
        // Remove % symbol if present and try to parse
        var cleanValue = value.Replace("%", "").Trim();
        
        if (double.TryParse(cleanValue, out var result))
            return result;
            
        return null;
    }
    
    private string BuildFullAddress()
    {
        var addressParts = new List<string>();
        
        if (!string.IsNullOrWhiteSpace(Address1))
            addressParts.Add(Address1);
            
        if (!string.IsNullOrWhiteSpace(Address2))
            addressParts.Add(Address2);
            
        var streetAddress = string.Join(", ", addressParts);
        var cityStateZip = new List<string>();
        
        if (!string.IsNullOrWhiteSpace(City))
            cityStateZip.Add(City);
            
        if (!string.IsNullOrWhiteSpace(State))
            cityStateZip.Add(State);
            
        if (!string.IsNullOrWhiteSpace(Zip))
            cityStateZip.Add(Zip);
            
        var locationPart = string.Join(", ", cityStateZip);
        
        if (!string.IsNullOrWhiteSpace(streetAddress) && !string.IsNullOrWhiteSpace(locationPart))
            return $"{streetAddress}, {locationPart}";
        else if (!string.IsNullOrWhiteSpace(streetAddress))
            return streetAddress;
        else if (!string.IsNullOrWhiteSpace(locationPart))
            return locationPart;
        else
            return string.Empty;
    }
}
