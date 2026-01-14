namespace EvoAPI.Shared.DTOs
{
    public class RegionDto
    {
        public int RegionId { get; set; }
        public string RegionName { get; set; } = string.Empty;
        public string? RegionDescription { get; set; }
        public string? RegionAcronym { get; set; }
        public int? RfmUserId { get; set; }
        public string? RfmName { get; set; }
        public string? RfmPicture { get; set; }
        public int ZoneCount { get; set; }
    }

    public class ZoneEmployeeDto
    {
        public int UserId { get; set; }
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
        public string? Picture { get; set; }
        public bool Active { get; set; }
        public string? EmployeeNumber { get; set; }
    }
}
