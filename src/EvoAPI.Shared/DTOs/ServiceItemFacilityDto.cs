namespace EvoAPI.Shared.DTOs
{
    public class ServiceItemFacilityDto
    {
        public int Id { get; set; }
        public string Facility { get; set; } = string.Empty;
    }

    public class CreateServiceItemFacilityRequest
    {
        public string Facility { get; set; } = string.Empty;
    }

    public class UpdateServiceItemFacilityRequest
    {
        public int Id { get; set; }
        public string Facility { get; set; } = string.Empty;
    }
}
