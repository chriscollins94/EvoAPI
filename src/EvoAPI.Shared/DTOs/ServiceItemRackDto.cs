namespace EvoAPI.Shared.DTOs
{
    public class ServiceItemRackDto
    {
        public int Id { get; set; }
        public string Rack { get; set; } = string.Empty;
    }

    public class CreateServiceItemRackRequest
    {
        public string Rack { get; set; } = string.Empty;
    }

    public class UpdateServiceItemRackRequest
    {
        public int Id { get; set; }
        public string Rack { get; set; } = string.Empty;
    }
}
