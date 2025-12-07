namespace EvoAPI.Shared.DTOs
{
    public class UserPantsWaistDto
    {
        public int Id { get; set; }
        public string Size { get; set; } = string.Empty;
        public string Sex { get; set; } = string.Empty;
    }

    public class CreateUserPantsWaistRequest
    {
        public string Size { get; set; } = string.Empty;
        public string Sex { get; set; } = string.Empty;
    }

    public class UpdateUserPantsWaistRequest
    {
        public int Id { get; set; }
        public string Size { get; set; } = string.Empty;
        public string Sex { get; set; } = string.Empty;
    }
}
