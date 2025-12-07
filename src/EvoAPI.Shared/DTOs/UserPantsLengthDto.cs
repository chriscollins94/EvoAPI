namespace EvoAPI.Shared.DTOs
{
    public class UserPantsLengthDto
    {
        public int Id { get; set; }
        public string Size { get; set; } = string.Empty;
        public string Sex { get; set; } = string.Empty;
    }

    public class CreateUserPantsLengthRequest
    {
        public string Size { get; set; } = string.Empty;
        public string Sex { get; set; } = string.Empty;
    }

    public class UpdateUserPantsLengthRequest
    {
        public int Id { get; set; }
        public string Size { get; set; } = string.Empty;
        public string Sex { get; set; } = string.Empty;
    }
}
