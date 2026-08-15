namespace EvoAPI.Shared.DTOs
{
    public class UserClothingSizeDto
    {
        public int Id { get; set; }
        public string ClothingSize { get; set; } = string.Empty;
    }

    public class CreateUserClothingSizeRequest
    {
        public string ClothingSize { get; set; } = string.Empty;
    }

    public class UpdateUserClothingSizeRequest
    {
        public int Id { get; set; }
        public string ClothingSize { get; set; } = string.Empty;
    }
}
