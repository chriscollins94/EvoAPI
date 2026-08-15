namespace EvoAPI.Shared.DTOs
{
    public class UserRelationshipDto
    {
        public int Id { get; set; }
        public string Relationship { get; set; } = string.Empty;
    }

    public class CreateUserRelationshipRequest
    {
        public string Relationship { get; set; } = string.Empty;
    }

    public class UpdateUserRelationshipRequest
    {
        public int Id { get; set; }
        public string Relationship { get; set; } = string.Empty;
    }
}
