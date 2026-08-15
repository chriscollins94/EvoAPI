namespace EvoAPI.Shared.DTOs
{
    public class UserEmergencyContactDto
    {
        public int XuecId { get; set; }
        public int UserId { get; set; }
        public int? RelationshipId { get; set; }
        public string? RelationshipName { get; set; }
        public string? Name { get; set; }
        public string? Phone { get; set; }
        public DateTime InsertDateTime { get; set; }
        public DateTime? ModifiedDateTime { get; set; }
    }

    public class CreateUserEmergencyContactRequest
    {
        public int? RelationshipId { get; set; }
        public string? Name { get; set; }
        public string? Phone { get; set; }
    }

    public class UpdateUserEmergencyContactRequest
    {
        public int XuecId { get; set; }
        public int? RelationshipId { get; set; }
        public string? Name { get; set; }
        public string? Phone { get; set; }
    }
}
