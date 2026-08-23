namespace EvoAPI.Shared.DTOs;

public class CompanyChatParticipantsDto
{
    public string ConversationId { get; set; } = string.Empty;

    /// <summary>
    /// False when the conversation does not exist yet in the TalkJS app
    /// (e.g. the test app before anyone has opened the chat page against it).
    /// </summary>
    public bool ConversationExists { get; set; }

    public List<CompanyChatParticipantDto> Participants { get; set; } = new();
}

public class CompanyChatParticipantDto
{
    /// <summary>TalkJS user id — matches [User].u_id.</summary>
    public string Id { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public string Email { get; set; } = string.Empty;

    /// <summary>Null when the TalkJS id has no matching row in the User table.</summary>
    public bool? Active { get; set; }
}
