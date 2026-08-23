using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using Microsoft.Extensions.Logging;

namespace EvoAPI.Infrastructure.Services;

/// <summary>
/// Talks to the TalkJS REST API to manage the single company-wide chat conversation.
/// Every employee is auto-joined to this conversation by the chat page, and TalkJS
/// caps participants per conversation by plan, so inactive employees must be removed
/// to free spots for new hires.
///
/// Credentials live in ConfigSetting (cs_type 'talkjs'): 'AppId' and 'SecretKey'.
/// The test and production databases point at their respective TalkJS apps.
/// Keep the conversation id in sync with OfficialTalkJSChat.js in evotech.
/// </summary>
public class ChatService : IChatService
{
    private const string ConversationId = "company_chat_25";

    private readonly HttpClient _httpClient;
    private readonly IDataService _dataService;
    private readonly ILogger<ChatService> _logger;

    public ChatService(
        HttpClient httpClient,
        IDataService dataService,
        ILogger<ChatService> logger)
    {
        _httpClient = httpClient;
        _dataService = dataService;
        _logger = logger;
    }

    public async Task<CompanyChatParticipantsDto> GetCompanyChatParticipantsAsync()
    {
        var (appId, secretKey) = await GetTalkJsCredentialsAsync();

        var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"https://api.talkjs.com/v1/{appId}/conversations/{ConversationId}");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", secretKey);

        var response = await _httpClient.SendAsync(request);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            // Conversation not created yet in this TalkJS app (normal for the test app)
            return new CompanyChatParticipantsDto
            {
                ConversationId = ConversationId,
                ConversationExists = false
            };
        }

        response.EnsureSuccessStatusCode();

        var body = await response.Content.ReadAsStringAsync();
        var result = new CompanyChatParticipantsDto
        {
            ConversationId = ConversationId,
            ConversationExists = true
        };

        var participantIds = new List<string>();
        using (var doc = JsonDocument.Parse(body))
        {
            if (doc.RootElement.TryGetProperty("participants", out var participants))
            {
                foreach (var participant in participants.EnumerateObject())
                {
                    participantIds.Add(participant.Name);
                }
            }
        }

        // Join TalkJS ids (which are [User].u_id values) against the user table
        // so the UI can show names and flag inactive employees.
        var userTable = await _dataService.GetUsersChatInfoAsync();
        var usersById = new Dictionary<string, (string name, string email, bool active)>();
        foreach (System.Data.DataRow row in userTable.Rows)
        {
            var id = row["u_id"]?.ToString();
            if (string.IsNullOrEmpty(id))
            {
                continue;
            }
            var name = $"{row["u_firstname"]} {row["u_lastname"]}".Trim();
            usersById[id] = (
                name,
                row["u_email"]?.ToString() ?? string.Empty,
                row["u_active"] != DBNull.Value && Convert.ToBoolean(row["u_active"])
            );
        }

        foreach (var id in participantIds)
        {
            if (usersById.TryGetValue(id, out var userInfo))
            {
                result.Participants.Add(new CompanyChatParticipantDto
                {
                    Id = id,
                    Name = string.IsNullOrWhiteSpace(userInfo.name) ? $"User {id}" : userInfo.name,
                    Email = userInfo.email,
                    Active = userInfo.active
                });
            }
            else
            {
                result.Participants.Add(new CompanyChatParticipantDto
                {
                    Id = id,
                    Name = $"Unknown user ({id})",
                    Active = null
                });
            }
        }

        return result;
    }

    public async Task RemoveCompanyChatParticipantAsync(string userId)
    {
        var (appId, secretKey) = await GetTalkJsCredentialsAsync();

        var request = new HttpRequestMessage(
            HttpMethod.Delete,
            $"https://api.talkjs.com/v1/{appId}/conversations/{ConversationId}/participants/{Uri.EscapeDataString(userId)}");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", secretKey);

        var response = await _httpClient.SendAsync(request);
        if (!response.IsSuccessStatusCode)
        {
            var body = await response.Content.ReadAsStringAsync();
            _logger.LogError("TalkJS participant removal failed for user {UserId}: {Status} {Body}",
                userId, (int)response.StatusCode, body);
            throw new InvalidOperationException(
                $"TalkJS returned {(int)response.StatusCode} removing participant {userId}");
        }
    }

    private async Task<(string appId, string secretKey)> GetTalkJsCredentialsAsync()
    {
        var appId = await _dataService.GetConfigSettingValueAsync("talkjs", "AppId");
        var secretKey = await _dataService.GetConfigSettingValueAsync("talkjs", "SecretKey");

        if (string.IsNullOrWhiteSpace(appId) || string.IsNullOrWhiteSpace(secretKey))
        {
            throw new InvalidOperationException(
                "TalkJS credentials not found in ConfigSetting (cs_type 'talkjs', identifiers 'AppId' and 'SecretKey')");
        }

        return (appId, secretKey);
    }
}
