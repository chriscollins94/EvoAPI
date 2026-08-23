using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;

namespace EvoAPI.Api.Controllers;

[ApiController]
[Route("EvoApi/chat")]
public class ChatController : BaseController
{
    private readonly IChatService _chatService;

    public ChatController(
        IChatService chatService,
        IAuditService auditService)
    {
        _chatService = chatService;
        InitializeAuditService(auditService);
    }

    [HttpGet("company-chat/participants")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<CompanyChatParticipantsDto>>> GetParticipants()
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var result = await _chatService.GetCompanyChatParticipantsAsync();

            stopwatch.Stop();
            await LogAuditAsync("ChatGetParticipants",
                new { result.ConversationId, result.ConversationExists, count = result.Participants.Count },
                stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<CompanyChatParticipantsDto>
            {
                Success = true,
                Message = result.ConversationExists
                    ? $"Found {result.Participants.Count} participants"
                    : "Conversation does not exist yet in this TalkJS app",
                Data = result,
                Count = result.Participants.Count
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("ChatGetParticipants", ex, null);
            return StatusCode(500, new ApiResponse<CompanyChatParticipantsDto>
            {
                Success = false,
                Message = $"Failed to get chat participants: {ex.Message}"
            });
        }
    }

    [HttpDelete("company-chat/participants/{userId}")]
    [AdminOnly]
    public async Task<ActionResult<ApiResponse<object>>> RemoveParticipant(string userId)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            if (string.IsNullOrWhiteSpace(userId))
            {
                return BadRequest(new ApiResponse<object>
                {
                    Success = false,
                    Message = "User id is required"
                });
            }

            await _chatService.RemoveCompanyChatParticipantAsync(userId.Trim());

            stopwatch.Stop();
            await LogAuditAsync("ChatRemoveParticipant", new { userId }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

            return Ok(new ApiResponse<object>
            {
                Success = true,
                Message = $"Removed participant {userId} from the company chat"
            });
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            await LogAuditErrorAsync("ChatRemoveParticipant", ex, new { userId });
            return StatusCode(500, new ApiResponse<object>
            {
                Success = false,
                Message = $"Failed to remove participant: {ex.Message}"
            });
        }
    }
}
