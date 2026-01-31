using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs.Authentication;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace EvoAPI.Core.Services;

public class JwtTokenService : IJwtTokenService
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<JwtTokenService> _logger;

    public JwtTokenService(
        IConfiguration configuration,
        ILogger<JwtTokenService> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public JwtTokenResult CreateJwtToken(AuthenticatedUser user, string xsrfToken, int timeoutInMinutes)
    {
        _logger.LogInformation("Creating JWT token for user: {Username}", user.Username);

        // Enforce max timeout
        var maxTimeout = _configuration.GetValue<int>("Jwt:MaxExpiryInMinutes", 1440);
        if (timeoutInMinutes > maxTimeout)
        {
            timeoutInMinutes = maxTimeout;
            _logger.LogWarning("Token timeout exceeded max, capping at {MaxTimeout} minutes", maxTimeout);
        }

        var expiration = DateTime.UtcNow.AddMinutes(timeoutInMinutes);
        var expiresInSeconds = (int)TimeSpan.FromMinutes(timeoutInMinutes).TotalSeconds;

        // Build claims list - must match EvoWS format
        var claims = new List<Claim>
        {
            new Claim(ClaimTypes.Name, user.Username),
            new Claim("username", user.Username),
            new Claim("firstname", user.FirstName),
            new Claim("lastname", user.LastName),
            new Claim("passwordchanged", user.PasswordChanged?.ToString("o") ?? string.Empty),
            new Claim("picture", user.Picture),
            new Claim("id", user.UserId.ToString()),
            new Claim("XSRF-TOKEN", xsrfToken)
        };

        // Add function claims (multiple claims with same key)
        foreach (var function in user.Functions)
        {
            claims.Add(new Claim("function", function));
        }

        // Add access level claim
        claims.Add(new Claim("accesslevel", user.AccessLevel));

        // Get signing key - use the same method as Program.cs
        var keyString = "{ 08, 98, 50, 42, 23, 02, 49, 3, 45, 94, 236, 171, 97, 208, 160, 38, 99, 76, 251, 210, 86, 6, 90, 121, 208, 251, 70, 178, 75, 208, 67, 26, 62, 110, 190, 160, 162, 162, 97, 168, 177, 209, 30, 40, 82, 208, 50, 193, 118, 119, 135, 47, 74, 94, 228, 99, 54, 22, 189, 248, 169, 43, 168, 161 }";
        var keyBytes = Encoding.ASCII.GetBytes(keyString);
        var signingKey = new SymmetricSecurityKey(keyBytes);

        var tokenDescriptor = new SecurityTokenDescriptor
        {
            Subject = new ClaimsIdentity(claims),
            Expires = expiration,
            SigningCredentials = new SigningCredentials(signingKey, SecurityAlgorithms.HmacSha256),
            // Don't set Issuer/Audience for EvoWS compatibility
        };

        var tokenHandler = new JwtSecurityTokenHandler();
        var token = tokenHandler.CreateToken(tokenDescriptor);
        var tokenString = tokenHandler.WriteToken(token);

        _logger.LogInformation("JWT token created successfully for user: {Username}, expires: {Expiration}",
            user.Username, expiration);

        return new JwtTokenResult
        {
            Token = tokenString,
            XsrfToken = xsrfToken,
            Expiration = expiration,
            ExpiresInSeconds = expiresInSeconds
        };
    }

    public ClaimsPrincipal? ValidateJwtToken(string jwtToken)
    {
        try
        {
            var keyString = "{ 08, 98, 50, 42, 23, 02, 49, 3, 45, 94, 236, 171, 97, 208, 160, 38, 99, 76, 251, 210, 86, 6, 90, 121, 208, 251, 70, 178, 75, 208, 67, 26, 62, 110, 190, 160, 162, 162, 97, 168, 177, 209, 30, 40, 82, 208, 50, 193, 118, 119, 135, 47, 74, 94, 228, 99, 54, 22, 189, 248, 169, 43, 168, 161 }";
            var keyBytes = Encoding.ASCII.GetBytes(keyString);
            var signingKey = new SymmetricSecurityKey(keyBytes);

            var tokenHandler = new JwtSecurityTokenHandler();
            var validationParameters = new TokenValidationParameters
            {
                ValidateIssuer = false,
                ValidateAudience = false,
                ValidateLifetime = true,
                ValidateIssuerSigningKey = true,
                IssuerSigningKey = signingKey,
                ClockSkew = TimeSpan.FromMinutes(5)
            };

            var principal = tokenHandler.ValidateToken(jwtToken, validationParameters, out _);
            return principal;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "JWT token validation failed");
            return null;
        }
    }
}
