using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using System.Text;
using EvoAPI.Api.Middleware;
using EvoAPI.Core.Interfaces;
using EvoAPI.Core.Services;
using EvoAPI.Infrastructure.Services;

var builder = WebApplication.CreateBuilder(args);

// Add user secrets to configuration
if (builder.Environment.IsDevelopment() || builder.Environment.EnvironmentName == "Test")
{
    builder.Configuration.AddUserSecrets<Program>();
}

// Add secrets file if it exists (for local development in any environment)
var secretsPath = Path.Combine(builder.Environment.ContentRootPath, "appsettings.secrets.json");
if (File.Exists(secretsPath))
{
    builder.Configuration.AddJsonFile("appsettings.secrets.json", optional: true, reloadOnChange: true);
}

// Log the current environment and connection string for debugging
var logger = LoggerFactory.Create(config => config.AddConsole()).CreateLogger("Startup");
logger.LogInformation("=== ENVIRONMENT CONFIGURATION ===");
logger.LogInformation("Environment: {Environment}", builder.Environment.EnvironmentName);
logger.LogInformation("ApplicationName: {ApplicationName}", builder.Environment.ApplicationName);
logger.LogInformation("ContentRootPath: {ContentRootPath}", builder.Environment.ContentRootPath);

// Debug: Check if DB_PASSWORD is loaded
var dbPassword = builder.Configuration["DB_PASSWORD"];
logger.LogInformation("DB_PASSWORD from config: {HasPassword}", !string.IsNullOrEmpty(dbPassword) ? "Found" : "NOT FOUND");

// Replace password placeholder with actual password from secrets/environment
var connectionString = builder.Configuration.GetConnectionString("DefaultConnection");
logger.LogInformation("Original ConnectionString: {ConnectionString}", connectionString?.Replace("Password=", "Password=***"));

if (!string.IsNullOrEmpty(connectionString) && connectionString.Contains("{DB_PASSWORD}"))
{
    if (!string.IsNullOrEmpty(dbPassword))
    {
        connectionString = connectionString.Replace("{DB_PASSWORD}", dbPassword);
        // This is the correct way to update the configuration
        builder.Configuration.GetSection("ConnectionStrings")["DefaultConnection"] = connectionString;
        logger.LogInformation("Password replacement successful");
    }
    else
    {
        logger.LogError("DB_PASSWORD not found in configuration - User Secrets may not be loaded properly");
    }
}

logger.LogInformation("Final ConnectionString: {ConnectionString}", connectionString?.Replace("Password=", "Password=***"));
logger.LogInformation("=== END ENVIRONMENT CONFIG ===");

// Configure Kestrel for HTTPS only in local development
if (builder.Environment.IsDevelopment() || builder.Environment.EnvironmentName == "Local" || builder.Environment.EnvironmentName == "Test")
{
    builder.WebHost.ConfigureKestrel(options =>
    {
        options.ListenLocalhost(5001, listenOptions =>
        {
            listenOptions.UseHttps();
        });
    });
}

// Add services to the container.
builder.Services.AddControllers();

// Register application services
builder.Services.AddScoped<IAuditService, AuditService>();
builder.Services.AddScoped<IDataService, DataService>();

// Register HttpClient for Google Maps service
builder.Services.AddHttpClient();
builder.Services.AddScoped<IGoogleMapsService, GoogleMapsService>();

// Add JWT Authentication
builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.TokenValidationParameters = new TokenValidationParameters
        {
            // Disable issuer/audience validation for compatibility with existing EvoWS tokens
            ValidateIssuer = false,
            ValidateAudience = false,
            ValidateLifetime = true,
            ValidateIssuerSigningKey = true,
            // Use the exact same key that EvoWS uses
            IssuerSigningKey = GetEvoWSSigningKey(),
            ClockSkew = TimeSpan.FromMinutes(5) // Allow some clock skew
        };
        
        // Support both Bearer tokens and cookies (for compatibility with existing system)
        options.Events = new JwtBearerEvents
        {
            OnMessageReceived = context =>
            {
                // Check for JWT in cookie if not in Authorization header
                if (string.IsNullOrEmpty(context.Token))
                {
                    context.Token = context.Request.Cookies["AccessToken"];
                }
                return Task.CompletedTask;
            }
        };
    });

// Add Authorization policies
builder.Services.AddAuthorization(options =>
{
    options.AddPolicy("AdminOnly", policy => policy.RequireClaim("accesslevel", "ADMIN"));
    options.AddPolicy("UserAdminOnly", policy => 
    {
        policy.RequireClaim("accesslevel", "ADMIN");
        policy.RequireClaim("function", "Admin - User Admin Edit");
    });
    options.AddPolicy("LoggedIn", policy => policy.RequireAuthenticatedUser());
});

// Add CORS support
builder.Services.AddCors(options =>
{
    options.AddPolicy("EvoPolicy", policy =>
    {
        policy.WithOrigins(
            "https://evoprod.azurewebsites.net",
            "https://evoqa.azurewebsites.net", 
            "https://evotest.azurewebsites.net",
            "https://localhost:44393",
            "https://localhost:3000",
            "http://localhost:3000"
        )
        .AllowAnyHeader()
        .AllowAnyMethod()
        .AllowCredentials();
    });
});

// Add Swagger/OpenAPI
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
// Enable Swagger in all environments for development/testing
app.UseSwagger();
app.UseSwaggerUI();

app.UseHttpsRedirection();
app.UseCors("EvoPolicy");

// Add audit middleware
app.UseMiddleware<AuditMiddleware>();

app.UseAuthentication();
app.UseAuthorization();

app.MapControllers();

app.Run();

// Helper method to create the same signing key that EvoWS uses
static SymmetricSecurityKey GetEvoWSSigningKey()
{
    // The EvoWS system uses Encoding.ASCII.GetBytes() on the config value string
    // The config contains a string representation of a byte array, but it's treated as a string
    var keyString = "{ 08, 98, 50, 42, 23, 02, 49, 3, 45, 94, 236, 171, 97, 208, 160, 38, 99, 76, 251, 210, 86, 6, 90, 121, 208, 251, 70, 178, 75, 208, 67, 26, 62, 110, 190, 160, 162, 162, 97, 168, 177, 209, 30, 40, 82, 208, 50, 193, 118, 119, 135, 47, 74, 94, 228, 99, 54, 22, 189, 248, 169, 43, 168, 161 }";
    var keyBytes = Encoding.ASCII.GetBytes(keyString);
    return new SymmetricSecurityKey(keyBytes);
}
