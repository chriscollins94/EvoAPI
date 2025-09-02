# Security Configuration

## Database Password Management

The database passwords are **NOT** stored in source control for security. Instead, they are managed through:

### Development Environment
- Uses **User Secrets** (`dotnet user-secrets`)
- Password is stored locally on developer machine
- To set up: `dotnet user-secrets set "DB_PASSWORD" "your-password-here"`

### Test Environment
- Uses **Environment Variables**
- Set `DB_PASSWORD` environment variable before running
- Example: `$env:DB_PASSWORD = "your-password"; dotnet run`

### Production Environment
- Uses **Environment Variables** or **Azure Key Vault**
- Set `DB_PASSWORD` environment variable on server
- Connection strings use `{DB_PASSWORD}` placeholder which gets replaced at runtime

## Configuration Files

All `appsettings.*.json` files use the placeholder `{DB_PASSWORD}` instead of actual passwords:

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=...;Password={DB_PASSWORD};..."
  }
}
```

## Setting Up New Developer

1. Clone the repository
2. Initialize user secrets: `dotnet user-secrets init --project src/EvoAPI.Api`
3. Set the password: `dotnet user-secrets set "DB_PASSWORD" "actual-password"`
4. Run the application: `dotnet run --project src/EvoAPI.Api`

## Deployment

### Azure Web App
Set the `DB_PASSWORD` environment variable in the Azure portal under Configuration > Application Settings.

### Other Hosting
Ensure the `DB_PASSWORD` environment variable is set before starting the application.

## Important Notes

- **Never commit actual passwords to Git**
- User secrets are stored outside the project directory and won't be committed
- Environment variables override configuration files
- The `{DB_PASSWORD}` placeholder is replaced at application startup
