taskkill /F /IM dotnet.exe

$env:ASPNETCORE_ENVIRONMENT="Test"; dotnet run --project src/EvoAPI.Api

dotnet run --project src/EvoAPI.Api --environment Local
dotnet run --project src/EvoAPI.Api --environment Test
dotnet run --project src/EvoAPI.Api --environment Production

# Publishing
dotnet publish src/EvoAPI.Api -c Release -o publish/evoapi

# Production deployment notes:
# - Set ASPNETCORE_ENVIRONMENT=Production in production environment
# - Ensure appsettings.Production.json exists with production config
# - Deploy contents of publish/evoapi folder to production server

