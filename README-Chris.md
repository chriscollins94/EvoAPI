taskkill /F /IM dotnet.exe

$env:ASPNETCORE_ENVIRONMENT="Test"; dotnet run --project src/EvoAPI.Api

# Publishing
## Test Environment
dotnet publish src/EvoAPI.Api -c Release -o publish/evoapi-test

## Production Environment  
dotnet publish src/EvoAPI.Api -c Release -o publish/evoapi

# Deployment notes:
## Test deployment:
# - Set ASPNETCORE_ENVIRONMENT=Test in test environment
# - Deploy contents of publish/evoapi-test folder to test server
# - Uses appsettings.Test.json configuration

## Production deployment:
# - Set ASPNETCORE_ENVIRONMENT=Production in production environment
# - Deploy contents of publish/evoapi folder to production server  
# - Uses appsettings.Production.json configuration

