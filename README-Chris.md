taskkill /F /IM dotnet.exe

$env:ASPNETCORE_ENVIRONMENT="Test"; dotnet run --project src/EvoAPI.Api

dotnet run --project src/EvoAPI.Api --environment Local
dotnet run --project src/EvoAPI.Api --environment Test
dotnet run --project src/EvoAPI.Api --environment Production



dotnet publish src/EvoAPI.Api -c Release -o publish/evoapi

