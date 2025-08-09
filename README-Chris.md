taskkill /F /IM dotnet.exe
dotnet run --project src/EvoAPI.Api
dotnet run --project src/EvoAPI.Api --environment Local
dotnet run --project src/EvoAPI.Api --environment Test
dotnet run --project src/EvoAPI.Api --environment Production