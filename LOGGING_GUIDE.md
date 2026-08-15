# EvoAPI Logging Guide

## Where Logs Are Written

### Development Environment
- **Console/Terminal** - All logs appear in the console where you run `dotnet run`
- **Debug Window** - If running from Visual Studio, logs appear in the Debug Output window
- **Level**: Information and above (configured in appsettings.json)

### Production Environment
You need to configure a logging provider. Options:

#### Option 1: Application Insights (Recommended for Azure)
```bash
dotnet add package Microsoft.ApplicationInsights.AspNetCore
```

Then in `Program.cs`:
```csharp
builder.Services.AddApplicationInsightsTelemetry();
```

#### Option 2: File Logging (Serilog)
```bash
dotnet add package Serilog.AspNetCore
dotnet add package Serilog.Sinks.File
```

Then in `Program.cs`:
```csharp
using Serilog;

Log.Logger = new LoggerConfiguration()
    .WriteTo.File("logs/evoapi-.txt", rollingInterval: RollingInterval.Day)
    .CreateLogger();

builder.Host.UseSerilog();
```

#### Option 3: Database Logging
```bash
dotnet add package Serilog.Sinks.MSSqlServer
```

## Monitoring Login Performance

### Key Metrics to Track

The authentication endpoints now log these metrics:

1. **Login Duration** (via Stopwatch):
```csharp
var stopwatch = Stopwatch.StartNew();
// ... login logic ...
stopwatch.Stop();
_logger.LogInformation("Login completed in {ElapsedMs}ms for user: {Username}",
    stopwatch.ElapsedMilliseconds, request.Username);
```

2. **Component Timing** (add these to AuthenticationService.cs):
```csharp
var sw = Stopwatch.StartNew();
var user = await _dataService.ExecuteQueryAsync(sql, parameters);
_logger.LogInformation("ValidateCredentials query took {ElapsedMs}ms", sw.ElapsedMilliseconds);

sw.Restart();
var functions = await GetUserPermissionsAsync(username);
_logger.LogInformation("GetUserPermissions took {ElapsedMs}ms", sw.ElapsedMilliseconds);
```

### Log Queries

**Current logs for authentication**:
- `Login Attempt` - Fire-and-forget audit
- `Login Success` - Fire-and-forget audit
- `Login Failed` - Fire-and-forget audit
- `Login error for user: {Username}` - Logged with error details

**Search patterns**:
```
// Application Insights (KQL)
traces
| where message contains "Login"
| where timestamp > ago(1h)
| project timestamp, message, customDimensions

// Serilog file logs (grep)
grep "Login" logs/evoapi-*.txt
grep "ElapsedMs" logs/evoapi-*.txt
```

## Performance Baseline

**Target Performance**:
- Login endpoint: < 500ms (p95)
- GetUserToken endpoint: < 100ms (p95)

**Optimization Thresholds**:
- If ValidateCredentials > 200ms → Check User table index
- If GetUserPermissions > 300ms → Check role/function join indexes
- If CreateLoginTracking > 100ms → Check TimeTracking table index

## Alerting Recommendations

Set up alerts for:
1. Login failures > 10/minute
2. Login duration > 2 seconds
3. 500 errors on authentication endpoints

## Current Configuration

**appsettings.json**:
```json
"Logging": {
  "LogLevel": {
    "Default": "Information",
    "Microsoft.AspNetCore": "Warning"
  }
}
```

This means:
- ✅ `_logger.LogInformation()` - Written
- ✅ `_logger.LogWarning()` - Written
- ✅ `_logger.LogError()` - Written
- ❌ `_logger.LogDebug()` - NOT written (unless changed to Debug)

## Recommended Next Steps

1. **For Production**: Add Application Insights or Serilog file logging
2. **For Performance Monitoring**: Add component-level timing logs
3. **For Alerting**: Configure alerts based on error rates and latency
