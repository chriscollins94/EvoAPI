# Development Setup Guide

## Running All Services Together for Development

This guide shows how to run EvoWS (auth), EvoAPI (new API), and EvoTech (frontend) together for development and testing.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Development Setup                        │
├─────────────────────────────────────────────────────────────┤
│  Frontend (EvoTech)                                         │
│  https://localhost:3000/evotech                            │
│  ┌─────────────────┐                                       │
│  │    Next.js      │  Proxy Routes:                        │
│  │    Proxy        │  /api/evoapi/* → https://localhost:5001 │
│  │                 │  /api/evows/*  → https://localhost:44307│
│  └─────────────────┘  /evoWS/*      → https://localhost:44307│
└─────────────────────────────────────────────────────────────┘
           │                       │                    │
           ▼                       ▼                    ▼
┌──────────────────┐  ┌────────────────────┐  ┌─────────────────┐
│   EvoAPI         │  │      EvoWS         │  │    Database     │
│ (New .NET 8 API) │  │ (Legacy Auth API)  │  │                 │
│ Port: 5001       │  │ Port: 44307        │  │                 │
│                  │  │                    │  │                 │
│ - New endpoints  │  │ - Authentication   │  │                 │
│ - JWT validation │  │ - Cookie creation  │  │                 │
│ - Clean arch     │  │ - Legacy endpoints │  │                 │
└──────────────────┘  └────────────────────┘  └─────────────────┘
```

## Quick Start

### Option 1: Using VS Code Tasks (Recommended)

1. **Open EvoAPI project in VS Code**
2. **Press `Ctrl+Shift+P`** and type "Tasks: Run Task"
3. **Select "Start All Development Services"**

This will start both EvoAPI and EvoTech frontend automatically.

### Option 2: Manual Start

#### Terminal 1 - Start EvoAPI
```bash
cd c:\Users\chris\source\repos\EvoAPI
dotnet run --project src/EvoAPI.Api
```

#### Terminal 2 - Start EvoTech Frontend (with SSL)
```bash
cd c:\Users\chris\source\repos\evotech
npm install  # Install http-proxy-middleware if needed
npm run dev  # This uses your existing SSL server setup
```

#### Terminal 3 - Start EvoWS (Legacy Auth)
Keep running from Visual Studio as usual on https://localhost:44307/

## URL Structure

### Frontend Access
- **Main App**: https://localhost:3000/evotech
- **With Authentication**: Users log in through existing EvoWS system

### API Endpoints
From the frontend, you can now call:

#### New EvoAPI Endpoints
```javascript
// These get proxied to https://localhost:5001/api/*
fetch('/api/evoapi/sample/status')           // Public endpoint
fetch('/api/evoapi/sample/user-info')        // Authenticated endpoint  
fetch('/api/evoapi/sample/admin/system-info') // Admin-only endpoint
```

#### Legacy EvoWS Endpoints (for comparison)
```javascript
// These get proxied to https://localhost:44307/*
fetch('/api/evows/Evo/GetActiveEmployees')   // Legacy endpoint
fetch('/evoWS/api/Auth/Login')               // Authentication
```

## Cookie Sharing

The proxy setup ensures cookies are shared between all services:
- User logs in via EvoWS → `AccessToken` cookie is set
- Frontend calls EvoAPI → Cookie is automatically forwarded
- EvoAPI validates JWT from cookie → Authentication works!

## Testing Authentication

1. **Start all services** using the steps above
2. **Navigate to**: https://localhost:3000/evotech
3. **Log in** through your existing authentication system
4. **Test new endpoints**:
   - Visit: https://localhost:3000/evotech (should work with existing auth)
   - Call: `/api/evoapi/sample/user-info` (should return your user data)
   - Call: `/api/evoapi/sample/admin/system-info` (admin only)

## Development Workflow

### Adding New EvoAPI Endpoints

1. **Create controller** in `src/EvoAPI.Api/Controllers/`
2. **Use `BaseController`** for automatic authentication
3. **Test via frontend** proxy: `/api/evoapi/your-endpoint`
4. **Update frontend** to call new endpoint instead of legacy

### Example: Converting Schedule Endpoint

#### Old EvoWS Call
```javascript
// Current frontend call
const response = await fetch('/evoWS/api/Evo/GetWorkOrdersSchedule', {
    method: 'POST',
    body: JSON.stringify(filter),
    credentials: 'include'
});
```

#### New EvoAPI Call  
```javascript
// New frontend call
const response = await fetch('/api/evoapi/schedule/workorders', {
    method: 'GET',
    credentials: 'include'
});
```

## Debugging

### VS Code Debugging
- Set breakpoints in EvoAPI controllers
- Press `F5` or use "Launch EvoAPI + Frontend" configuration
- Frontend and API will start automatically

### Troubleshooting

#### Port Conflicts
- EvoAPI: 5001 (configurable in appsettings.Development.json)
- EvoTech: 3000 (configurable in package.json dev:proxy script)
- EvoWS: 44307 (from Visual Studio)

#### Cookie Issues
- Ensure all services run on localhost (not 127.0.0.1)
- Check browser dev tools → Application → Cookies
- `AccessToken` cookie should be visible and shared

#### Proxy Issues
- Check Next.js console for proxy errors
- Verify EvoWS and EvoAPI are running before starting frontend
- Update `next.config.js` proxy destinations if ports change

## Production Considerations

This setup is for development only. In production:
- Remove proxy configuration from `next.config.js`
- Use `npm run build` and `npm run export` for static builds
- Deploy APIs separately with proper CORS configuration
- Use environment variables for API endpoints
