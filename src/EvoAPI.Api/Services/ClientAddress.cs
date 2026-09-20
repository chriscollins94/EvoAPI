namespace EvoAPI.Api.Services;

/// Best-effort client address behind Azure App Service, used to partition rate limits and to
/// stamp audit rows for anonymous calls.
///
/// App Service terminates TLS and APPENDS the true caller to X-Forwarded-For. A caller can send
/// its own X-Forwarded-For with any value it likes, but it cannot control the entry Azure adds
/// at the END. So the LAST entry is the one to trust; taking the first would let a caller pick a
/// fresh rate-limit bucket per request simply by spoofing the header. Locally there is no proxy
/// and the connection address is used.
public static class ClientAddress
{
    public static string Resolve(HttpContext context)
    {
        var forwarded = context.Request.Headers["X-Forwarded-For"].ToString();
        if (!string.IsNullOrWhiteSpace(forwarded))
        {
            var last = forwarded.Split(',').Last().Trim();
            // App Service includes the port ("1.2.3.4:51234"); drop it so one client is one bucket.
            var colon = last.LastIndexOf(':');
            if (colon > 0 && last.IndexOf(':') == colon) last = last[..colon];
            if (!string.IsNullOrEmpty(last)) return last;
        }
        return context.Connection.RemoteIpAddress?.ToString() ?? "unknown";
    }
}
