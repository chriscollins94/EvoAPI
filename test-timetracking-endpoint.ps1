# Test script for TimeTrackingDetail endpoint
# This script tests the new /EvoApi/timetrackingdetail endpoint

$uri = "https://localhost:5001/EvoApi/timetrackingdetail"
$body = @{
    u_id = 1
    ttt_id = 1
    wo_id = 123
} | ConvertTo-Json

$headers = @{
    "Content-Type" = "application/json"
}

Write-Host "Testing TimeTrackingDetail endpoint..."
Write-Host "URI: $uri"
Write-Host "Body: $body"

try {
    # Note: This will fail without proper authentication, but it will test the endpoint exists
    $response = Invoke-RestMethod -Uri $uri -Method Post -Body $body -Headers $headers -SkipCertificateCheck
    Write-Host "Success: $($response | ConvertTo-Json -Depth 3)"
} catch {
    Write-Host "Expected error (authentication required): $($_.Exception.Message)"
    if ($_.Exception.Response.StatusCode -eq 401) {
        Write-Host "✓ Endpoint exists and requires authentication (expected behavior)"
    } else {
        Write-Host "✗ Unexpected error: $($_.Exception.Response.StatusCode)"
    }
}