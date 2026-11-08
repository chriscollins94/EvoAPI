-- NTE notification: what has been sent. Read-only, safe on TEST or PROD.
-- Newest first. an_body is cut to 120 chars here; select the row by an_id for the full message.
--
-- an_status      Sent / Failed / Skipped (reason in an_errormessage) / Sending (in flight) / Reviewed (review-period
--                rows released at go-live cutover; the tech gets a real alert at their next check-in)
-- an_overridden  1 = rerouted by SmokeTestOverride* (an_recipient is the reviewer, an_intendedrecipient the tech/zone)
-- an_attempts    > 1 = retried after a Failed send; earlier errors kept in an_errormessage after ' || earlier: '

SET NOCOUNT ON;

SELECT TOP 100
       an_id, an_insertdatetime, an_sentdatetime, an_status, an_attempts,
       an_channel, an_threshold, an_percent, an_nte, an_spent,
       an_srnumber, sr_id, an_techname, u_id, an_zonename,
       an_intendedrecipient, an_recipient, an_overridden,
       an_subject, LEFT(an_body, 120) AS an_body_120,
       an_errormessage, an_servername
FROM dbo.AuditNTE
ORDER BY an_id DESC;

-- Totals by status and channel for the last 30 days
SELECT an_status, an_channel, COUNT(*) AS rows_, SUM(CASE WHEN an_overridden = 1 THEN 1 ELSE 0 END) AS overridden
FROM dbo.AuditNTE
WHERE an_insertdatetime >= DATEADD(DAY, -30, GETDATE())
GROUP BY an_status, an_channel
ORDER BY an_status, an_channel;
