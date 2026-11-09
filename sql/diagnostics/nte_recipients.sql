-- NTE notification: the recipient query the scan runs (copy of NteQueryRepository.ActiveServiceRequestsSql).
-- Read-only. One row per (service request, checked-in tech). Only SRs with a tech checked in right now
-- (open check-in newer than @MaxOpenCheckInHours) are returned; nothing else enters the scan (D9).
-- Keep in sync with the C# constant when the rule changes. See NTE_NOTIFICATION_PLAN.md D2 / D8 / D9.

DECLARE @MaxOpenCheckInHours int = 24;   -- NteConfig/MaxOpenCheckInHours

WITH CheckedIn AS (
    SELECT DISTINCT wo.sr_id, tt.u_id
    FROM dbo.TimeTracking tt WITH (NOLOCK)
    INNER JOIN dbo.WorkOrder wo WITH (NOLOCK) ON wo.wo_id = tt.wo_id
    WHERE tt.ttt_id = 3
      AND tt.tt_end IS NULL
      AND tt.u_id IS NOT NULL
      AND tt.tt_begin >= DATEADD(HOUR, -@MaxOpenCheckInHours, GETUTCDATE())
)
SELECT
    sr.sr_id                                AS SrId,
    sr.sr_requestnumber                     AS SrNumber,
    sr.sr_nte                               AS Nte,
    c.c_name                                AS CustomerName,
    l.l_location                            AS LocationName,
    sr.wo_id_primary                        AS PrimaryWoId,
    u.u_id                                  AS TechUserId,
    u.u_firstname                           AS TechFirstName,
    u.u_lastname                            AS TechLastName,
    COALESCE(NULLIF(LTRIM(RTRIM(u.u_phonemobile)), ''),
             NULLIF(LTRIM(RTRIM(u.u_phonehome)), ''))   AS TechMobile,
    CASE WHEN NULLIF(LTRIM(RTRIM(u.u_phonemobile)), '') IS NOT NULL THEN 'EvoCell'
         WHEN NULLIF(LTRIM(RTRIM(u.u_phonehome)), '')   IS NOT NULL THEN 'PersonalCell'
         ELSE NULL END                      AS TechMobileSource,
    CAST(1 AS bit)                          AS TechIsCheckedIn,
    z.z_id                                  AS ZoneId,
    z.z_description                         AS ZoneName,
    z.z_email                               AS ZoneEmail
FROM dbo.ServiceRequest sr WITH (NOLOCK)
INNER JOIN CheckedIn ci ON ci.sr_id = sr.sr_id
LEFT JOIN dbo.[user] u WITH (NOLOCK) ON u.u_id = ci.u_id
LEFT JOIN dbo.Zone z WITH (NOLOCK) ON z.z_id = u.z_id
LEFT JOIN dbo.xrefCompanyCallCenter xccc WITH (NOLOCK) ON xccc.xccc_id = sr.xccc_id
LEFT JOIN dbo.Company c WITH (NOLOCK) ON c.c_id = xccc.c_id
LEFT JOIN dbo.Location l WITH (NOLOCK) ON l.l_id = sr.l_id
WHERE sr.s_id NOT IN (6, 9)
  AND sr.sr_nte IS NOT NULL
  AND sr.sr_nte > 0
ORDER BY sr.sr_id, u.u_id;
