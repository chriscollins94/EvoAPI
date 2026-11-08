-- NTE notification: go-live cutover, run BY HAND once, on PROD, at the moment the override rows are
-- cleared (end of the review period). NOT part of a deploy; safe to rerun.
--
-- Why: every text/email sent while SmokeTestOverrideMobile / SmokeTestOverrideEmail were set went to
-- the reviewers, not the tech, but it still occupies its dedup key in AuditNTE. Left as 'Sent', a tech
-- checked in on one of those SRs would never be warned. 'Reviewed' keeps the row for the record but
-- the scan's fired-key lookup ignores it, so the next check-in on that SR alerts the tech for real.
--
-- Only rows that were actually rerouted (an_overridden = 1) are touched. Rows sent to a real tech
-- during the EnabledTechIds phase are genuine and stay 'Sent'.

UPDATE dbo.AuditNTE
   SET an_status = 'Reviewed',
       an_modifieddatetime = GETDATE()
 WHERE an_overridden = 1
   AND an_status IN ('Sent', 'Skipped', 'Failed');

SELECT @@ROWCOUNT AS rows_marked_reviewed;
GO
