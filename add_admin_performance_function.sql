-- Adds the 'Admin - Performance' function and attaches it to the System
-- Administrator role. Gates the Performance settings/report pages in evotech
-- and the EvoApi/performance endpoints ([PerformanceOnly]).
--
-- Users need to log out/in after this so a fresh JWT picks up the new claim.

IF NOT EXISTS (SELECT 1 FROM [Function] WHERE f_functionidentifier = 'Admin - Performance')
BEGIN
    INSERT INTO [Function] (f_function, f_functionidentifier)
    VALUES ('Performance Upload & Dashboard', 'Admin - Performance');
END;
GO

-- Attach to the System Administrator role (adjust r_role if a different role
-- should carry it, or add extra INSERTs for additional roles).
INSERT INTO xrefRoleFunction (r_id, f_id)
SELECT r.r_id, f.f_id
FROM Role r
CROSS JOIN [Function] f
WHERE r.r_role = 'System Administrator'
  AND f.f_functionidentifier = 'Admin - Performance'
  AND NOT EXISTS (
      SELECT 1 FROM xrefRoleFunction xrf
      WHERE xrf.r_id = r.r_id AND xrf.f_id = f.f_id
  );
GO
