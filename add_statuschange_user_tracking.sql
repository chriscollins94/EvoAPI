-- =============================================================================
-- Status change user tracking
-- Adds u_id to StatusChange / StatusSecondaryChange, stamps it in the status
-- triggers from SESSION_CONTEXT('app_user_id') (already set by EvoWS on every
-- connection; EvoAPI sets it as of the same deploy), and backfills history
-- from the ServiceRequestActivity audit table.
--
-- Run on TEST first, then PROD together with the EvoAPI + evotech deploy.
-- Safe to re-run (column adds are guarded; backfills only touch NULL u_id).
-- =============================================================================

IF COL_LENGTH('dbo.StatusChange', 'u_id') IS NULL
    ALTER TABLE dbo.StatusChange ADD u_id INT NULL;
GO

IF COL_LENGTH('dbo.StatusSecondaryChange', 'u_id') IS NULL
    ALTER TABLE dbo.StatusSecondaryChange ADD u_id INT NULL;
GO

-- -----------------------------------------------------------------------------
-- Primary status trigger: same logic as before + u_id from session context.
-- -----------------------------------------------------------------------------
ALTER TRIGGER [dbo].[trgServiceRequestStatusChange] ON dbo.ServiceRequest FOR INSERT, UPDATE AS
SET NOCOUNT ON

	DECLARE @sr_id int;
	DECLARE @s_id_current int;
	DECLARE @s_id_new int;
	DECLARE @ss_id_new int;

	DECLARE @dt_priorstatus datetime;
	DECLARE @minutesinpriorstatus int;

	SELECT @sr_id = (SELECT top 1 sr_id FROM inserted)
	SELECT @s_id_current = (SELECT s_id FROM deleted WHERE sr_id = @sr_id)
	SELECT @s_id_new = (SELECT top 1 s_id FROM inserted)
	SELECT @ss_id_new = (SELECT top 1 ss_id FROM inserted)

	SELECT @dt_priorstatus = (SELECT top 1 sc_insertdatetime FROM StatusChange WHERE s_id_new = @s_id_current AND sr_id = @sr_id ORDER BY sc_id DESC)
	SELECT @minutesinpriorstatus = DATEDIFF(mi, @dt_priorstatus, GETDATE())

	IF (@sr_id IS NOT NULL) AND (@s_id_new IS NOT NULL) AND ((@s_id_current <> @s_id_new) OR (@s_id_current IS NULL))
	BEGIN
		INSERT StatusChange (sr_id, s_id_prior, s_id_new, sc_minutesinpriorstatus, u_id)
		VALUES (@sr_id, @s_id_current, @s_id_new, @minutesinpriorstatus,
		        TRY_CONVERT(INT, SESSION_CONTEXT(N'app_user_id')))
	END
GO

-- -----------------------------------------------------------------------------
-- Secondary status trigger: same logic as before (including the
-- sr_nteexcludequotedsi maintenance) + u_id from session context.
-- -----------------------------------------------------------------------------
ALTER TRIGGER [dbo].[trgWorkOrderStatusSecondaryChange] ON [dbo].[WorkOrder] FOR INSERT, UPDATE AS
SET NOCOUNT ON

	DECLARE @sr_id int;
	DECLARE @wo_id int;
	DECLARE @ss_id_current int;
	DECLARE @ss_id_new int;

	DECLARE @dt_priorstatus datetime;
	DECLARE @minutesinpriorstatus int;

	SELECT @sr_id = (SELECT top 1 sr_id FROM inserted)
	SELECT @wo_id = (SELECT top 1 wo_id FROM inserted)
	SELECT @ss_id_current = (SELECT ss_id FROM deleted WHERE wo_id = @wo_id)
	SELECT @ss_id_new = (SELECT top 1 ss_id FROM inserted)

	SELECT @dt_priorstatus = (SELECT top 1 ssc_insertdatetime FROM StatusSecondaryChange WHERE ss_id_new = @ss_id_current AND wo_id = @wo_id ORDER BY ssc_id DESC)
	SELECT @minutesinpriorstatus = DATEDIFF(mi, @dt_priorstatus, GETDATE())

	IF (@wo_id IS NOT NULL) AND (@ss_id_new IS NOT NULL) AND ((@ss_id_current <> @ss_id_new) OR (@ss_id_current IS NULL))
	BEGIN
		INSERT StatusSecondaryChange (wo_id, ss_id_prior, ss_id_new, ssc_minutesinpriorstatus, u_id)
		VALUES (@wo_id, @ss_id_current, @ss_id_new, @minutesinpriorstatus,
		        TRY_CONVERT(INT, SESSION_CONTEXT(N'app_user_id')))
	END


	IF (@ss_id_new <> 10)
	BEGIN
		UPDATE ServiceRequest SET sr_nteexcludequotedsi = 0 WHERE sr_id = @sr_id
	END

	IF (@ss_id_new = 10)
	BEGIN
		UPDATE ServiceRequest SET sr_nteexcludequotedsi = 1 WHERE sr_id = @sr_id
	END
GO

-- -----------------------------------------------------------------------------
-- Backfill primary status changes from the ServiceRequestActivity audit trail.
-- Both triggers fire on the same statement, so a matching activity row exists
-- for every StatusChange row since activity logging began; match on sr_id,
-- the s_id transition in the JSON snapshots, and a +/-3 second window.
-- -----------------------------------------------------------------------------
-- Rows older than activity logging can never match, so bound the scan —
-- without this the update walks the entire StatusChange history (100k+ rows).
DECLARE @activityStart datetime = (SELECT MIN(sra_insertdatetime) FROM dbo.ServiceRequestActivity);

UPDATE sc
SET sc.u_id = a.app_user_id
FROM dbo.StatusChange sc
CROSS APPLY (
    SELECT TOP 1 sra.app_user_id
    FROM dbo.ServiceRequestActivity sra
    WHERE sra.entity_name = 'ServiceRequest'
      AND sra.sr_id = sc.sr_id
      AND sra.app_user_id IS NOT NULL
      AND ABS(DATEDIFF(SECOND, sra.sra_insertdatetime, sc.sc_insertdatetime)) <= 3
      AND ISNULL(JSON_VALUE(sra.old_values, '$.s_id'), '-1') = ISNULL(CAST(sc.s_id_prior AS VARCHAR(10)), '-1')
      AND JSON_VALUE(sra.new_values, '$.s_id') = CAST(sc.s_id_new AS VARCHAR(10))
    ORDER BY sra.sra_id DESC
) a
WHERE sc.u_id IS NULL
  AND sc.sc_insertdatetime >= @activityStart;
GO

-- -----------------------------------------------------------------------------
-- Backfill secondary status changes from WorkOrder activity rows.
-- -----------------------------------------------------------------------------
DECLARE @activityStart2 datetime = (SELECT MIN(sra_insertdatetime) FROM dbo.ServiceRequestActivity);

UPDATE ssc
SET ssc.u_id = a.app_user_id
FROM dbo.StatusSecondaryChange ssc
CROSS APPLY (
    SELECT TOP 1 sra.app_user_id
    FROM dbo.ServiceRequestActivity sra
    WHERE sra.entity_name = 'WorkOrder'
      AND sra.entity_id = ssc.wo_id
      AND sra.app_user_id IS NOT NULL
      AND ABS(DATEDIFF(SECOND, sra.sra_insertdatetime, ssc.ssc_insertdatetime)) <= 3
      AND ISNULL(JSON_VALUE(sra.old_values, '$.ss_id'), '-1') = ISNULL(CAST(ssc.ss_id_prior AS VARCHAR(10)), '-1')
      AND JSON_VALUE(sra.new_values, '$.ss_id') = CAST(ssc.ss_id_new AS VARCHAR(10))
    ORDER BY sra.sra_id DESC
) a
WHERE ssc.u_id IS NULL
  AND ssc.ssc_insertdatetime >= @activityStart2;
GO

-- Quick verification
SELECT 'StatusChange' AS tbl, COUNT(*) AS total, SUM(CASE WHEN u_id IS NOT NULL THEN 1 ELSE 0 END) AS with_user FROM dbo.StatusChange
UNION ALL
SELECT 'StatusSecondaryChange', COUNT(*), SUM(CASE WHEN u_id IS NOT NULL THEN 1 ELSE 0 END) FROM dbo.StatusSecondaryChange;
GO
