-- XRF revisit tables.
-- Certain High Volume locations need a second trip so the tech can shoot the service
-- line with the XRF analyzer. Each XRF location is keyed by PREMISE NUMBER back to its
-- HighVolumeBatchDetail row (hvbd_id is resolved when the wave is loaded; see
-- sql/seed/insert_xrf_batch_template.sql). The page shows the original High Volume visit
-- read-only and the tech picks a result (Complete / Error / Inaccessible / Dirty/Wet), adds an
-- optional comment and presses "Submit XRF". Any result finalizes the stop.
--
-- Meter number is NOT the key: source lists proved unreliable for it. It is kept on the row,
-- optional and stored as given, so a later report can compare it with High Volume's value.
--
-- XRF is independent of service requests, work orders and billing for now.
--
-- Run on TEST first; run on PROD together with the EvoAPI + evotech deploy.

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'XrfBatch')
BEGIN
    CREATE TABLE dbo.XrfBatch (
        xrfb_id               INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_XrfBatch PRIMARY KEY,
        xrfb_insertdatetime   DATETIME     NOT NULL CONSTRAINT DF_XrfBatch_insertdatetime DEFAULT (GETDATE()),
        xrfb_modifieddatetime DATETIME     NULL,
        xrfb_filename         NVARCHAR(50) NOT NULL   -- wave name shown in the "Select a Wave" dropdown
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'XrfBatchDetail')
BEGIN
    CREATE TABLE dbo.XrfBatchDetail (
        xrfbd_id                INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_XrfBatchDetail PRIMARY KEY,
        xrfb_id                 INT            NOT NULL CONSTRAINT FK_XrfBatchDetail_XrfBatch REFERENCES dbo.XrfBatch (xrfb_id),
        hvbd_id                 INT            NULL,      -- matched HighVolumeBatchDetail row; NULL when no HV row existed for the premise at load
        xrfbd_premisenumber     NVARCHAR(50)   NOT NULL,  -- key back to High Volume (hvbd_premisenumber)
        xrfbd_meternumber       NVARCHAR(50)   NULL,      -- informational only, as given on the wave list; never joined on or displayed
        xrfbd_team              NVARCHAR(10)   NULL,      -- team assigned for the revisit (defaults to the HV team at load)
        xrfbd_comment           NVARCHAR(1000) NULL,      -- optional tech comment entered at submit
        xrfbd_result            NVARCHAR(20)   NULL,      -- required at submit: Complete | Error | Inaccessible | Dirty/Wet | XRF Not Used
        u_id                    INT            NULL,      -- tech who submitted the result
        xrfbd_completeddatetime DATETIME       NULL,
        xrfbd_latitude          DECIMAL(9, 6)  NULL,      -- device position when Submit XRF was pressed (NULL when unavailable/denied)
        xrfbd_longitude         DECIMAL(9, 6)  NULL,
        xrfbd_geoaccuracy       INT            NULL,      -- accuracy radius in meters reported by the device
        xrfbd_insertdatetime    DATETIME       NOT NULL CONSTRAINT DF_XrfBatchDetail_insertdatetime DEFAULT (GETDATE()),
        xrfbd_modifieddatetime  DATETIME       NULL,
        CONSTRAINT CK_XrfBatchDetail_result
            CHECK (xrfbd_result IS NULL OR xrfbd_result IN ('Complete', 'Error', 'Inaccessible', 'Dirty/Wet', 'XRF Not Used'))
    );

    CREATE INDEX IX_XrfBatchDetail_xrfb_id       ON dbo.XrfBatchDetail (xrfb_id);
    CREATE INDEX IX_XrfBatchDetail_hvbd_id       ON dbo.XrfBatchDetail (hvbd_id);
    CREATE INDEX IX_XrfBatchDetail_premisenumber ON dbo.XrfBatchDetail (xrfbd_premisenumber);
    CREATE INDEX IX_XrfBatchDetail_completed     ON dbo.XrfBatchDetail (xrfbd_completeddatetime) INCLUDE (xrfbd_team);
END;
GO
