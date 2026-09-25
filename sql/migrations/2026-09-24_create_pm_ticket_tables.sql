-- =====================================================================================================
-- Preventative Maintenance build slice 3: ticket entry (New Service Request, Preventative path)
-- Plan: Temp\PM\PM Workflow - POC Plan.md section 8c (2026-09-24); design: technical-design\data-model.html
--
-- Adds, all guarded so the script can be re-run:
--   1. ServiceType lookup (Reactionary / Preventative / Proposal-Quote / Administrative) for the new Service Type tiles.
--   2. Nullable columns on ServiceRequest: svt_id (NULL = Reactionary, nothing is backfilled), fr_id (the form rule in
--      effect when the ticket was created), sr_pmunitcount (units to service), sr_servicebydate (customer's
--      service-by date). sr_servicebydate is a NEW column on purpose: sr_datedue is the QuickBooks invoice due date
--      (Evo.DataLayer EvoData.cs GetInvoice copies it into invoice.DueDate), so it cannot double as a service-by date.
--   3. Priority.p_allowpreventative: which priorities a Preventative ticket may use. Set for the three Scheduled
--      priorities by name; editable on the priorities admin page.
--   4. PMVisit: one row per Preventative service request. Created at ticket entry with the rule / template / season
--      pinned and the customer's PM parameters and pricing snapshotted; the tech visit (slice 4) fills the rest.
--      All visit-side columns exist now so slice 4 needs no ALTER.
--   5. PMVisitUnit: the units the ticket was priced for (from the unit builder), later confirmed / added by the tech.
--
-- Nothing here is dropped or altered destructively. Legacy EvoWS/EvoUI inserts list their columns explicitly, so the
-- new nullable ServiceRequest columns do not affect them. sr_flatorhourly / sr_rateflat are existing columns: a
-- Contract-priced PM ticket is stored as 'flat' with the contract total in sr_rateflat, which the legacy invoice
-- already turns into a "Flat Rate Applied" line.
-- Deploy together with EvoAPI (PmTicketController, SR create changes) and evotech (New Service Request wizard,
-- priorities admin), behind the PreventativeMaintenance feature flag.
-- =====================================================================================================

SET NOCOUNT ON;
GO

------------------------------------------------------------------------------------------------------
-- 1. ServiceType lookup
------------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ServiceType')
BEGIN
    CREATE TABLE dbo.ServiceType (
        svt_id                INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        svt_insertdatetime    DATETIME      NOT NULL CONSTRAINT DF_ServiceType_insertdatetime DEFAULT (GETDATE()),
        svt_modifieddatetime  DATETIME      NULL,
        svt_servicetype       NVARCHAR(40)  NOT NULL,   -- Reactionary | Preventative | Proposal/Quote | Administrative
        svt_code              VARCHAR(20)   NOT NULL,   -- stable key for code: Reactionary | Preventative | Proposal | Administrative
        svt_description       NVARCHAR(200) NULL,
        svt_order             INT           NOT NULL CONSTRAINT DF_ServiceType_order DEFAULT (0),
        svt_active            BIT           NOT NULL CONSTRAINT DF_ServiceType_active DEFAULT (1),
        CONSTRAINT UQ_ServiceType_Code UNIQUE (svt_code)
    );
END;
GO

MERGE dbo.ServiceType AS tgt
USING (VALUES
    (N'Reactionary Service', 'Reactionary',    N'Today''s flow: repair / service call',              1),
    (N'Preventative Service', 'Preventative',  N'PM workflow: template-driven visit, PM pricing',    2),
    (N'Proposal / Quote',     'Proposal',      N'Quote or proposal ticket',                          3),
    (N'Administrative',       'Administrative', N'Administrative ticket',                            4)) AS src (name, code, descr, ord)
ON tgt.svt_code = src.code
WHEN NOT MATCHED THEN INSERT (svt_servicetype, svt_code, svt_description, svt_order) VALUES (src.name, src.code, src.descr, src.ord);
GO

------------------------------------------------------------------------------------------------------
-- 2. ServiceRequest columns (all nullable; NULL svt_id reads as Reactionary)
------------------------------------------------------------------------------------------------------
IF COL_LENGTH('dbo.ServiceRequest', 'svt_id') IS NULL
    ALTER TABLE dbo.ServiceRequest ADD svt_id INT NULL CONSTRAINT FK_ServiceRequest_ServiceType REFERENCES dbo.ServiceType (svt_id);
IF COL_LENGTH('dbo.ServiceRequest', 'fr_id') IS NULL
    ALTER TABLE dbo.ServiceRequest ADD fr_id INT NULL CONSTRAINT FK_ServiceRequest_FormRule REFERENCES dbo.FormRule (fr_id);   -- rule in effect at creation
IF COL_LENGTH('dbo.ServiceRequest', 'sr_pmunitcount') IS NULL
    ALTER TABLE dbo.ServiceRequest ADD sr_pmunitcount INT NULL;                                                             -- DICT F028 "# of units"
IF COL_LENGTH('dbo.ServiceRequest', 'sr_servicebydate') IS NULL
    ALTER TABLE dbo.ServiceRequest ADD sr_servicebydate DATE NULL;                                                          -- DOC section 3 "service by date"
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ServiceRequest_ServiceType' AND object_id = OBJECT_ID('dbo.ServiceRequest'))
    CREATE INDEX IX_ServiceRequest_ServiceType ON dbo.ServiceRequest (svt_id) INCLUDE (l_id, t_id);
GO

------------------------------------------------------------------------------------------------------
-- 3. Priority.p_allowpreventative
------------------------------------------------------------------------------------------------------
IF COL_LENGTH('dbo.Priority', 'p_allowpreventative') IS NULL
    ALTER TABLE dbo.Priority ADD p_allowpreventative BIT NOT NULL CONSTRAINT DF_Priority_allowpreventative DEFAULT (0);
GO

-- The three "Scheduled ..." priorities (green, a week or more out) are the only ones a PM may use (DOC section 3).
-- Only sets the flag where nothing is flagged yet, so an admin's later edits survive a re-run.
IF NOT EXISTS (SELECT 1 FROM dbo.Priority WHERE p_allowpreventative = 1)
BEGIN
    UPDATE dbo.Priority SET p_allowpreventative = 1, p_modifieddatetime = GETDATE()
    WHERE p_priority IN ('Scheduled Next Week', 'Scheduled This Month', 'Scheduled Next Month');
    PRINT 'Priorities allowed for Preventative: ' + CAST(@@ROWCOUNT AS VARCHAR(10)) + ' (expected 3: Scheduled Next Week / This Month / Next Month).';
END;
GO

------------------------------------------------------------------------------------------------------
-- 4. PMVisit: one per Preventative service request
------------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PMVisit')
BEGIN
    CREATE TABLE dbo.PMVisit (
        pmv_id                          INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        pmv_insertdatetime              DATETIME       NOT NULL CONSTRAINT DF_PMVisit_insertdatetime DEFAULT (GETDATE()),
        pmv_modifieddatetime            DATETIME       NULL,
        sr_id                           INT            NOT NULL REFERENCES dbo.ServiceRequest (sr_id),
        u_id_createdby                  INT            NULL,

        -- pinned for the life of the visit
        fr_id                           INT            NULL REFERENCES dbo.FormRule (fr_id),
        ft_id                           INT            NULL REFERENCES dbo.FormTemplate (ft_id),
        ft_version                      INT            NULL,
        pms_id                          INT            NULL REFERENCES dbo.PMSeason (pms_id),
        pmv_visittype                   NVARCHAR(20)   NULL,   -- Spring Cooling | Fall Heating | Full PM | Inspection Only | Filter Change (copied from the season or derived from the sub-trade)
        pmv_firsttime                   BIT            NOT NULL CONSTRAINT DF_PMVisit_firsttime DEFAULT (0),   -- "First Time PM ONLY Rules": no prior visit / PM ticket at this location + parent trade
        pmv_firsttimesource             VARCHAR(10)    NULL,   -- Auto | Office (office overrode the detection)

        -- billing snapshot (ticket entry)
        pmv_billingmode                 NVARCHAR(30)   NULL,   -- SetNte | ContractTiered | TimeAndMaterials
        pmv_nteguideline                NVARCHAR(20)   NULL,
        pmv_firsttimerule               NVARCHAR(30)   NULL,
        pmv_contracttotal               DECIMAL(9,2)   NULL,   -- computed (or overridden) contract total; also in sr_rateflat
        pmv_pricingjson                 NVARCHAR(MAX)  NULL,   -- line breakdown behind the total (units, add-ons, trip charge, override)

        -- customer PM parameters snapshot (copied from PMRule so later rule edits do not alter this ticket)
        pmv_coolingsetpoint             TINYINT        NULL,
        pmv_heatingsetpoint             TINYINT        NULL,
        pmv_thermostatschedule          NVARCHAR(20)   NULL,
        pmv_thermostatschedulenote      NVARCHAR(200)  NULL,
        pmv_thermostatlock              BIT            NULL,
        pmv_antialgae                   BIT            NULL,
        pmv_phototimestamp              BIT            NULL,
        pmv_managerseesoldfilters       BIT            NULL,
        pmv_pricingdiscussallowed       BIT            NULL,
        pmv_immediatequoterequired      BIT            NULL,
        pmv_immediatecallifincomplete   BIT            NULL,
        pmv_submissiondeadline          NVARCHAR(20)   NULL,
        pmv_submissiondeadlinenote      NVARCHAR(200)  NULL,
        pmv_closeoutdocs                NVARCHAR(100)  NULL,   -- CSV: Invoice, Signoff, Checklist, Photos, Quote, SiteStamp
        pmv_submissiondestination       NVARCHAR(20)   NULL,
        pmv_ivrrequired                 BIT            NULL,
        pmv_customerform                NVARCHAR(20)   NULL,   -- None | CustomerPdfForm | ExternalLink | Portal
        pmv_customerformurl             NVARCHAR(500)  NULL,
        att_id_customerform             INT            NULL,
        pmv_customerformnote            NVARCHAR(500)  NULL,

        -- office confirmation at ticket entry
        pmv_paramsconfirmed_u_id        INT            NULL,
        pmv_paramsconfirmeddatetime     DATETIME       NULL,

        -- tech capture (slice 4): site level
        pmv_weather                     NVARCHAR(100)  NULL,
        pmv_outdoortempf                DECIMAL(5,1)   NULL,
        pmv_weathersource               NVARCHAR(40)   NULL,
        pmv_weatherdatetime             DATETIME       NULL,
        pmv_indoortempf                 DECIMAL(5,1)   NULL,   -- F101
        pmv_managername                 NVARCHAR(100)  NULL,   -- F023
        pmv_managernotified             NVARCHAR(10)   NULL,   -- F024
        pmv_ivrcheckin                  NVARCHAR(40)   NULL,   -- F005
        pmv_safetyissues                NVARCHAR(200)  NULL,   -- CSV
        pmv_safetynote                  NVARCHAR(1000) NULL,
        pmv_reschedulerequired          BIT            NULL,

        -- completion (slice 4)
        pmv_status                      VARCHAR(15)    NOT NULL CONSTRAINT DF_PMVisit_status DEFAULT ('NotStarted'),   -- NotStarted | InProgress | Incomplete | Submitted | QCApproved | Sent
        pmv_fullscopecompleted          BIT            NULL,   -- F030
        pmv_incompletereason            NVARCHAR(1000) NULL,   -- F031
        pmv_callcenternotified          NVARCHAR(10)   NULL,   -- F032
        pmv_notificationdetails         NVARCHAR(500)  NULL,   -- F033
        pmv_customercontacted           BIT            NULL,
        pmv_customerrep                 NVARCHAR(100)  NULL,   -- F201
        pmv_customerformcompleted       BIT            NULL,
        pmv_ivrcheckout                 NVARCHAR(40)   NULL,   -- F006
        pmv_submitteddatetime           DATETIME       NULL,
        u_id_submitted                  INT            NULL,

        -- QC and transmission (slice 5)
        pmv_qc_u_id                     INT            NULL,
        pmv_qcdatetime                  DATETIME       NULL,
        pmv_qcnote                      NVARCHAR(2000) NULL,
        pmv_sentdatetime                DATETIME       NULL,
        pmv_sentreference               NVARCHAR(100)  NULL,   -- F212

        CONSTRAINT UQ_PMVisit_ServiceRequest UNIQUE (sr_id)
    );
END;
GO

------------------------------------------------------------------------------------------------------
-- 5. PMVisitUnit: the units on the ticket (priced at entry) and worked on the visit
------------------------------------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PMVisitUnit')
BEGIN
    CREATE TABLE dbo.PMVisitUnit (
        pmvu_id                     INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        pmvu_insertdatetime         DATETIME       NOT NULL CONSTRAINT DF_PMVisitUnit_insertdatetime DEFAULT (GETDATE()),
        pmvu_modifieddatetime       DATETIME       NULL,
        pmv_id                      INT            NOT NULL REFERENCES dbo.PMVisit (pmv_id) ON DELETE CASCADE,
        pmvu_sequence               TINYINT        NOT NULL CONSTRAINT DF_PMVisitUnit_sequence DEFAULT (1),   -- unit # on the ticket / tab order
        pmvu_source                 VARCHAR(10)    NOT NULL CONSTRAINT DF_PMVisitUnit_source DEFAULT ('Ticket'),   -- Ticket | Tech
        as_id                       INT            NULL REFERENCES dbo.Asset (as_id),           -- existing asset picked at entry; NULL = typed (type + tonnage only) until the tech captures it
        asc_id                      INT            NULL REFERENCES dbo.AssetCategory (asc_id),  -- equipment type used for pricing
        pmvu_capacitytons           DECIMAL(6,2)   NULL,                                        -- tonnage used for pricing
        pmvu_label                  NVARCHAR(120)  NULL,                                        -- "RTU-1 · Carrier 48TCED08 (7.5 t)" or "Unit 3 · RTU ≤10 t"
        pmvu_price                  DECIMAL(9,2)   NULL,                                        -- tier price applied to this unit
        pmvu_tierlabel              NVARCHAR(80)   NULL,                                        -- which tier row priced it
        -- tech visit (slice 4)
        pmvu_status                 VARCHAR(12)    NOT NULL CONSTRAINT DF_PMVisitUnit_status DEFAULT ('Pending'),   -- Pending | Serviced | NotServiced
        pmvu_notservicedreason      NVARCHAR(200)  NULL,
        pmvu_assetconfirmed         BIT            NOT NULL CONSTRAINT DF_PMVisitUnit_assetconfirmed DEFAULT (0),
        pmvu_operationalatarrival   NVARCHAR(25)   NULL,   -- F069
        pmvu_operationalatdeparture NVARCHAR(25)   NULL,   -- F070
        pmvu_condition              NVARCHAR(25)   NULL,   -- F067
        pmvu_recommendation         NVARCHAR(25)   NULL,   -- F068
        pmvu_note                   NVARCHAR(2000) NULL,
        pmvu_completeddatetime      DATETIME       NULL
    );
    CREATE INDEX IX_PMVisitUnit_Visit ON dbo.PMVisitUnit (pmv_id, pmvu_sequence);
END;
GO

PRINT 'PM ticket tables ready: ServiceType, ServiceRequest.svt_id/fr_id/sr_pmunitcount/sr_servicebydate, Priority.p_allowpreventative, PMVisit, PMVisitUnit.';
GO
