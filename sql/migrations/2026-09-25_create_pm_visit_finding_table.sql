-- =====================================================================================================
-- Preventative Maintenance build slice 4b: checkout (findings, proposal ticket, materials, signatures, submit)
-- Plan: Temp\PM\PM Workflow - POC Plan.md section 8d (2026-09-25); design: technical-design\data-model.html
--
-- Adds, guarded so the script can be re-run:
--   1. PMVisitFinding: one deficiency / recommendation the tech records on a visit (dictionary F182-F191, P25), per unit or
--      site-wide. Findings flagged "quote required" are rolled into one proposal service request at submit; sr_id_proposal
--      links each finding to it (the dictionary's "deficiency ID is just the quote WO#").
--   2. Nothing else: PMVisit already carries the checkout / submit columns (slice 3) and PMVisitAnswer the signatures (4a).
--      Materials use the existing xrefWorkOrderServiceItem rows (with as_id) written through the legacy service.
--
-- Deploy together with EvoAPI (PmVisitController) and evotech, behind the PreventativeMaintenance feature flag.
-- =====================================================================================================

SET NOCOUNT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PMVisitFinding')
BEGIN
    CREATE TABLE dbo.PMVisitFinding (
        pmvf_id                     INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        pmvf_insertdatetime         DATETIME       NOT NULL CONSTRAINT DF_PMVisitFinding_insertdatetime DEFAULT (GETDATE()),
        pmvf_modifieddatetime       DATETIME       NULL,
        pmv_id                      INT            NOT NULL REFERENCES dbo.PMVisit (pmv_id) ON DELETE CASCADE,
        pmvu_id                     INT            NULL REFERENCES dbo.PMVisitUnit (pmvu_id),   -- NULL = site-wide finding
        pmvf_component              NVARCHAR(120)  NULL,     -- F182 related unit / component
        pmvf_description            NVARCHAR(MAX)  NULL,     -- F183
        pmvf_severity               NVARCHAR(30)   NULL,     -- F184 Severity list
        pmvf_impact                 NVARCHAR(40)   NULL,     -- F185
        pmvf_risk                   NVARCHAR(20)   NULL,     -- F186
        pmvf_action                 NVARCHAR(40)   NULL,     -- F187
        pmvf_parts                  NVARCHAR(400)  NULL,     -- F188
        pmvf_laborestimate          NVARCHAR(200)  NULL,     -- F189
        pmvf_quoterequired          BIT            NOT NULL CONSTRAINT DF_PMVisitFinding_quoterequired DEFAULT (0),   -- F190
        pmvf_callcentercontacted    NVARCHAR(10)   NULL,     -- F191 Yes / No / N/A
        pmvf_contactdetails         NVARCHAR(400)  NULL,
        att_id                      INT            NULL,     -- P25 photo
        sr_id_proposal              INT            NULL REFERENCES dbo.ServiceRequest (sr_id),   -- the proposal ticket created at submit
        u_id                        INT            NULL
    );
    CREATE INDEX IX_PMVisitFinding_Visit ON dbo.PMVisitFinding (pmv_id) INCLUDE (pmvu_id, pmvf_quoterequired, sr_id_proposal);
END;
GO

PRINT 'PMVisitFinding ready.';
GO
