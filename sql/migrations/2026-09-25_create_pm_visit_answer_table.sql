-- =====================================================================================================
-- Preventative Maintenance build slice 4a: the technician's PM visit (Site tab, Assets tab, per-unit questions)
-- Plan: Temp\PM\PM Workflow - POC Plan.md section 8d (2026-09-25); design: technical-design\data-model.html
--
-- Adds, guarded so the script can be re-run:
--   1. PMVisitAnswer: one row per answer per unit per repeat (circuit / compressor / stage / phase). Site- and
--      checkout-level answers have no unit. Modelled on xrefServiceRequestCheckListAnswer: the question text is
--      snapshotted, a photo or signature is an Attachment referenced by att_id, and the answer's own GPS (when the
--      photo carried one) is kept beside it. Upserted per answer, save-as-you-go.
--   2. Nothing else: PMVisit and PMVisitUnit (slice 3) already carry every visit-side column this slice writes.
--
-- Deploy together with EvoAPI (PmVisitController) and evotech (/tech/pm pages) plus the one-line routing change in the
-- legacy EvoUI tech schedule, behind the PreventativeMaintenance feature flag.
-- =====================================================================================================

SET NOCOUNT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PMVisitAnswer')
BEGIN
    CREATE TABLE dbo.PMVisitAnswer (
        pmva_id                 INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        pmva_insertdatetime     DATETIME       NOT NULL CONSTRAINT DF_PMVisitAnswer_insertdatetime DEFAULT (GETDATE()),
        pmva_modifieddatetime   DATETIME       NULL,
        pmv_id                  INT            NOT NULL REFERENCES dbo.PMVisit (pmv_id) ON DELETE CASCADE,
        pmvu_id                 INT            NULL REFERENCES dbo.PMVisitUnit (pmvu_id),   -- NULL = site / checkout level
        fq_id                   INT            NOT NULL REFERENCES dbo.FormQuestion (fq_id),
        pmva_code               NVARCHAR(12)   NULL,                                         -- copy of the question code (F104, T044, P16) for exports / formulas
        pmva_repeatindex        TINYINT        NOT NULL CONSTRAINT DF_PMVisitAnswer_repeatindex DEFAULT (0),   -- circuit / compressor / stage / phase number, 0 = not repeating
        pmva_question           NVARCHAR(400)  NULL,                                         -- text snapshot, as xsrcla_question
        pmva_answer             NVARCHAR(MAX)  NULL,
        pmva_numeric            DECIMAL(12,3)  NULL,                                         -- typed copy for readings and calculated fields
        att_id                  INT            NULL,                                         -- photo / signature (Attachment)
        pmva_latitude           DECIMAL(9,6)   NULL,
        pmva_longitude          DECIMAL(9,6)   NULL,
        u_id                    INT            NULL,                                         -- who answered
        CONSTRAINT UQ_PMVisitAnswer_Question UNIQUE (pmv_id, pmvu_id, fq_id, pmva_repeatindex)
    );
    CREATE INDEX IX_PMVisitAnswer_Visit ON dbo.PMVisitAnswer (pmv_id) INCLUDE (pmvu_id, fq_id, pmva_repeatindex);
END;
GO

PRINT 'PMVisitAnswer ready.';
GO
