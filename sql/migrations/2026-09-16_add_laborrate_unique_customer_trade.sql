-- Enforces one LaborRate row per customer/trade (xccc_id + t_id).
--
-- Why: InsertTimeQuoted (Evo.DataLayer/EvoData.cs) looks up the regular rate with a scalar
-- subquery on xccc_id + t_id. Duplicate LaborRate rows made that subquery return more than
-- one value (SQL error 512) and the Schedule page's Time Quoted "Add Manual Entry" button
-- returned a 500. The code now takes TOP 1 and InsertLaborRate refuses a second row, but the
-- unique index is what stops duplicates from creeping back in through any other path.
--
-- Ships with EvoWS + EvoUI (InsertLaborRate returns 409 on a duplicate; company.js shows a
-- message). Run on TEST first, then PROD with that deploy.
--
-- Duplicates must be resolved BEFORE the index can be created. The script aborts and lists
-- them if any exist. Review them with diagnostics/laborrate_duplicates.sql, decide which row
-- to keep per customer/trade, then delete the rest (and their xrefLaborRateCheckList rows)
-- and rerun. Idempotent once the index exists.

SET NOCOUNT ON;

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_LaborRate_xccc_id_t_id' AND object_id = OBJECT_ID('dbo.LaborRate'))
BEGIN
    PRINT 'UX_LaborRate_xccc_id_t_id already exists - nothing to do.';
    RETURN;
END

IF EXISTS (SELECT 1 FROM dbo.LaborRate GROUP BY xccc_id, t_id HAVING COUNT(*) > 1)
BEGIN
    SELECT lr.xccc_id, lr.t_id, lr.lr_id, lr.lr_rateregular, lr.lr_flatorhourly, lr.lr_descriptionoverride, lr.lr_note
    FROM dbo.LaborRate lr
    JOIN (SELECT xccc_id, t_id FROM dbo.LaborRate GROUP BY xccc_id, t_id HAVING COUNT(*) > 1) d
      ON d.xccc_id = lr.xccc_id AND d.t_id = lr.t_id
    ORDER BY lr.xccc_id, lr.t_id, lr.lr_id;

    RAISERROR ('LaborRate has duplicate customer/trade rows (listed above). Resolve them, then rerun this script.', 16, 1);
    RETURN;
END

CREATE UNIQUE INDEX UX_LaborRate_xccc_id_t_id ON dbo.LaborRate (xccc_id, t_id);
PRINT 'Created UX_LaborRate_xccc_id_t_id.';
GO
