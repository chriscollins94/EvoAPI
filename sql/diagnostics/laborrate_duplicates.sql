-- Lists LaborRate rows that share a customer/trade (xccc_id + t_id), with the service
-- requests that would hit the TimeQuoted rate lookup for each pair. Read-only.
-- Pairs with migrations/2026-09-16_add_laborrate_unique_customer_trade.sql.

-- 1. Duplicate rate rows, newest lr_id first within each pair (the code keeps the newest).
SELECT lr.xccc_id, c.c_name, t.t_trade, lr.lr_id,
       lr.lr_rateregular, lr.lr_rateovertime, lr.lr_flatorhourly, lr.lr_markup,
       lr.lr_descriptionoverride, lr.lr_note,
       (SELECT COUNT(*) FROM dbo.xrefLaborRateCheckList x WHERE x.lr_id = lr.lr_id) AS checklist_links
FROM dbo.LaborRate lr
JOIN (SELECT xccc_id, t_id FROM dbo.LaborRate GROUP BY xccc_id, t_id HAVING COUNT(*) > 1) d
  ON d.xccc_id = lr.xccc_id AND d.t_id = lr.t_id
LEFT JOIN dbo.xrefCompanyCallCenter xccc ON xccc.xccc_id = lr.xccc_id
LEFT JOIN dbo.Company c ON c.c_id = xccc.c_id
LEFT JOIN dbo.Trade t ON t.t_id = lr.t_id
ORDER BY lr.xccc_id, lr.t_id, lr.lr_id DESC;

-- 2. Open service requests affected by each duplicate pair.
SELECT sr.sr_id, sr.xccc_id, sr.t_id
FROM dbo.ServiceRequest sr
JOIN (SELECT xccc_id, t_id FROM dbo.LaborRate GROUP BY xccc_id, t_id HAVING COUNT(*) > 1) d
  ON d.xccc_id = sr.xccc_id AND d.t_id = sr.t_id
ORDER BY sr.sr_id DESC;

-- 3. Delete template (fill in the lr_id values to remove, run inside a transaction):
-- BEGIN TRAN;
-- DELETE FROM dbo.xrefLaborRateCheckList WHERE lr_id IN (/* lr_ids to drop */);
-- DELETE FROM dbo.LaborRate WHERE lr_id IN (/* lr_ids to drop */);
-- COMMIT;
