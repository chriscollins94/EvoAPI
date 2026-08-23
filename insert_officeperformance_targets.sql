-- =============================================================================
-- Office Performance dashboard: metric targets
-- Seeds ConfigSetting rows (cs_type = 'PerformanceTarget', identifiers prefixed
-- 'OfficePerf.') read by the Office Performance dashboard and edited from
-- Office > Settings > Performance > Office tab.
--
-- Every office metric is LOWER IS BETTER; direction lives in the frontend metric
-- definitions (officePerformanceMetrics.js), not here. All defaults start at 10
-- and are expected to be tuned by an admin once real data is visible.
--
-- Open Now / Opened YTD / Invoiced YTD are deliberately untargeted (flat data).
--
-- Status group keys are StatusSecondary.ss_code values ('Complete-Other' = every
-- Complete-family status except Complete-4 Ready to Invoice).
--
-- Run on TEST first, then PROD with the evotech deploy. Safe to re-run — each
-- row inserts only if its identifier does not already exist.
-- =============================================================================

INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_description)
SELECT 1, 'PerformanceTarget', v.identifier, v.value, v.description
FROM (VALUES
    -- Workload & throughput
    ('OfficePerf.AvgDaysToInvoice',            '10', 'Office target: Avg days from created to first invoiced (lower is better)'),

    -- Time in secondary status (days, YTD average of completed stays)
    ('OfficePerf.Dur.Incomplete-2',            '10', 'Office target: Avg days in Needs to be Quoted (lower is better)'),
    ('OfficePerf.Dur.Incomplete-20',           '10', 'Office target: Avg days in Needs Quote Written (lower is better)'),
    ('OfficePerf.Dur.Incomplete-9',            '10', 'Office target: Avg days in Need to Order Parts (lower is better)'),
    ('OfficePerf.Dur.Complete-4',              '10', 'Office target: Avg days in Complete - Ready to Invoice (lower is better)'),

    -- Customer inquiry analysis: times the status was entered YTD
    ('OfficePerf.CiEntries.Incomplete-10',     '10', 'Office target: Times entered Needs to Be Scheduled YTD (lower is better)'),
    ('OfficePerf.CiEntries.Incomplete-12',     '10', 'Office target: Times entered Pending Tech Info YTD (lower is better)'),
    ('OfficePerf.CiEntries.Incomplete-17',     '10', 'Office target: Times entered Pending Tech Info - Basic YTD (lower is better)'),
    ('OfficePerf.CiEntries.Incomplete-20',     '10', 'Office target: Times entered Needs Quote Written YTD (lower is better)'),
    ('OfficePerf.CiEntries.Incomplete-9',      '10', 'Office target: Times entered Need to Order Parts YTD (lower is better)'),
    ('OfficePerf.CiEntries.Incomplete-5',      '10', 'Office target: Times entered Parts Ordered YTD (lower is better)'),
    ('OfficePerf.CiEntries.Complete-4',        '10', 'Office target: Times entered Complete - Ready to Invoice YTD (lower is better)'),
    ('OfficePerf.CiEntries.Complete-Other',    '10', 'Office target: Times entered other Complete statuses YTD (lower is better)'),

    -- Customer inquiry analysis: average hours in the status (completed stays YTD)
    ('OfficePerf.CiHours.Incomplete-10',       '10', 'Office target: Avg hours in Needs to Be Scheduled (lower is better)'),
    ('OfficePerf.CiHours.Incomplete-12',       '10', 'Office target: Avg hours in Pending Tech Info (lower is better)'),
    ('OfficePerf.CiHours.Incomplete-17',       '10', 'Office target: Avg hours in Pending Tech Info - Basic (lower is better)'),
    ('OfficePerf.CiHours.Incomplete-20',       '10', 'Office target: Avg hours in Needs Quote Written (lower is better)'),
    ('OfficePerf.CiHours.Incomplete-9',        '10', 'Office target: Avg hours in Need to Order Parts (lower is better)'),
    ('OfficePerf.CiHours.Incomplete-5',        '10', 'Office target: Avg hours in Parts Ordered (lower is better)'),
    ('OfficePerf.CiHours.Complete-4',          '10', 'Office target: Avg hours in Complete - Ready to Invoice (lower is better)'),
    ('OfficePerf.CiHours.Complete-Other',      '10', 'Office target: Avg hours in other Complete statuses (lower is better)'),

    -- Customer inquiry analysis: average hours in the status when the customer inquired
    ('OfficePerf.CiInqHours.Incomplete-10',    '10', 'Office target: Avg hours in Needs to Be Scheduled at inquiry (lower is better)'),
    ('OfficePerf.CiInqHours.Incomplete-12',    '10', 'Office target: Avg hours in Pending Tech Info at inquiry (lower is better)'),
    ('OfficePerf.CiInqHours.Incomplete-17',    '10', 'Office target: Avg hours in Pending Tech Info - Basic at inquiry (lower is better)'),
    ('OfficePerf.CiInqHours.Incomplete-20',    '10', 'Office target: Avg hours in Needs Quote Written at inquiry (lower is better)'),
    ('OfficePerf.CiInqHours.Incomplete-9',     '10', 'Office target: Avg hours in Need to Order Parts at inquiry (lower is better)'),
    ('OfficePerf.CiInqHours.Incomplete-5',     '10', 'Office target: Avg hours in Parts Ordered at inquiry (lower is better)'),
    ('OfficePerf.CiInqHours.Complete-4',       '10', 'Office target: Avg hours in Complete - Ready to Invoice at inquiry (lower is better)'),
    ('OfficePerf.CiInqHours.Complete-Other',   '10', 'Office target: Avg hours in other Complete statuses at inquiry (lower is better)')
) v(identifier, value, description)
WHERE NOT EXISTS (
    SELECT 1 FROM ConfigSetting cs
    WHERE cs.cs_type = 'PerformanceTarget' AND cs.cs_identifier = v.identifier
);
GO

-- Quick verification
SELECT cs_identifier, cs_value, cs_description
FROM ConfigSetting
WHERE cs_type = 'PerformanceTarget' AND cs_identifier LIKE 'OfficePerf.%'
ORDER BY cs_identifier;
GO
