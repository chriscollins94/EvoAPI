-- Lookback window (days) for the job-mix pie charts on the Performance report
-- (% of jobs by Parent Trade / SubTrade / Call Center). Editable from the
-- Metric Targets section of the admin Performance page.

IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'PerformanceTarget' AND cs_identifier = 'Perf.JobMixLookbackDays')
BEGIN
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_description)
    VALUES (1, 'PerformanceTarget', 'Perf.JobMixLookbackDays', '365', 'Job mix pie charts: include service requests created in the last N days');
END;
GO
