-- One-time cleanup for a TEST database that was seeded with the first (PM-named) version of the
-- slice-1 tables on 2026-09-06, before the rename to the Form* engine. Production never had them.
-- Drops children first so the foreign keys do not block. Safe to re-run; skips tables that are gone.

IF OBJECT_ID('dbo.xrefPMCustomerRuleQuestion', 'U') IS NOT NULL DROP TABLE dbo.xrefPMCustomerRuleQuestion;
IF OBJECT_ID('dbo.PMSeason', 'U') IS NOT NULL AND COL_LENGTH('dbo.PMSeason', 'pmcr_id') IS NOT NULL DROP TABLE dbo.PMSeason;
IF OBJECT_ID('dbo.PMRateTier', 'U') IS NOT NULL AND COL_LENGTH('dbo.PMRateTier', 'pmcr_id') IS NOT NULL DROP TABLE dbo.PMRateTier;
IF OBJECT_ID('dbo.PMCustomerRule', 'U') IS NOT NULL DROP TABLE dbo.PMCustomerRule;
IF OBJECT_ID('dbo.PMTemplateQuestion', 'U') IS NOT NULL DROP TABLE dbo.PMTemplateQuestion;
IF OBJECT_ID('dbo.PMTemplateSection', 'U') IS NOT NULL DROP TABLE dbo.PMTemplateSection;
IF OBJECT_ID('dbo.PMTemplate', 'U') IS NOT NULL DROP TABLE dbo.PMTemplate;
IF OBJECT_ID('dbo.PMAnswerList', 'U') IS NOT NULL DROP TABLE dbo.PMAnswerList;
GO
PRINT 'PM slice-1 tables dropped (PMBillingMode kept; PMRateTier / PMSeason recreated by create_form_tables.sql)';
GO
