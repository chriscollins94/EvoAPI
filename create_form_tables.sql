-- Forms engine (build slice 1 of the Preventative Maintenance workflow, generalised so other
-- checklists can move onto it later).
--
-- Two layers:
--   Generic form engine
--     FormAnswerList        named, shared answer lists (Task Result, Condition, Yes/No/NA ...)
--     FormTemplate          one question bank per parent trade, versioned (clone to a new version)
--     FormSection           tabs of a template; phase = Site | Asset | Unit | Findings | Checkout
--     FormQuestion          one row per question / task / photo; fq_code is the stable key (dictionary IDs)
--     FormRule              which template a company pairing (xccc_id) + parent trade uses
--     xrefFormRuleQuestion  per-rule question overrides (only rows that differ from the template default)
--   PM-specific business terms, one row per FormRule
--     PMBillingMode         lookup: SetNte | ContractTiered | TimeAndMaterials
--     PMRule                billing, inclusions, customer-required form, PM parameters, contacts
--     PMRateTier            quarterly price matrix rows and add-on lines
--     PMSeason              seasons / visit types; t_id_season is the PM sub-trade chosen at ticket entry
--
-- Definitions come from Temp\PM\technical-design\model.js (renamed PM* -> Form* on 2026-09-06 so the
-- engine is not tied to PM; the naming map is in Temp\PM\technical-design\SEED_DECISIONS.md section G).
--
-- Run order: this script, then the generated seeds under sql\forms\
--   (seed_form_template_hvac.sql, seed_asset_categories_hvac.sql, seed_form_rules_pm_hvac.sql).
-- A test DB seeded before the rename must run sql\forms\drop_pm_slice1_tables.sql first.
-- Run on TEST first. PROD deploy ships this script + the seeds + EvoAPI + evotech together.
--
-- Feature flag: inserted OFF ('0'). Turn it on per environment with
--   UPDATE ConfigSetting SET cs_value = '1' WHERE cs_type = 'featureflag' AND cs_identifier = 'PreventativeMaintenance';
--
-- Answer types: the engine reuses the existing CheckListAnswerType rows (Textbox, Drop Down,
-- Signature, Photo, Readonly). Numeric and calculated questions are Textbox / Readonly rows refined
-- by fq_datatype and fq_calcformula - no rows are added to CheckListAnswerType because the legacy and
-- evotech checklist editors list that table unfiltered and the legacy tech UI cannot render new types.

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FormAnswerList')
BEGIN
    CREATE TABLE dbo.FormAnswerList (
        fal_id               INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        fal_insertdatetime   DATETIME      NOT NULL CONSTRAINT DF_FormAnswerList_insertdatetime DEFAULT (GETDATE()),
        fal_modifieddatetime DATETIME      NULL,
        fal_name             NVARCHAR(60)  NOT NULL,   -- Dropdown Lists sheet "List Name" or a generated name
        fal_values           NVARCHAR(MAX) NOT NULL,   -- semicolon separated, in display order
        fal_failvalues       NVARCHAR(MAX) NULL,       -- subset that counts as an issue (drives If-issue photos / findings prompt)
        fal_active           BIT           NOT NULL CONSTRAINT DF_FormAnswerList_active DEFAULT (1),
        CONSTRAINT UQ_FormAnswerList_Name UNIQUE (fal_name)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FormTemplate')
BEGIN
    CREATE TABLE dbo.FormTemplate (
        ft_id               INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        ft_insertdatetime   DATETIME      NOT NULL CONSTRAINT DF_FormTemplate_insertdatetime DEFAULT (GETDATE()),
        ft_modifieddatetime DATETIME      NULL,
        t_id                INT           NOT NULL REFERENCES dbo.Trade(t_id),   -- parent trade (HVAC, Electrical ...)
        ft_name             NVARCHAR(100) NOT NULL,
        ft_version          INT           NOT NULL CONSTRAINT DF_FormTemplate_version DEFAULT (1),
        ft_active           BIT           NOT NULL CONSTRAINT DF_FormTemplate_active DEFAULT (1),   -- one active version per trade (enforced by the API)
        ft_note             NVARCHAR(MAX) NULL,
        CONSTRAINT UQ_FormTemplate_Trade_Version UNIQUE (t_id, ft_version)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FormSection')
BEGIN
    CREATE TABLE dbo.FormSection (
        fs_id               INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        fs_insertdatetime   DATETIME      NOT NULL CONSTRAINT DF_FormSection_insertdatetime DEFAULT (GETDATE()),
        fs_modifieddatetime DATETIME      NULL,
        ft_id               INT           NOT NULL REFERENCES dbo.FormTemplate(ft_id),
        fs_name             NVARCHAR(100) NOT NULL,
        fs_phase            NVARCHAR(20)  NOT NULL,   -- Site | Asset | Unit | Findings | Checkout
        fs_order            INT           NOT NULL CONSTRAINT DF_FormSection_order DEFAULT (0),
        fs_repeatperunit    BIT           NOT NULL CONSTRAINT DF_FormSection_repeatperunit DEFAULT (0),
        fs_condition        NVARCHAR(MAX) NULL,       -- JSON, e.g. {"equipmentType":[...],"heatingType":[...],"season":[...]}
        fs_active           BIT           NOT NULL CONSTRAINT DF_FormSection_active DEFAULT (1)
    );

    CREATE INDEX IX_FormSection_Template ON dbo.FormSection(ft_id, fs_order);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FormQuestion')
BEGIN
    CREATE TABLE dbo.FormQuestion (
        fq_id               INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        fq_insertdatetime   DATETIME      NOT NULL CONSTRAINT DF_FormQuestion_insertdatetime DEFAULT (GETDATE()),
        fq_modifieddatetime DATETIME      NULL,
        fs_id               INT           NOT NULL REFERENCES dbo.FormSection(fs_id),
        fq_code             NVARCHAR(12)  NOT NULL,   -- dictionary ID: F135, T044, P16; stable key for formulas, exports, PDF, asset attributes
        clat_id             INT           NOT NULL REFERENCES dbo.CheckListAnswerType(clat_id),   -- Textbox | Drop Down | Signature | Photo | Readonly
        fq_question         NVARCHAR(400) NOT NULL,
        fq_order            INT           NOT NULL CONSTRAINT DF_FormQuestion_order DEFAULT (0),
        fq_requirement      NVARCHAR(15)  NOT NULL,   -- Always | Recommended | Optional | Conditional | Configured
        fq_condition        NVARCHAR(MAX) NULL,       -- JSON: {"equipmentType":[..],"heatingType":[..],"attr":{"BeltDriven":true},"mount":[..],"season":[..],"whenCode":"F110","whenIn":[..],"any":[{..},{..}]}
        fal_id              INT           NULL REFERENCES dbo.FormAnswerList(fal_id),   -- named list, or ...
        fq_answervalues     NVARCHAR(MAX) NULL,       -- ... one-off list, semicolon separated (same idea as clq_answervalues)
        fq_datatype         NVARCHAR(12)  NULL,       -- Text | Number | Decimal | Bool | Date | DateTime | MultiSelect
        fq_unit             NVARCHAR(12)  NULL,       -- °F, psig, in. w.c., A, V, ppm, %, RPM, CFM ...
        fq_min              DECIMAL(12,3) NULL,       -- sanity range for numeric readings (not in the workbook)
        fq_max              DECIMAL(12,3) NULL,
        fq_repeatkey        NVARCHAR(15)  NULL,       -- Circuit | Compressor | HeatStage | Phase | FilterBank | BeltDrive (count comes from the asset)
        fq_calcformula      NVARCHAR(200) NULL,       -- e.g. {F093}-{F092}; codes in braces reference other questions
        fq_writesto         NVARCHAR(10)  NOT NULL CONSTRAINT DF_FormQuestion_writesto DEFAULT ('Visit'),   -- Visit | Asset
        fq_photorequired    NVARCHAR(20)  NOT NULL CONSTRAINT DF_FormQuestion_photorequired DEFAULT ('No'),   -- Always | CustomerConfigured | IfIssue | Recommended | No
        fq_phototiming      NVARCHAR(12)  NULL,       -- Before | After | During | BeforeAfter
        fq_linkedcode       NVARCHAR(12)  NULL,       -- the task whose failing answer makes an IfIssue photo mandatory
        fq_estminutes       DECIMAL(5,2)  NULL,       -- from the dictionary's man-hours column (x60)
        fq_seasons          NVARCHAR(60)  NULL,       -- semicolon list of visit types this question applies to; NULL = all
        fq_skip_answer      NVARCHAR(200) NULL,       -- skip logic carried over from CheckListQuestion
        fq_skip_to_order    INT           NULL,
        fq_triggernote      NVARCHAR(400) NULL,       -- dictionary "Trigger / Applies When" text when it could not be made structured
        fq_active           BIT           NOT NULL CONSTRAINT DF_FormQuestion_active DEFAULT (1),
        CONSTRAINT UQ_FormQuestion_Section_Code UNIQUE (fs_id, fq_code)   -- per-template uniqueness is enforced by the API
    );

    CREATE INDEX IX_FormQuestion_Section ON dbo.FormQuestion(fs_id, fq_order);
    CREATE INDEX IX_FormQuestion_Code ON dbo.FormQuestion(fq_code);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'FormRule')
BEGIN
    CREATE TABLE dbo.FormRule (
        fr_id               INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        fr_insertdatetime   DATETIME      NOT NULL CONSTRAINT DF_FormRule_insertdatetime DEFAULT (GETDATE()),
        fr_modifieddatetime DATETIME      NULL,
        xccc_id             INT           NOT NULL REFERENCES dbo.xrefCompanyCallCenter(xccc_id),
        t_id                INT           NOT NULL REFERENCES dbo.Trade(t_id),          -- parent trade
        ft_id               INT           NULL REFERENCES dbo.FormTemplate(ft_id),      -- template version the customer is on
        fr_active           BIT           NOT NULL CONSTRAINT DF_FormRule_active DEFAULT (1),
        fr_note             NVARCHAR(MAX) NULL,
        CONSTRAINT UQ_FormRule_Company_Trade UNIQUE (xccc_id, t_id)
    );

    CREATE INDEX IX_FormRule_Template ON dbo.FormRule(ft_id);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'xrefFormRuleQuestion')
BEGIN
    CREATE TABLE dbo.xrefFormRuleQuestion (
        xfrq_id                  INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        xfrq_insertdatetime      DATETIME      NOT NULL CONSTRAINT DF_xrefFormRuleQuestion_insertdatetime DEFAULT (GETDATE()),
        xfrq_modifieddatetime    DATETIME      NULL,
        fr_id                    INT           NOT NULL REFERENCES dbo.FormRule(fr_id) ON DELETE CASCADE,
        fq_id                    INT           NOT NULL REFERENCES dbo.FormQuestion(fq_id),
        xfrq_enabled             BIT           NOT NULL CONSTRAINT DF_xrefFormRuleQuestion_enabled DEFAULT (1),
        xfrq_requirementoverride NVARCHAR(15)  NULL,   -- Always | Recommended | Optional | Hidden
        xfrq_questionoverride    NVARCHAR(400) NULL,   -- customer wording
        xfrq_photooverride       NVARCHAR(20)  NULL,   -- Always | IfIssue | Recommended | No
        CONSTRAINT UQ_xrefFormRuleQuestion_Rule_Question UNIQUE (fr_id, fq_id)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PMBillingMode')
BEGIN
    CREATE TABLE dbo.PMBillingMode (
        pmbm_id               INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        pmbm_insertdatetime   DATETIME      NOT NULL CONSTRAINT DF_PMBillingMode_insertdatetime DEFAULT (GETDATE()),
        pmbm_modifieddatetime DATETIME      NULL,
        pmbm_mode             NVARCHAR(30)  NOT NULL,   -- SetNte | ContractTiered | TimeAndMaterials
        pmbm_description      NVARCHAR(200) NULL,
        pmbm_order            INT           NOT NULL CONSTRAINT DF_PMBillingMode_order DEFAULT (0),
        CONSTRAINT UQ_PMBillingMode_Mode UNIQUE (pmbm_mode)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM dbo.PMBillingMode)
BEGIN
    INSERT INTO dbo.PMBillingMode (pmbm_mode, pmbm_description, pmbm_order)
    VALUES
        ('SetNte',           'Set NTE - the final total must equal the work order NTE', 1),
        ('ContractTiered',   'Contract pricing - tiered price sheet by equipment type and tonnage', 2),
        ('TimeAndMaterials', 'Time and materials - hourly rate with optional caps', 3);
END;
GO

-- PM business terms: exactly one row per FormRule (created with the rule from the PM CUSTOMER RULES tab)
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PMRule')
BEGIN
    CREATE TABLE dbo.PMRule (
        pmr_id                        INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        pmr_insertdatetime            DATETIME      NOT NULL CONSTRAINT DF_PMRule_insertdatetime DEFAULT (GETDATE()),
        pmr_modifieddatetime          DATETIME      NULL,
        fr_id                         INT           NOT NULL REFERENCES dbo.FormRule(fr_id) ON DELETE CASCADE,
        -- billing
        pmbm_id                       INT           NULL REFERENCES dbo.PMBillingMode(pmbm_id),
        pmr_nteguideline              NVARCHAR(20)  NULL,   -- MatchSetNte | MatchPortalNte | IgnoreNte
        pmr_firsttimerule             NVARCHAR(30)  NULL,   -- None | TripChargeExtra | BillTripLaborMaterials
        pmr_hourlyrate                DECIMAL(9,2)  NULL,
        pmr_hourcapperunit            DECIMAL(5,2)  NULL,
        pmr_hourcappervisit           DECIMAL(5,2)  NULL,
        pmr_tripcharge                DECIMAL(9,2)  NULL,
        att_id_pricingcontract        INT           NULL REFERENCES dbo.Attachment(att_id),       -- company attachment
        pmr_pricingnote               NVARCHAR(MAX) NULL,   -- pricing sheet "Additional Comments"
        -- inclusions
        pmr_filtersincluded           BIT           NULL,
        pmr_nofilterchange            BIT           NULL,   -- e.g. Petco: no filters changed or charged
        pmr_freefilters               INT           NULL,   -- e.g. CLS: 5 filters free
        pmr_freebeltchangesperyear    INT           NULL,   -- e.g. CCF: 1 belt change per year free
        pmr_beltschargeable           BIT           NULL,   -- e.g. First Cash: belts / coil cleaner billable as add-on repairs
        -- customer-required form
        pmr_customerform              NVARCHAR(20)  NULL,   -- None | CustomerPdfForm | ExternalLink | Portal
        pmr_customerformurl           NVARCHAR(500) NULL,
        att_id_customerform           INT           NULL REFERENCES dbo.Attachment(att_id),
        pmr_customerformnote          NVARCHAR(400) NULL,   -- e.g. "2 checklists required", "data sheet only if portal down"
        -- parameters shown to the tech
        pmr_coolingsetpoint           TINYINT       NULL,   -- °F
        pmr_heatingsetpoint           TINYINT       NULL,   -- °F
        pmr_thermostatschedule        NVARCHAR(20)  NULL,   -- 24/7 | Occupied | CustomerSpecific
        pmr_thermostatschedulenote    NVARCHAR(400) NULL,
        pmr_thermostatlock            BIT           NULL,
        pmr_antialgae                 BIT           NULL,
        pmr_phototimestamp            BIT           NULL,
        pmr_managerseesoldfilters     BIT           NULL,
        pmr_pricingdiscussallowed     BIT           NULL,
        pmr_immediatequoterequired    BIT           NULL,
        pmr_immediatecallifincomplete BIT           NULL,
        pmr_submissiondeadline        NVARCHAR(20)  NULL,   -- SameDay | Within1Week | CustomerSpecific
        pmr_submissiondeadlinenote    NVARCHAR(400) NULL,
        pmr_closeoutdocs              NVARCHAR(200) NULL,   -- CSV: Invoice, Signoff, Checklist, Photos, Quote, SiteStamp
        pmr_submissiondestination     NVARCHAR(20)  NULL,   -- Portal | Email | CallCenter | Api
        pmr_ivrrequired               BIT           NULL,
        -- contacts
        pmr_contactname               NVARCHAR(100) NULL,
        pmr_contactemail              NVARCHAR(200) NULL,
        pmr_notifyemail               NVARCHAR(200) NULL,
        CONSTRAINT UQ_PMRule_FormRule UNIQUE (fr_id)
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PMRateTier')
BEGIN
    CREATE TABLE dbo.PMRateTier (
        pmrt_id                  INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        pmrt_insertdatetime      DATETIME      NOT NULL CONSTRAINT DF_PMRateTier_insertdatetime DEFAULT (GETDATE()),
        pmrt_modifieddatetime    DATETIME      NULL,
        pmr_id                   INT           NOT NULL REFERENCES dbo.PMRule(pmr_id) ON DELETE CASCADE,
        asc_id                   INT           NULL REFERENCES dbo.AssetCategory(asc_id),   -- equipment type; NULL = any
        pmrt_label               NVARCHAR(60)  NULL,       -- RTU, Split System, Additional Units, Coil cleaner ...
        pmrt_mintons             DECIMAL(5,1)  NULL,
        pmrt_maxtons             DECIMAL(5,1)  NULL,
        pmrt_firstunitprice      DECIMAL(9,2)  NULL,
        pmrt_additionalunitprice DECIMAL(9,2)  NULL,
        pmrt_addon               BIT           NOT NULL CONSTRAINT DF_PMRateTier_addon DEFAULT (0),   -- 1 = add-on line (belt change, coil cleaner), priced in pmrt_firstunitprice
        pmrt_addonlabel          NVARCHAR(60)  NULL,
        pmrt_order               INT           NOT NULL CONSTRAINT DF_PMRateTier_order DEFAULT (0),
        pmrt_active              BIT           NOT NULL CONSTRAINT DF_PMRateTier_active DEFAULT (1)
    );

    CREATE INDEX IX_PMRateTier_Rule ON dbo.PMRateTier(pmr_id, pmrt_order);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'PMSeason')
BEGIN
    CREATE TABLE dbo.PMSeason (
        pms_id               INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        pms_insertdatetime   DATETIME      NOT NULL CONSTRAINT DF_PMSeason_insertdatetime DEFAULT (GETDATE()),
        pms_modifieddatetime DATETIME      NULL,
        pmr_id               INT           NOT NULL REFERENCES dbo.PMRule(pmr_id) ON DELETE CASCADE,
        pms_name             NVARCHAR(40)  NOT NULL,   -- Spring / Fall / Major / Minor ...
        t_id_season          INT           NULL REFERENCES dbo.Trade(t_id),   -- PM sub-trade chosen at ticket entry (PM - Spring, PM - Fall ...)
        pms_visittype        NVARCHAR(20)  NULL,       -- a "PM Season" answer-list value: Spring Cooling | Fall Heating | Full PM | Inspection Only | Filter Change | Inventory Only (matches fq_seasons)
        pms_startmonthday    CHAR(5)       NULL,       -- MM-DD
        pms_endmonthday      CHAR(5)       NULL,       -- MM-DD
        pms_visitsperyear    INT           NULL,
        pms_intervaldays     INT           NULL,
        pms_order            INT           NOT NULL CONSTRAINT DF_PMSeason_order DEFAULT (0),
        pms_active           BIT           NOT NULL CONSTRAINT DF_PMSeason_active DEFAULT (1)
    );

    CREATE INDEX IX_PMSeason_Rule ON dbo.PMSeason(pmr_id, pms_order);
END;
GO

-- Feature flag (o_id is NOT NULL on ConfigSetting; the app hardcodes 1). Same convention as Weekly2faCode: '0' off, '1' on.
IF NOT EXISTS (SELECT 1 FROM ConfigSetting WHERE cs_type = 'featureflag' AND cs_identifier = 'PreventativeMaintenance')
BEGIN
    INSERT INTO ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_description)
    VALUES (1, 'featureflag', 'PreventativeMaintenance', '0', 'Preventative Maintenance workflow: Form Templates settings page and PM Customer Rules tab (0 = off, 1 = on)');
END;
GO
