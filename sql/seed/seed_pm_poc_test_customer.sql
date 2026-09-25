-- =====================================================================================================
-- TEST ONLY. Preventative Maintenance POC test customer: 23rd Group Facility Services / DELETE - Archwood Meadows
-- (xrefCompanyCallCenter 25, company 10, location "Archwood Meadows" 4661). Chris's pick (2026-09-24) for all PM
-- testing; every piece of POC content the workflow needs for this pairing is added here so the New Service Request
-- Preventative path, the FORM RULES tab and the LOCATIONS > PM panel all have something to show.
--
-- Loosely follows the mockup story (Temp\PM\mockups: four rooftop units, Fall Heating, contract pricing, coil cleaner):
--   1. Labor rates for PM - Spring and PM - Fall (copied from the pairing's PM - HVAC rate) so the seasonal sub-trades
--      appear in the Preventative trade list.
--   2. PM terms on the pairing's HVAC form rule: contract pricing, "do not follow NTE", trip charge on a first-time PM,
--      inclusions, customer form, setpoints and the other parameters, contact. Only NULL columns are filled, so edits
--      made on the FORM RULES tab survive a re-run.
--   3. Price tiers: RTU and Split System by tonnage band plus a fallback tier for any other type, and two add-ons
--      (coil cleaner per visit, belt change per unit). The test tiers that were there before are replaced.
--   4. Seasons: Spring Cooling (PM - Spring), Fall Heating (PM - Fall), Full PM (PM - HVAC).
--   5. Three more rooftop units at Archwood Meadows (RTU-2 Carrier, RTU-3 Lennox, RTU-4 Trane) with filters, belts and
--      attributes; the profile's unit count set to 4 and Roof hatch + Ladder added to its access requirements.
-- Re-runnable: everything is keyed by name / tag and skipped when present. Never part of a PROD deploy.
-- =====================================================================================================
SET NOCOUNT ON;

DECLARE @xccc INT = (SELECT TOP 1 xccc_id FROM dbo.xrefCompanyCallCenter WHERE cc_id = 106 AND c_id = 10);
DECLARE @hvac INT = (SELECT TOP 1 t_id FROM dbo.Trade WHERE t_trade = 'HVAC' AND t_parentonly = 1);
DECLARE @lId  INT = (SELECT TOP 1 l_id FROM dbo.Location WHERE c_id = 10 AND LTRIM(RTRIM(l_location)) = 'Archwood Meadows');
IF @xccc IS NULL OR @hvac IS NULL OR @lId IS NULL
BEGIN
    PRINT 'Pairing 23rd Group / DELETE - Archwood Meadows, the HVAC parent trade or the Archwood Meadows location was not found; nothing seeded.';
    RETURN;
END;
PRINT 'Pairing xccc_id ' + CAST(@xccc AS VARCHAR(10)) + ', location l_id ' + CAST(@lId AS VARCHAR(10));

------------------------------------------------------------------------------------------------------
-- 1. Labor rates for the seasonal PM sub-trades (copied from PM - HVAC)
------------------------------------------------------------------------------------------------------
DECLARE @pmHvacRate INT = (SELECT TOP 1 lr.lr_id FROM dbo.LaborRate lr JOIN dbo.Trade t ON t.t_id = lr.t_id WHERE lr.xccc_id = @xccc AND t.t_trade = 'PM - HVAC');
IF @pmHvacRate IS NULL
    PRINT 'No PM - HVAC labor rate on the pairing; seasonal rates not copied (add PM - HVAC on the TRADES tab first).';
ELSE
BEGIN
    INSERT INTO dbo.LaborRate (xccc_id, t_id, lr_descriptionoverride, lr_rateregular, lr_rateovertime, lr_rateholiday, lr_ratespecial, lr_nte,
        lr_ratescheduledafterhours, lr_rateregulardiscount, lr_rateregulardiscounthourslimit, lr_ratehelper, lr_ratehelperovertime, lr_rateflat,
        lr_tripcharge, lr_markup, lr_flatorhourly, lr_note)
    SELECT src.xccc_id, t.t_id, src.lr_descriptionoverride, src.lr_rateregular, src.lr_rateovertime, src.lr_rateholiday, src.lr_ratespecial, src.lr_nte,
        src.lr_ratescheduledafterhours, src.lr_rateregulardiscount, src.lr_rateregulardiscounthourslimit, src.lr_ratehelper, src.lr_ratehelperovertime, src.lr_rateflat,
        src.lr_tripcharge, src.lr_markup, src.lr_flatorhourly, 'PM POC test seed (copied from PM - HVAC)'
    FROM dbo.LaborRate src
    CROSS JOIN dbo.Trade t
    WHERE src.lr_id = @pmHvacRate
      AND t.t_id_parent = @hvac AND t.t_active = 1 AND t.t_trade IN ('PM - Spring', 'PM - Fall')
      AND NOT EXISTS (SELECT 1 FROM dbo.LaborRate x WHERE x.xccc_id = @xccc AND x.t_id = t.t_id);
    PRINT 'Seasonal PM labor rates added: ' + CAST(@@ROWCOUNT AS VARCHAR(10));
END;

------------------------------------------------------------------------------------------------------
-- 2. Form rule + PM terms (fill only what is empty)
------------------------------------------------------------------------------------------------------
DECLARE @ft INT = (SELECT TOP 1 ft_id FROM dbo.FormTemplate WHERE t_id = @hvac AND ft_active = 1 ORDER BY ft_version DESC);
IF NOT EXISTS (SELECT 1 FROM dbo.FormRule WHERE xccc_id = @xccc AND t_id = @hvac)
    INSERT INTO dbo.FormRule (xccc_id, t_id, ft_id, fr_active, fr_note) VALUES (@xccc, @hvac, @ft, 1, 'PM POC test customer');
DECLARE @fr INT = (SELECT fr_id FROM dbo.FormRule WHERE xccc_id = @xccc AND t_id = @hvac);
UPDATE dbo.FormRule SET ft_id = ISNULL(ft_id, @ft), fr_active = 1 WHERE fr_id = @fr;

IF NOT EXISTS (SELECT 1 FROM dbo.PMRule WHERE fr_id = @fr)
    INSERT INTO dbo.PMRule (fr_id) VALUES (@fr);
DECLARE @pmr INT = (SELECT pmr_id FROM dbo.PMRule WHERE fr_id = @fr);
DECLARE @contract INT = (SELECT pmbm_id FROM dbo.PMBillingMode WHERE pmbm_mode = 'ContractTiered');

UPDATE dbo.PMRule SET
    pmbm_id                       = ISNULL(pmbm_id, @contract),
    pmr_nteguideline              = ISNULL(pmr_nteguideline, N'IgnoreNte'),
    pmr_firsttimerule             = ISNULL(pmr_firsttimerule, N'TripChargeExtra'),
    pmr_tripcharge                = ISNULL(pmr_tripcharge, 65.00),
    pmr_hourlyrate                = ISNULL(pmr_hourlyrate, 80.00),
    pmr_hourcapperunit            = ISNULL(pmr_hourcapperunit, 1.50),
    pmr_pricingnote               = ISNULL(pmr_pricingnote, N'POC test customer: contract price sheet by unit type and tonnage; coil cleaner $41.80; belt change $30 per unit; 5 filters free.'),
    pmr_filtersincluded           = ISNULL(pmr_filtersincluded, 1),
    pmr_nofilterchange            = ISNULL(pmr_nofilterchange, 0),
    pmr_freefilters               = ISNULL(pmr_freefilters, 5),
    pmr_freebeltchangesperyear    = ISNULL(pmr_freebeltchangesperyear, 1),
    pmr_beltschargeable           = ISNULL(pmr_beltschargeable, 1),
    pmr_customerform              = ISNULL(pmr_customerform, N'CustomerPdfForm'),
    pmr_customerformnote          = ISNULL(pmr_customerformnote, N'2 customer checklists required (PDF), provided by the call center'),
    pmr_coolingsetpoint           = ISNULL(pmr_coolingsetpoint, 70),
    pmr_heatingsetpoint           = ISNULL(pmr_heatingsetpoint, 68),
    pmr_thermostatschedule        = ISNULL(pmr_thermostatschedule, N'24/7'),
    pmr_thermostatlock            = ISNULL(pmr_thermostatlock, 1),
    pmr_antialgae                 = ISNULL(pmr_antialgae, 1),
    pmr_phototimestamp            = ISNULL(pmr_phototimestamp, 1),
    pmr_managerseesoldfilters     = ISNULL(pmr_managerseesoldfilters, 1),
    pmr_pricingdiscussallowed     = ISNULL(pmr_pricingdiscussallowed, 0),
    pmr_immediatequoterequired    = ISNULL(pmr_immediatequoterequired, 1),
    pmr_immediatecallifincomplete = ISNULL(pmr_immediatecallifincomplete, 1),
    pmr_submissiondeadline        = ISNULL(pmr_submissiondeadline, N'Within1Week'),
    pmr_closeoutdocs              = ISNULL(pmr_closeoutdocs, N'Invoice,Signoff,Checklist,Photos'),
    pmr_submissiondestination     = ISNULL(pmr_submissiondestination, N'Email'),
    pmr_ivrrequired               = ISNULL(pmr_ivrrequired, 0),
    pmr_contactname               = ISNULL(pmr_contactname, N'Kelli Whelan'),
    pmr_contactemail              = ISNULL(pmr_contactemail, N'kwhelan@example.com'),
    pmr_modifieddatetime          = GETDATE()
WHERE pmr_id = @pmr;
PRINT 'PM terms filled on rule fr_id ' + CAST(@fr AS VARCHAR(10)) + ' / pmr_id ' + CAST(@pmr AS VARCHAR(10));

------------------------------------------------------------------------------------------------------
-- 3. Price tiers (replace: the earlier rows were placeholders from the slice-1 build)
------------------------------------------------------------------------------------------------------
DECLARE @rtu INT   = (SELECT TOP 1 asc_id FROM dbo.AssetCategory WHERE t_id = @hvac AND LTRIM(RTRIM(asc_category)) = 'RTU');
DECLARE @split INT = (SELECT TOP 1 asc_id FROM dbo.AssetCategory WHERE t_id = @hvac AND LTRIM(RTRIM(asc_category)) = 'Split System');
DELETE FROM dbo.PMRateTier WHERE pmr_id = @pmr;
INSERT INTO dbo.PMRateTier (pmr_id, asc_id, pmrt_label, pmrt_mintons, pmrt_maxtons, pmrt_firstunitprice, pmrt_additionalunitprice, pmrt_addon, pmrt_addonlabel, pmrt_order, pmrt_active)
VALUES
    (@pmr, @rtu,   N'RTU up to 10 t',          NULL,  10.00,  75.00, 60.00, 0, NULL,            1, 1),
    (@pmr, @rtu,   N'RTU 10 to 20 t',          10.01, 20.00,  95.00, 80.00, 0, NULL,            2, 1),
    (@pmr, @rtu,   N'RTU 20 to 40 t',          20.01, 40.00, 140.00, 120.00, 0, NULL,           3, 1),
    (@pmr, @split, N'Split System up to 10 t', NULL,  10.00,  70.00, 55.00, 0, NULL,            4, 1),
    (@pmr, NULL,   N'Any other unit',          NULL,  NULL,   65.00, 50.00, 0, NULL,            5, 1),
    (@pmr, NULL,   N'Coil cleaner',            NULL,  NULL,   41.80, NULL,  1, N'Coil cleaner', 6, 1),
    (@pmr, NULL,   N'Belt change',             NULL,  NULL,   30.00, NULL,  1, N'Belt change (per unit)', 7, 1);
PRINT 'Price tiers loaded: 7';

------------------------------------------------------------------------------------------------------
-- 4. Seasons
------------------------------------------------------------------------------------------------------
DECLARE @spring INT = (SELECT TOP 1 t_id FROM dbo.Trade WHERE t_id_parent = @hvac AND t_trade = 'PM - Spring');
DECLARE @fall   INT = (SELECT TOP 1 t_id FROM dbo.Trade WHERE t_id_parent = @hvac AND t_trade = 'PM - Fall');
DECLARE @pmhvac INT = (SELECT TOP 1 t_id FROM dbo.Trade WHERE t_id_parent = @hvac AND t_trade = 'PM - HVAC');
MERGE dbo.PMSeason AS tgt
USING (VALUES
    (N'Spring Cooling', @spring, N'Spring Cooling', '03-01', '05-31', 2, 180, 1),
    (N'Fall Heating',   @fall,   N'Fall Heating',   '09-01', '11-30', 2, 180, 2),
    (N'Full PM',        @pmhvac, N'Full PM',        NULL,    NULL,    1, NULL, 3)) AS src (name, tid, visittype, startmd, endmd, peryear, interval, ord)
ON tgt.pmr_id = @pmr AND tgt.pms_name = src.name
WHEN NOT MATCHED THEN INSERT (pmr_id, pms_name, t_id_season, pms_visittype, pms_startmonthday, pms_endmonthday, pms_visitsperyear, pms_intervaldays, pms_order, pms_active)
    VALUES (@pmr, src.name, src.tid, src.visittype, src.startmd, src.endmd, src.peryear, src.interval, src.ord, 1);
DECLARE @seasons INT = (SELECT COUNT(*) FROM dbo.PMSeason WHERE pmr_id = @pmr);
PRINT 'Seasons present: ' + CAST(@seasons AS VARCHAR(10));

------------------------------------------------------------------------------------------------------
-- 5. Units at Archwood Meadows + profile
------------------------------------------------------------------------------------------------------
DECLARE @rooftop INT = (SELECT TOP 1 aml_id FROM dbo.AssetMountLocation WHERE aml_location = N'Rooftop');
DECLARE @filter INT = (SELECT TOP 1 asct_id FROM dbo.AssetComponentType WHERE t_id = @hvac AND asct_type = N'Filter');
DECLARE @belt   INT = (SELECT TOP 1 asct_id FROM dbo.AssetComponentType WHERE t_id = @hvac AND asct_type = N'Belt');

DECLARE @Units TABLE (tag NVARCHAR(30), maker NVARCHAR(50), model NVARCHAR(60), serial NVARCHAR(60), tons DECIMAL(6,2), heat NVARCHAR(30), refrig NVARCHAR(20), yr SMALLINT, area NVARCHAR(100),
                      filtersize NVARCHAR(30), filterqty INT, filtertype NVARCHAR(40), beltsize NVARCHAR(20), beltqty INT, beltdriven BIT, economizer BIT, circuits INT, compressors INT, phases INT, heatstages INT);
INSERT INTO @Units VALUES
    (N'RTU-2', N'Carrier', N'48TCED08A2A5', N'1219G30418', 7.5, N'Gas', N'R-410A', 2019, N'Sales floor - rear',        N'20x25x2', 4, N'Pleated MERV 8', N'A46', 1, 1, 1, 2, 2, 3, 2),
    (N'RTU-3', N'Lennox',  N'LGH060H4B',    N'5816D11207', 5.0, N'Gas', N'R-410A', 2016, N'Stockroom / offices',       N'16x25x2', 2, N'Pleated',        NULL,  0, 0, 0, 1, 1, 3, 1),
    (N'RTU-4', N'Trane',   N'YSC036E3RHA',  N'17284KLM3F', 3.0, N'Gas', N'R-410A', 2017, N'Fitting rooms / restrooms', N'16x20x1', 2, N'Pleated',        NULL,  0, 0, 0, 1, 1, 3, 1);

DECLARE @tag NVARCHAR(30), @maker NVARCHAR(50), @model NVARCHAR(60), @serial NVARCHAR(60), @tons DECIMAL(6,2), @heat NVARCHAR(30), @refrig NVARCHAR(20), @yr SMALLINT, @area NVARCHAR(100),
        @fsize NVARCHAR(30), @fqty INT, @ftype NVARCHAR(40), @bsize NVARCHAR(20), @bqty INT, @beltdriven BIT, @econ BIT, @circ INT, @comp INT, @ph INT, @stages INT, @asId INT, @asm INT, @added INT = 0;
DECLARE cur CURSOR LOCAL FAST_FORWARD FOR SELECT * FROM @Units;
OPEN cur;
FETCH NEXT FROM cur INTO @tag, @maker, @model, @serial, @tons, @heat, @refrig, @yr, @area, @fsize, @fqty, @ftype, @bsize, @bqty, @beltdriven, @econ, @circ, @comp, @ph, @stages;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (SELECT 1 FROM dbo.Asset WHERE l_id = @lId AND as_unittag = @tag)
    BEGIN
        SET @asm = (SELECT TOP 1 asm_id FROM dbo.AssetManufacturer WHERE asc_id = @rtu AND asm_manufacturer = @maker);
        INSERT INTO dbo.Asset (l_id, asc_id, asm_id, as_manufacturer, as_modelnumber, as_serialnumber, as_description, as_specialinstructions,
            as_unittag, as_capacitytons, as_heatingtype, as_refrigeranttype, as_manufactureyear, as_servedarea, aml_id, as_active, as_insertdatetime)
        VALUES (@lId, @rtu, @asm, @maker, @model, @serial, @maker + N' ' + @model + N' rooftop unit', N'PM POC test seed',
            @tag, @tons, @heat, @refrig, @yr, @area, @rooftop, 1, GETDATE());
        SET @asId = SCOPE_IDENTITY();
        INSERT INTO dbo.AssetComponent (as_id, asct_id, ascp_quantity, ascp_size, ascp_type, ascp_active) VALUES (@asId, @filter, @fqty, @fsize, @ftype, 1);
        IF @bqty > 0 INSERT INTO dbo.AssetComponent (as_id, asct_id, ascp_quantity, ascp_size, ascp_active) VALUES (@asId, @belt, @bqty, @bsize, 1);
        INSERT INTO dbo.AssetAttribute (as_id, asat_id, asa_value, asa_numeric)
        SELECT @asId, at.asat_id, v.val, v.num
        FROM (VALUES
            (N'BeltDriven',  CASE WHEN @beltdriven = 1 THEN N'true' ELSE N'false' END, NULL),
            (N'Economizer',  CASE WHEN @econ = 1 THEN N'true' ELSE N'false' END, NULL),
            (N'EMS',         N'false', NULL),
            (N'ThreePhase',  N'true', NULL),
            (N'Ducted',      N'true', NULL),
            (N'Circuits',    CAST(@circ AS NVARCHAR(10)), @circ),
            (N'Compressors', CAST(@comp AS NVARCHAR(10)), @comp),
            (N'Phases',      CAST(@ph AS NVARCHAR(10)), @ph),
            (N'HeatStages',  CAST(@stages AS NVARCHAR(10)), @stages),
            (N'FilterBanks', N'1', 1),
            (N'BeltDrives',  CAST(@bqty AS NVARCHAR(10)), @bqty)) AS v (k, val, num)
        JOIN dbo.AssetAttributeType at ON at.t_id = @hvac AND at.asat_key = v.k;
        SET @added += 1;
    END;
    FETCH NEXT FROM cur INTO @tag, @maker, @model, @serial, @tons, @heat, @refrig, @yr, @area, @fsize, @fqty, @ftype, @bsize, @bqty, @beltdriven, @econ, @circ, @comp, @ph, @stages;
END;
CLOSE cur; DEALLOCATE cur;
PRINT 'Units added at Archwood Meadows: ' + CAST(@added AS VARCHAR(10));

-- Profile: four units, rooftop, roof hatch + ladder (keeps whatever else is already recorded)
DECLARE @hatch INT = (SELECT TOP 1 lar_id FROM dbo.LocationAccessRequirement WHERE lar_requirement = N'Roof hatch');
DECLARE @ladder INT = (SELECT TOP 1 lar_id FROM dbo.LocationAccessRequirement WHERE lar_requirement = N'Ladder');
IF NOT EXISTS (SELECT 1 FROM dbo.LocationTradeProfile WHERE l_id = @lId AND t_id = @hvac)
    INSERT INTO dbo.LocationTradeProfile (l_id, t_id, ltp_unitcount, ltp_accessrequirements, ltp_accessnote, ltp_mountlocations, ltp_managername, ltp_managerphone, ltp_note)
    VALUES (@lId, @hvac, 4, CAST(@hatch AS NVARCHAR(10)) + ',' + CAST(@ladder AS NVARCHAR(10)), N'Roof hatch in the stockroom; ladder behind the receiving door. Ask the manager for the key.',
            CAST(@rooftop AS NVARCHAR(10)), N'Terri Blackwell', N'(615) 555-0142', N'PM POC test location');
ELSE
    UPDATE dbo.LocationTradeProfile SET
        ltp_unitcount = CASE WHEN ISNULL(ltp_unitcount, 0) < 4 THEN 4 ELSE ltp_unitcount END,
        ltp_accessrequirements = CASE
            WHEN ISNULL(ltp_accessrequirements, '') = '' THEN CAST(@hatch AS NVARCHAR(10)) + ',' + CAST(@ladder AS NVARCHAR(10))
            ELSE ltp_accessrequirements
                + CASE WHEN ',' + ltp_accessrequirements + ',' LIKE '%,' + CAST(@hatch AS NVARCHAR(10)) + ',%' THEN '' ELSE ',' + CAST(@hatch AS NVARCHAR(10)) END
                + CASE WHEN ',' + ltp_accessrequirements + ',' LIKE '%,' + CAST(@ladder AS NVARCHAR(10)) + ',%' THEN '' ELSE ',' + CAST(@ladder AS NVARCHAR(10)) END END,
        ltp_mountlocations = CASE WHEN ISNULL(ltp_mountlocations, '') = '' THEN CAST(@rooftop AS NVARCHAR(10)) ELSE ltp_mountlocations END,
        ltp_modifieddatetime = GETDATE()
    WHERE l_id = @lId AND t_id = @hvac;
PRINT 'Profile ready.';

SELECT 'labor rates' AS what, COUNT(*) AS n FROM dbo.LaborRate lr JOIN dbo.Trade t ON t.t_id = lr.t_id WHERE lr.xccc_id = @xccc AND t.t_id_parent = @hvac AND t.t_trade LIKE 'PM%'
UNION ALL SELECT 'tiers', COUNT(*) FROM dbo.PMRateTier WHERE pmr_id = @pmr
UNION ALL SELECT 'seasons', COUNT(*) FROM dbo.PMSeason WHERE pmr_id = @pmr
UNION ALL SELECT 'HVAC units at location', COUNT(*) FROM dbo.Asset s JOIN dbo.AssetCategory c ON c.asc_id = s.asc_id WHERE s.l_id = @lId AND c.t_id = @hvac AND s.as_active = 1;
