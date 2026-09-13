PRINT 'Seeding backlogitem records';

IF OBJECT_ID('dbo.backlogitem', 'U') IS NULL
BEGIN
    RAISERROR('dbo.backlogitem does not exist. Run create_backlogitem_table.sql first.', 16, 1);
    RETURN;
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'EMP-DIR' AND bi_title = N'Employee Directory Zone Export')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'EMP-DIR',
        N'Employee Directory Zone Export',
        N'Bug with Employee directory export.eml',
        N'Daniel Stone',
        '2026-03-02',
        N'Reporting',
        N'Bug',
        N'High',
        N'Open',
        N'M',
        N'Zone field is not populated in the .csv export for the Employee Directory. The Zone column/value is missing for every employee row in the generated file.',
        NULL,
        N'Bug with Employee directory export
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateMon, 2 Mar 2026 21:35:59 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
Zone is not making it to the .csv
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'811-TRACK' AND bi_title = N'811 Trackable Reporting Field')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'811-TRACK',
        N'811 Trackable Reporting Field',
        N'Add a trackable_reportable field.eml',
        N'Daniel Stone',
        '2026-02-25',
        N'Database',
        N'Feature',
        N'Medium',
        N'Open',
        N'M',
        N'Add a trackable/reportable field (tied to the 811 workflow) to the database. When a tech checks 811, the system should auto-check this field if digging is involved. After quote approval a notification should prompt reaching out to 811. Tech must confirm utility marking before work begins.',
        NULL,
        N'Add a trackable/reportable field
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateWed, 25 Feb 2026 13:27:56 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
I need to add something to tickets that is when 811 is required.  If this is checked,  I would want to be able to report on it (with a date, maybe other information).
The tech would be the one that would normally check this… or if they say there is digging involved it automatically checks this.  Something like that.
Notes for me - Once the quote is approved we would reach out to 811 to get them to mark it.  In an emergency situation, the tech calls 811.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'CONNECT' AND bi_title = N'Service Item Tab Order')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'CONNECT',
        N'Service Item Tab Order',
        N'Connect.210.eml',
        N'(Connect thread)',
        '2026-02-11',
        N'UI/UX',
        N'Feature',
        N'Low',
        N'Open',
        N'M',
        N'Service item tabs should appear at the top of the list when added to a quote or order, not at the bottom. Currently each new item pushes to the bottom, requiring users to scroll down to find it.',
        NULL,
        N'New Post
Fromnoreply@evolutionmaintenance.com
DateWed, 11 Feb 2026 00:05:37 +0000
ToUndisclosed recipients:;
A new post has been added to the message board
Adding service items for incurred or quoted
When adding a new service items can the tab always populate at the top instead of the bottom?  If you are adding 20+ items we have to scroll to the bottom each time we enter an item.  This time consuming.
View this post on the message board:
https://www.evotrakker.com/evotech/board?group=10',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'CONNECT' AND bi_title = N'Swap Quoted / Incurred Items')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'CONNECT',
        N'Swap Quoted / Incurred Items',
        N'Connect.210-2.eml',
        N'(Connect thread)',
        '2026-02-11',
        N'Quoting',
        N'Feature',
        N'Medium',
        N'Open',
        N'M',
        N'Allow techs to swap service items between the Quoted and Incurred sections themselves, without needing to call the office. This would eliminate a significant volume of phone calls and reduce office workload.',
        NULL,
        N'New Post
Fromnoreply@evolutionmaintenance.com
DateTue, 10 Feb 2026 21:58:31 +0000
ToUndisclosed recipients:;
⚠ Content partially recovered — the original message may contain additional formatting or attachments not shown here. Refer to the original .eml file for full content.
A new post has been added to the message board
Quoting additions
Allowing the techs to be able to swap items from the quoted to incurred section, this would eliminate alot of time and phone calls asking the office to handle it.
View this post on the message board:
https://www.evotrakker.com/evotech/board?group=10',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'CONNECT' AND bi_title = N'Carry Over WO Attachments')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'CONNECT',
        N'Carry Over WO Attachments',
        N'Connect.211.eml',
        N'(Connect thread)',
        '2026-02-11',
        N'Work Orders',
        N'Feature',
        N'Low',
        N'Open',
        N'M',
        N'Enable the ability to carry over / include attachments from previous work orders when viewing or creating a new work order on the same service request.',
        NULL,
        N'New Post
Fromnoreply@evolutionmaintenance.com
DateWed, 11 Feb 2026 16:00:47 +0000
ToUndisclosed recipients:;
A new post has been added to the message board
Previous Work Order Attachments
Is there a way we can include it to where when techs go into a site instead of only seeing previous notes it can include the attachments that were uploaded? Photos checklists things of that nature
View this post on the message board:
https://www.evotrakker.com/evotech/board?group=10',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'MTG-0225' AND bi_title = N'811 Utility Marking Checkbox')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'MTG-0225',
        N'811 Utility Marking Checkbox',
        N'From Meeting notes.260225.docx',
        N'(Meeting)',
        '2026-02-25',
        N'Dispatch',
        N'Requirement',
        N'Medium',
        N'Open',
        N'M',
        N'Add a checkbox on the work order for techs to confirm 811 utility marking status. Capture the date checked and an optional note field. Build a report (new section under PAI) showing SR#, date checked, link to SR, and other info.',
        NULL,
        N'Meeting Notes — February 25, 2026
EvoTrakker Development Discussion
SourceFrom Meeting notes.260225.docx
DateFebruary 25, 2026
Notes General
811
- Checkbox — primarily tech to check it
- Capture date checked?
- Any other info (note)?
- Report — SR #, Date checked, link to SR, other info?
- New section under PAI
Receipt Report
- Add drop down similar to WO report — default to 1 year, make initial load fast
Verizon Connect Replacement
- Telemadix / no hardware
- Geotab — API same as what we have with Reveal? (research)
- Longer term, after high priority items
High Priority Items High Priority
NTE — Notification to Tech Phone
- Calc time / service items, etc.
- If X% under — text tech (90%)
- If X% over — text tech, zone email (100%)
- Phone numbers needed for all techs
- Text Message — techs
- Email message for Zone email
- Evo Cell as primary, else personal cell
New Service Request
- Finish up + Preventative Maintenance example
Auto Tech Assignment
- (See separate backlog entry)
Long Term Long Term
- After SR is created, before Invoice created — can we allow changing the Call Center / Company? What issues will that cause?
- Service Request Quotes — able to active/inactive Service Items
- If inactive, then doesn''t appear in the quote
- Possible Report — drive times, average, by time period, need example',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'MTG-0225' AND bi_title = N'Receipt Report Date Filter')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'MTG-0225',
        N'Receipt Report Date Filter',
        N'From Meeting notes.260225.docx',
        N'(Meeting)',
        '2026-02-25',
        N'Reporting',
        N'Feature',
        N'High',
        N'Open',
        N'M',
        N'Receipt report enhancements: add a date-range dropdown (similar to the WO report); default to last 1 year; optimize initial load performance to reduce wait time.',
        NULL,
        N'Meeting Notes — February 25, 2026
EvoTrakker Development Discussion
SourceFrom Meeting notes.260225.docx
DateFebruary 25, 2026
Notes General
811
- Checkbox — primarily tech to check it
- Capture date checked?
- Any other info (note)?
- Report — SR #, Date checked, link to SR, other info?
- New section under PAI
Receipt Report
- Add drop down similar to WO report — default to 1 year, make initial load fast
Verizon Connect Replacement
- Telemadix / no hardware
- Geotab — API same as what we have with Reveal? (research)
- Longer term, after high priority items
High Priority Items High Priority
NTE — Notification to Tech Phone
- Calc time / service items, etc.
- If X% under — text tech (90%)
- If X% over — text tech, zone email (100%)
- Phone numbers needed for all techs
- Text Message — techs
- Email message for Zone email
- Evo Cell as primary, else personal cell
New Service Request
- Finish up + Preventative Maintenance example
Auto Tech Assignment
- (See separate backlog entry)
Long Term Long Term
- After SR is created, before Invoice created — can we allow changing the Call Center / Company? What issues will that cause?
- Service Request Quotes — able to active/inactive Service Items
- If inactive, then doesn''t appear in the quote
- Possible Report — drive times, average, by time period, need example',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'MTG-0225' AND bi_title = N'NTE Utilization Notifications')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'MTG-0225',
        N'NTE Utilization Notifications',
        N'From Meeting notes.260225.docx',
        N'(Meeting)',
        '2026-02-25',
        N'Notifications',
        N'Feature',
        N'High',
        N'Open',
        N'M',
        N'NTE notifications: text the tech when utilization hits 90%; text the tech and send a zone email at 100%. Requires phone numbers on file for all techs — use Evo Cell as primary, personal cell as fallback. Send text to techs, email to Zone manager.',
        NULL,
        N'Meeting Notes — February 25, 2026
EvoTrakker Development Discussion
SourceFrom Meeting notes.260225.docx
DateFebruary 25, 2026
Notes General
811
- Checkbox — primarily tech to check it
- Capture date checked?
- Any other info (note)?
- Report — SR #, Date checked, link to SR, other info?
- New section under PAI
Receipt Report
- Add drop down similar to WO report — default to 1 year, make initial load fast
Verizon Connect Replacement
- Telemadix / no hardware
- Geotab — API same as what we have with Reveal? (research)
- Longer term, after high priority items
High Priority Items High Priority
NTE — Notification to Tech Phone
- Calc time / service items, etc.
- If X% under — text tech (90%)
- If X% over — text tech, zone email (100%)
- Phone numbers needed for all techs
- Text Message — techs
- Email message for Zone email
- Evo Cell as primary, else personal cell
New Service Request
- Finish up + Preventative Maintenance example
Auto Tech Assignment
- (See separate backlog entry)
Long Term Long Term
- After SR is created, before Invoice created — can we allow changing the Call Center / Company? What issues will that cause?
- Service Request Quotes — able to active/inactive Service Items
- If inactive, then doesn''t appear in the quote
- Possible Report — drive times, average, by time period, need example',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'MTG-0225' AND bi_title = N'New Service Request Flow')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'MTG-0225',
        N'New Service Request Flow',
        N'From Meeting notes.260225.docx',
        N'(Meeting)',
        '2026-02-25',
        N'Service Request',
        N'Feature',
        N'Medium',
        N'Open',
        N'M',
        N'Finish up the New Service Request creation flow in the app. Include a Preventative Maintenance SR type as a working example of the completed flow.',
        NULL,
        N'Meeting Notes — February 25, 2026
EvoTrakker Development Discussion
SourceFrom Meeting notes.260225.docx
DateFebruary 25, 2026
Notes General
811
- Checkbox — primarily tech to check it
- Capture date checked?
- Any other info (note)?
- Report — SR #, Date checked, link to SR, other info?
- New section under PAI
Receipt Report
- Add drop down similar to WO report — default to 1 year, make initial load fast
Verizon Connect Replacement
- Telemadix / no hardware
- Geotab — API same as what we have with Reveal? (research)
- Longer term, after high priority items
High Priority Items High Priority
NTE — Notification to Tech Phone
- Calc time / service items, etc.
- If X% under — text tech (90%)
- If X% over — text tech, zone email (100%)
- Phone numbers needed for all techs
- Text Message — techs
- Email message for Zone email
- Evo Cell as primary, else personal cell
New Service Request
- Finish up + Preventative Maintenance example
Auto Tech Assignment
- (See separate backlog entry)
Long Term Long Term
- After SR is created, before Invoice created — can we allow changing the Call Center / Company? What issues will that cause?
- Service Request Quotes — able to active/inactive Service Items
- If inactive, then doesn''t appear in the quote
- Possible Report — drive times, average, by time period, need example',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'MTG-0225' AND bi_title = N'Auto Tech Assignment Logic')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'MTG-0225',
        N'Auto Tech Assignment Logic',
        N'From Meeting notes.260225.docx',
        N'(Meeting)',
        '2026-02-25',
        N'Dispatch',
        N'Feature',
        N'Medium',
        N'Open',
        N'M',
        N'Auto Tech Assignment: implement logic to automatically suggest or assign a technician when a new service request is created, based on zone, skills, and availability.',
        NULL,
        N'Meeting Notes — February 25, 2026
EvoTrakker Development Discussion
SourceFrom Meeting notes.260225.docx
DateFebruary 25, 2026
Notes General
811
- Checkbox — primarily tech to check it
- Capture date checked?
- Any other info (note)?
- Report — SR #, Date checked, link to SR, other info?
- New section under PAI
Receipt Report
- Add drop down similar to WO report — default to 1 year, make initial load fast
Verizon Connect Replacement
- Telemadix / no hardware
- Geotab — API same as what we have with Reveal? (research)
- Longer term, after high priority items
High Priority Items High Priority
NTE — Notification to Tech Phone
- Calc time / service items, etc.
- If X% under — text tech (90%)
- If X% over — text tech, zone email (100%)
- Phone numbers needed for all techs
- Text Message — techs
- Email message for Zone email
- Evo Cell as primary, else personal cell
New Service Request
- Finish up + Preventative Maintenance example
Auto Tech Assignment
- (See separate backlog entry)
Long Term Long Term
- After SR is created, before Invoice created — can we allow changing the Call Center / Company? What issues will that cause?
- Service Request Quotes — able to active/inactive Service Items
- If inactive, then doesn''t appear in the quote
- Possible Report — drive times, average, by time period, need example',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'MTG-0225' AND bi_title = N'Change Call Center Post-Creation')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'MTG-0225',
        N'Change Call Center Post-Creation',
        N'From Meeting notes.260225.docx',
        N'(Meeting)',
        '2026-02-25',
        N'Service Request',
        N'Feature',
        N'Low',
        N'Open',
        N'M',
        N'Long term: allow the Call Center and/or Company to be changed on a service request after SR creation, up until the point an invoice is generated. Need to assess what downstream issues this would cause.',
        NULL,
        N'Meeting Notes — February 25, 2026
EvoTrakker Development Discussion
SourceFrom Meeting notes.260225.docx
DateFebruary 25, 2026
Notes General
811
- Checkbox — primarily tech to check it
- Capture date checked?
- Any other info (note)?
- Report — SR #, Date checked, link to SR, other info?
- New section under PAI
Receipt Report
- Add drop down similar to WO report — default to 1 year, make initial load fast
Verizon Connect Replacement
- Telemadix / no hardware
- Geotab — API same as what we have with Reveal? (research)
- Longer term, after high priority items
High Priority Items High Priority
NTE — Notification to Tech Phone
- Calc time / service items, etc.
- If X% under — text tech (90%)
- If X% over — text tech, zone email (100%)
- Phone numbers needed for all techs
- Text Message — techs
- Email message for Zone email
- Evo Cell as primary, else personal cell
New Service Request
- Finish up + Preventative Maintenance example
Auto Tech Assignment
- (See separate backlog entry)
Long Term Long Term
- After SR is created, before Invoice created — can we allow changing the Call Center / Company? What issues will that cause?
- Service Request Quotes — able to active/inactive Service Items
- If inactive, then doesn''t appear in the quote
- Possible Report — drive times, average, by time period, need example',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'MTG-0225' AND bi_title = N'SR Quote Service Item Visibility')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'MTG-0225',
        N'SR Quote Service Item Visibility',
        N'From Meeting notes.260225.docx',
        N'(Meeting)',
        '2026-02-25',
        N'Quoting',
        N'Feature',
        N'Low',
        N'Open',
        N'M',
        N'Long term: add the ability to mark individual service items as active or inactive within a Service Request quote. Inactive items should not appear in the quote output.',
        NULL,
        N'Meeting Notes — February 25, 2026
EvoTrakker Development Discussion
SourceFrom Meeting notes.260225.docx
DateFebruary 25, 2026
Notes General
811
- Checkbox — primarily tech to check it
- Capture date checked?
- Any other info (note)?
- Report — SR #, Date checked, link to SR, other info?
- New section under PAI
Receipt Report
- Add drop down similar to WO report — default to 1 year, make initial load fast
Verizon Connect Replacement
- Telemadix / no hardware
- Geotab — API same as what we have with Reveal? (research)
- Longer term, after high priority items
High Priority Items High Priority
NTE — Notification to Tech Phone
- Calc time / service items, etc.
- If X% under — text tech (90%)
- If X% over — text tech, zone email (100%)
- Phone numbers needed for all techs
- Text Message — techs
- Email message for Zone email
- Evo Cell as primary, else personal cell
New Service Request
- Finish up + Preventative Maintenance example
Auto Tech Assignment
- (See separate backlog entry)
Long Term Long Term
- After SR is created, before Invoice created — can we allow changing the Call Center / Company? What issues will that cause?
- Service Request Quotes — able to active/inactive Service Items
- If inactive, then doesn''t appear in the quote
- Possible Report — drive times, average, by time period, need example',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'MTG-0225' AND bi_title = N'Drive Time Analytics Report')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'MTG-0225',
        N'Drive Time Analytics Report',
        N'From Meeting notes.260225.docx',
        N'(Meeting)',
        '2026-02-25',
        N'Reporting',
        N'Feature',
        N'Low',
        N'Open',
        N'M',
        N'Long term: build a drive time analytics report showing average drive times by tech, by time period, etc. Need a worked example of what the report should look like before building.',
        NULL,
        N'Meeting Notes — February 25, 2026
EvoTrakker Development Discussion
SourceFrom Meeting notes.260225.docx
DateFebruary 25, 2026
Notes General
811
- Checkbox — primarily tech to check it
- Capture date checked?
- Any other info (note)?
- Report — SR #, Date checked, link to SR, other info?
- New section under PAI
Receipt Report
- Add drop down similar to WO report — default to 1 year, make initial load fast
Verizon Connect Replacement
- Telemadix / no hardware
- Geotab — API same as what we have with Reveal? (research)
- Longer term, after high priority items
High Priority Items High Priority
NTE — Notification to Tech Phone
- Calc time / service items, etc.
- If X% under — text tech (90%)
- If X% over — text tech, zone email (100%)
- Phone numbers needed for all techs
- Text Message — techs
- Email message for Zone email
- Evo Cell as primary, else personal cell
New Service Request
- Finish up + Preventative Maintenance example
Auto Tech Assignment
- (See separate backlog entry)
Long Term Long Term
- After SR is created, before Invoice created — can we allow changing the Call Center / Company? What issues will that cause?
- Service Request Quotes — able to active/inactive Service Items
- If inactive, then doesn''t appear in the quote
- Possible Report — drive times, average, by time period, need example',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'QB-DASH' AND bi_title = N'QuickBooks Data Import Portal')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'QB-DASH',
        N'QuickBooks Data Import Portal',
        N'Need another place... dashboards.eml',
        N'Daniel Stone',
        '2026-03-03',
        N'Reporting',
        N'Feature',
        N'Medium',
        N'Open',
        N'M',
        N'Step 1 — QuickBooks data import: build an upload portal (similar to Heartland/Enterprise) for four custom QB reports: Cost of Goods Sold, All Other Expenses, Total Current Year Sales (remove A/R portion to avoid double-counting), and Sales by Employee Number.',
        NULL,
        N'Need another place to bring in outside data into EvoTrakker... more
dashboards
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateTue, 3 Mar 2026 14:39:27 +0000
ToChris Collins <chris.collins@outlook.com>
Short story  - we are going to need to build more dashboards into EvoTrakker.  We are currently working with an outside consultant (Jim) on what some of those metrics are going to be (more to come on that).  At the same time, I want to integrate an outside
report I currently run (the Profit Report) into EvoTrakker.  The more I talk to Jim, pulling in the data for the Profit Report will probably cover most of the metrics he will suggest, and it accomplishes something I need as well.
Longer story  - today I currently pull four custom reports out of QuickBooks (examples attached - I copy and dump the data as is for three of these, one must be slightly modified).  Two on expenses (Cost of Goods Sold, All other Expenses) and two on revenue
(Total Current Year Sales, Sales by Employee Number).   With the Current Year Sales report from QuickBooks, I remove the accounts receivable portion or it double counts things.
•
Step one of EvoTrakker integration would be getting a home for these to be uploaded (similar to how I upload the Heartland time off data or the Enterprise maintenance data).
•
Step two would be to ensure that the necessary lookup “tables” or data points are also in EvoTrakker (most are already there). These would include:
•
Employee Number / Name (called class code in QuickBooks)
•
Zone Assignments
•
Tying the data back (through WO#) to trades and customers (this becomes more critical for the work the outside consultant is doing vs. the Profit Report)
•
Step three may be just getting a rudimentary report devolved to show high level information because the next step will be the most involved to get right…
•
Step four would be building in the math/logic of how to assign costs/revenues beyond just the ones that are tied to the employee #/name (class code from QuickBooks).
•
Definition of the quarter (calendar)
•
How to handle “Company” and “Zone” specific expenses (how they get assigned to the individual techs)
•
Methodology to apply role adjustments.  This means some techs would take on more of the impact of expenses… an advanced HVAC tech may get 120% of the impact of expense while a handyman may only take on 80% -
this makes things fairer overall.  This would be assigned by Eric or me and would be relatively static.  Overall, each zone would average 100% over all its techs.
•
How to calculate a profitability score
•
Rules behind what equals an A-F score based on that profitability score.
•
Step five would be rolling up all this data into Zone-by-Zone
ADMIN  summary reports and an overall company report.  Today, this is what the zone version looks like…
•
Step six would be to get a very high-level summary (maybe just the score) available for the ZFM and the
techs .  This step would also include getting data from the Payroll Worksheets summarized and built in.
Finally, we need to work together to ensure that the trip charge is properly being applied so that the Utilization number includes it.  Currently, I know in one report it is not include and I am unsure on another.  I need to confirm the “Tech Activity” Report
has this built in.
From that report (this is live data - he is at 57%):
From the Tech Performance Dashboard (this is static data - he is at 51.2%):
This is something I upload - I take the Payroll worksheet (each week) and dump into an Excel spreadsheet.  THIS DOES NOT include trip charges due to an issue we were having with its calculation a while back - the older it is in the payroll worksheet, it miscalculates
in EvoTrakker.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'QB-DASH' AND bi_title = N'Profit Report Lookup Tables')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'QB-DASH',
        N'Profit Report Lookup Tables',
        N'Need another place... dashboards.eml',
        N'Daniel Stone',
        '2026-03-03',
        N'Reporting',
        N'Requirement',
        N'Medium',
        N'Open',
        N'M',
        N'Step 2 — Lookup tables: confirm EvoTrakker has Employee Number/Name (QB class code), Zone Assignments, and WO# linkage to trades and customers in place as data keys before building report logic.',
        NULL,
        N'Need another place to bring in outside data into EvoTrakker... more
dashboards
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateTue, 3 Mar 2026 14:39:27 +0000
ToChris Collins <chris.collins@outlook.com>
Short story  - we are going to need to build more dashboards into EvoTrakker.  We are currently working with an outside consultant (Jim) on what some of those metrics are going to be (more to come on that).  At the same time, I want to integrate an outside
report I currently run (the Profit Report) into EvoTrakker.  The more I talk to Jim, pulling in the data for the Profit Report will probably cover most of the metrics he will suggest, and it accomplishes something I need as well.
Longer story  - today I currently pull four custom reports out of QuickBooks (examples attached - I copy and dump the data as is for three of these, one must be slightly modified).  Two on expenses (Cost of Goods Sold, All other Expenses) and two on revenue
(Total Current Year Sales, Sales by Employee Number).   With the Current Year Sales report from QuickBooks, I remove the accounts receivable portion or it double counts things.
•
Step one of EvoTrakker integration would be getting a home for these to be uploaded (similar to how I upload the Heartland time off data or the Enterprise maintenance data).
•
Step two would be to ensure that the necessary lookup “tables” or data points are also in EvoTrakker (most are already there). These would include:
•
Employee Number / Name (called class code in QuickBooks)
•
Zone Assignments
•
Tying the data back (through WO#) to trades and customers (this becomes more critical for the work the outside consultant is doing vs. the Profit Report)
•
Step three may be just getting a rudimentary report devolved to show high level information because the next step will be the most involved to get right…
•
Step four would be building in the math/logic of how to assign costs/revenues beyond just the ones that are tied to the employee #/name (class code from QuickBooks).
•
Definition of the quarter (calendar)
•
How to handle “Company” and “Zone” specific expenses (how they get assigned to the individual techs)
•
Methodology to apply role adjustments.  This means some techs would take on more of the impact of expenses… an advanced HVAC tech may get 120% of the impact of expense while a handyman may only take on 80% -
this makes things fairer overall.  This would be assigned by Eric or me and would be relatively static.  Overall, each zone would average 100% over all its techs.
•
How to calculate a profitability score
•
Rules behind what equals an A-F score based on that profitability score.
•
Step five would be rolling up all this data into Zone-by-Zone
ADMIN  summary reports and an overall company report.  Today, this is what the zone version looks like…
•
Step six would be to get a very high-level summary (maybe just the score) available for the ZFM and the
techs .  This step would also include getting data from the Payroll Worksheets summarized and built in.
Finally, we need to work together to ensure that the trip charge is properly being applied so that the Utilization number includes it.  Currently, I know in one report it is not include and I am unsure on another.  I need to confirm the “Tech Activity” Report
has this built in.
From that report (this is live data - he is at 57%):
From the Tech Performance Dashboard (this is static data - he is at 51.2%):
This is something I upload - I take the Payroll worksheet (each week) and dump into an Excel spreadsheet.  THIS DOES NOT include trip charges due to an issue we were having with its calculation a while back - the older it is in the payroll worksheet, it miscalculates
in EvoTrakker.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'QB-DASH' AND bi_title = N'Cost Allocation Methodology')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'QB-DASH',
        N'Cost Allocation Methodology',
        N'Need another place... dashboards.eml',
        N'Daniel Stone',
        '2026-03-03',
        N'Reporting',
        N'Requirement',
        N'Medium',
        N'Open',
        N'M',
        N'Steps 3–4 — Cost/revenue methodology: define how company-level and zone-level expenses are allocated to individual techs, including role adjustment percentages (e.g. advanced HVAC tech = 120% of expense impact, handyman = 80%; each zone averages 100% across its techs). Assigned by Eric/Daniel, relatively static. Define profitability score calculation and A–F grading rules.',
        NULL,
        N'Need another place to bring in outside data into EvoTrakker... more
dashboards
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateTue, 3 Mar 2026 14:39:27 +0000
ToChris Collins <chris.collins@outlook.com>
Short story  - we are going to need to build more dashboards into EvoTrakker.  We are currently working with an outside consultant (Jim) on what some of those metrics are going to be (more to come on that).  At the same time, I want to integrate an outside
report I currently run (the Profit Report) into EvoTrakker.  The more I talk to Jim, pulling in the data for the Profit Report will probably cover most of the metrics he will suggest, and it accomplishes something I need as well.
Longer story  - today I currently pull four custom reports out of QuickBooks (examples attached - I copy and dump the data as is for three of these, one must be slightly modified).  Two on expenses (Cost of Goods Sold, All other Expenses) and two on revenue
(Total Current Year Sales, Sales by Employee Number).   With the Current Year Sales report from QuickBooks, I remove the accounts receivable portion or it double counts things.
•
Step one of EvoTrakker integration would be getting a home for these to be uploaded (similar to how I upload the Heartland time off data or the Enterprise maintenance data).
•
Step two would be to ensure that the necessary lookup “tables” or data points are also in EvoTrakker (most are already there). These would include:
•
Employee Number / Name (called class code in QuickBooks)
•
Zone Assignments
•
Tying the data back (through WO#) to trades and customers (this becomes more critical for the work the outside consultant is doing vs. the Profit Report)
•
Step three may be just getting a rudimentary report devolved to show high level information because the next step will be the most involved to get right…
•
Step four would be building in the math/logic of how to assign costs/revenues beyond just the ones that are tied to the employee #/name (class code from QuickBooks).
•
Definition of the quarter (calendar)
•
How to handle “Company” and “Zone” specific expenses (how they get assigned to the individual techs)
•
Methodology to apply role adjustments.  This means some techs would take on more of the impact of expenses… an advanced HVAC tech may get 120% of the impact of expense while a handyman may only take on 80% -
this makes things fairer overall.  This would be assigned by Eric or me and would be relatively static.  Overall, each zone would average 100% over all its techs.
•
How to calculate a profitability score
•
Rules behind what equals an A-F score based on that profitability score.
•
Step five would be rolling up all this data into Zone-by-Zone
ADMIN  summary reports and an overall company report.  Today, this is what the zone version looks like…
•
Step six would be to get a very high-level summary (maybe just the score) available for the ZFM and the
techs .  This step would also include getting data from the Payroll Worksheets summarized and built in.
Finally, we need to work together to ensure that the trip charge is properly being applied so that the Utilization number includes it.  Currently, I know in one report it is not include and I am unsure on another.  I need to confirm the “Tech Activity” Report
has this built in.
From that report (this is live data - he is at 57%):
From the Tech Performance Dashboard (this is static data - he is at 51.2%):
This is something I upload - I take the Payroll worksheet (each week) and dump into an Excel spreadsheet.  THIS DOES NOT include trip charges due to an issue we were having with its calculation a while back - the older it is in the payroll worksheet, it miscalculates
in EvoTrakker.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'QB-DASH' AND bi_title = N'Zone & Tech Report Outputs')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'QB-DASH',
        N'Zone & Tech Report Outputs',
        N'Need another place... dashboards.eml',
        N'Daniel Stone',
        '2026-03-03',
        N'Reporting',
        N'Feature',
        N'Low',
        N'Open',
        N'M',
        N'Steps 5–6 — Reporting outputs: zone-by-zone admin summary reports and an overall company report. Plus a simplified view (score only) available to Zone Field Managers and individual techs. Step 6 also includes rolling in summarized Payroll Worksheet data.',
        NULL,
        N'Need another place to bring in outside data into EvoTrakker... more
dashboards
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateTue, 3 Mar 2026 14:39:27 +0000
ToChris Collins <chris.collins@outlook.com>
Short story  - we are going to need to build more dashboards into EvoTrakker.  We are currently working with an outside consultant (Jim) on what some of those metrics are going to be (more to come on that).  At the same time, I want to integrate an outside
report I currently run (the Profit Report) into EvoTrakker.  The more I talk to Jim, pulling in the data for the Profit Report will probably cover most of the metrics he will suggest, and it accomplishes something I need as well.
Longer story  - today I currently pull four custom reports out of QuickBooks (examples attached - I copy and dump the data as is for three of these, one must be slightly modified).  Two on expenses (Cost of Goods Sold, All other Expenses) and two on revenue
(Total Current Year Sales, Sales by Employee Number).   With the Current Year Sales report from QuickBooks, I remove the accounts receivable portion or it double counts things.
•
Step one of EvoTrakker integration would be getting a home for these to be uploaded (similar to how I upload the Heartland time off data or the Enterprise maintenance data).
•
Step two would be to ensure that the necessary lookup “tables” or data points are also in EvoTrakker (most are already there). These would include:
•
Employee Number / Name (called class code in QuickBooks)
•
Zone Assignments
•
Tying the data back (through WO#) to trades and customers (this becomes more critical for the work the outside consultant is doing vs. the Profit Report)
•
Step three may be just getting a rudimentary report devolved to show high level information because the next step will be the most involved to get right…
•
Step four would be building in the math/logic of how to assign costs/revenues beyond just the ones that are tied to the employee #/name (class code from QuickBooks).
•
Definition of the quarter (calendar)
•
How to handle “Company” and “Zone” specific expenses (how they get assigned to the individual techs)
•
Methodology to apply role adjustments.  This means some techs would take on more of the impact of expenses… an advanced HVAC tech may get 120% of the impact of expense while a handyman may only take on 80% -
this makes things fairer overall.  This would be assigned by Eric or me and would be relatively static.  Overall, each zone would average 100% over all its techs.
•
How to calculate a profitability score
•
Rules behind what equals an A-F score based on that profitability score.
•
Step five would be rolling up all this data into Zone-by-Zone
ADMIN  summary reports and an overall company report.  Today, this is what the zone version looks like…
•
Step six would be to get a very high-level summary (maybe just the score) available for the ZFM and the
techs .  This step would also include getting data from the Payroll Worksheets summarized and built in.
Finally, we need to work together to ensure that the trip charge is properly being applied so that the Utilization number includes it.  Currently, I know in one report it is not include and I am unsure on another.  I need to confirm the “Tech Activity” Report
has this built in.
From that report (this is live data - he is at 57%):
From the Tech Performance Dashboard (this is static data - he is at 51.2%):
This is something I upload - I take the Payroll worksheet (each week) and dump into an Excel spreadsheet.  THIS DOES NOT include trip charges due to an issue we were having with its calculation a while back - the older it is in the payroll worksheet, it miscalculates
in EvoTrakker.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'QB-DASH' AND bi_title = N'Trip Charge Utilization Fix')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'QB-DASH',
        N'Trip Charge Utilization Fix',
        N'Need another place... dashboards.eml',
        N'Daniel Stone',
        '2026-03-03',
        N'Reporting',
        N'Bug',
        N'High',
        N'Open',
        N'M',
        N'Utilization numbers in the Profit Report are miscalculated because trip charges are not being correctly factored in. Fix the calculation logic so Utilization properly includes trip charges.',
        NULL,
        N'Need another place to bring in outside data into EvoTrakker... more
dashboards
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateTue, 3 Mar 2026 14:39:27 +0000
ToChris Collins <chris.collins@outlook.com>
Short story  - we are going to need to build more dashboards into EvoTrakker.  We are currently working with an outside consultant (Jim) on what some of those metrics are going to be (more to come on that).  At the same time, I want to integrate an outside
report I currently run (the Profit Report) into EvoTrakker.  The more I talk to Jim, pulling in the data for the Profit Report will probably cover most of the metrics he will suggest, and it accomplishes something I need as well.
Longer story  - today I currently pull four custom reports out of QuickBooks (examples attached - I copy and dump the data as is for three of these, one must be slightly modified).  Two on expenses (Cost of Goods Sold, All other Expenses) and two on revenue
(Total Current Year Sales, Sales by Employee Number).   With the Current Year Sales report from QuickBooks, I remove the accounts receivable portion or it double counts things.
•
Step one of EvoTrakker integration would be getting a home for these to be uploaded (similar to how I upload the Heartland time off data or the Enterprise maintenance data).
•
Step two would be to ensure that the necessary lookup “tables” or data points are also in EvoTrakker (most are already there). These would include:
•
Employee Number / Name (called class code in QuickBooks)
•
Zone Assignments
•
Tying the data back (through WO#) to trades and customers (this becomes more critical for the work the outside consultant is doing vs. the Profit Report)
•
Step three may be just getting a rudimentary report devolved to show high level information because the next step will be the most involved to get right…
•
Step four would be building in the math/logic of how to assign costs/revenues beyond just the ones that are tied to the employee #/name (class code from QuickBooks).
•
Definition of the quarter (calendar)
•
How to handle “Company” and “Zone” specific expenses (how they get assigned to the individual techs)
•
Methodology to apply role adjustments.  This means some techs would take on more of the impact of expenses… an advanced HVAC tech may get 120% of the impact of expense while a handyman may only take on 80% -
this makes things fairer overall.  This would be assigned by Eric or me and would be relatively static.  Overall, each zone would average 100% over all its techs.
•
How to calculate a profitability score
•
Rules behind what equals an A-F score based on that profitability score.
•
Step five would be rolling up all this data into Zone-by-Zone
ADMIN  summary reports and an overall company report.  Today, this is what the zone version looks like…
•
Step six would be to get a very high-level summary (maybe just the score) available for the ZFM and the
techs .  This step would also include getting data from the Payroll Worksheets summarized and built in.
Finally, we need to work together to ensure that the trip charge is properly being applied so that the Utilization number includes it.  Currently, I know in one report it is not include and I am unsure on another.  I need to confirm the “Tech Activity” Report
has this built in.
From that report (this is live data - he is at 57%):
From the Tech Performance Dashboard (this is static data - he is at 51.2%):
This is something I upload - I take the Payroll worksheet (each week) and dump into an Excel spreadsheet.  THIS DOES NOT include trip charges due to an issue we were having with its calculation a while back - the older it is in the payroll worksheet, it miscalculates
in EvoTrakker.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'AZURE-CERT' AND bi_title = N'Validate App Service Domain')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'AZURE-CERT',
        N'Validate App Service Domain',
        N'Action required Validate domain ownership for Azure App Service certificates.eml',
        N'Microsoft Azure',
        '2026-02-20',
        N'Infrastructure',
        N'Requirement',
        N'Medium',
        N'Open',
        N'M',
        N'Validate domain ownership for Azure App Service SSL certificates to prevent certificate expiration. Follow the Azure-provided steps to re-verify the domain.',
        NULL,
        N'Action required: Validate domain ownership for Azure App Service
certificates
From"Microsoft Azure" <azure-noreply@microsoft.com>
Date20 Feb 2026 02:03:55 +0000
Tochris.collins@outlook.com
Domain ownership validation is required for certificate issuance and renewal starting 1 March 2026.
Upcoming changes to Azure App Service certificates
You&#x27;re receiving this notification because you have one or more Azure App Service certificates in your Azure subscription.
As part of  industry-wide certificate compliance updates  and an update to the certificate issuance platform, Azure App Service certificates are changing how certificates are issued and renewed. These updates align with new industry standards that reduce certificate validity periods and the maximum time that domain validation information can be reused.
Industry standards now limit both certificate validity and domain validation reuse to a maximum of 200 days  .  To comply with these standards, GoDaddy is implementing a 198-day validity period .
Starting 1 March 2026 :
Certificate validity
Certificates issued under the new standards have a 198-day validity period. To maintain a full year of certificate billing coverage, Azure App Service will automatically issue certificates during renewal at no additional cost to customers, subject to domain ownership validation requirements.
For additional details, refer to  our documentation .
Domain validation reuse
Industry standards are decreasing the domain ownership validation reuse period. App Service certificates don&#x27;t have automated domain re-validation. If domain ownership validation is required, certificate orders remain in pending issuance until validation is completed. Failure to complete domain validation will result in failed issuance or renewal, which could cause service downtime. For more information, please see  Domain validation reuse .
Required action
Complete domain ownership validation when prompted during the renewal or auto-renewal process.
If  domain ownership validation isn&#x27;t completed,  certificate issuance or renewal will remain in pending issuance and may fail, which could result in certificate expiration and service downtime.
Help and support
If you have questions, get answers from community experts in  Microsoft Q&A . If you have a support plan and need technical help, submit a  support request .
Links provided herein may take you to a third-party website and are provided for convenience only. Third-party websites are subject to the third-party&#x27;s terms and privacy statements.
Account Information
Subscription ID:
E1898250-B599-449F-93E5-A8F82DA52853
Subscription name:
Pay-As-You-Go
Please help us improve our communication by telling us what you think about this email in a  survey .
This message from Microsoft is an important part of a program, service, or product that you or your company purchased or participates in. Microsoft respects your privacy. Please read our
Privacy Statement .
This is a mandatory service communication. To set your contact preferences for other communications, visit the
Promotional Communications Manager .
Microsoft Corporation,  One Microsoft Way, Redmond, WA 98052',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'KEY-VAULT' AND bi_title = N'Azure Key Vault RBAC Migration')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'KEY-VAULT',
        N'Azure Key Vault RBAC Migration',
        N'FW Action required Transition Azure Key Vault access policies to Azure RBAC or configure Azure Key Vault to explicitly use access policies.eml',
        N'Microsoft Azure',
        '2026-02-05',
        N'Security',
        N'Requirement',
        N'Medium',
        N'Open',
        N'M',
        N'Migrate Azure Key Vault from legacy access policies to Azure RBAC before transitioning to API version 2026-02-01 — OR explicitly configure new vaults to continue using legacy access policies. Deadline: all API versions prior to 2026-02-01 retire on February 27, 2027. Risk: failure to act will cause HTTP 403 errors and operational failures for any code or pipeline that creates new Key Vaults.',
        NULL,
        N'FW: Action required: Transition Azure Key Vault access policies to
Azure RBAC or configure Azure Key Vault to explicitly use access policies
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateThu, 5 Feb 2026 23:15:46 +0000
To"chris.collins@outlook.com" <chris.collins@outlook.com>
You probably got this already
From:  Microsoft Azure <azure-noreply@microsoft.com>
Date:  Thursday, February 5, 2026 at 5:14 PM
To:  Daniel Stone <daniel@evolutionmaintenance.com>
Subject:  Action required: Transition Azure Key Vault access policies to Azure RBAC or configure Azure Key Vault to explicitly use access policies
Transition to Azure RBAC
You’re receiving this email because you’re using Azure Key Vault.
On 27 February 2027, all Azure Key Vault API versions prior to 2026-02-01 will be retired.
Azure Key Vault API version 2026-02-01  —releasing
in February 2026—   introduces
an important security update   :
Azure role-based access control (RBAC) will be the default access control
model for all newly created vaults.   Existing key vaults will continue using their current access control model. Azure portal behavior will remain unchanged.
If you’re using legacy access policies for new and existing vaults,   we recommend
migrating
to Azure RBAC    before transitioning to API version 2026-02-01  .
To learn why Azure RBAC is critical to security, read our    blog   .
If you want to continue using legacy access policies for new key vault creation after transitioning to API version 2026-02-01, you&#x27;ll need to
explicitly
configure access policies    as the access control model in your CLI, PowerShell, Rest API, ARM, Bicep, and Terraform templates. If you don’t take this action, all newly created vaults will be created with Azure
RBAC as the default access control model, which can result in HTTP 403 errors and failures in your code and operations due to missing roles.
Required action
Migrate
new and existing vaults to Azure RBAC    before transitioning to API version
2026-02-01 or    explicitly
configure new vaults to use legacy access policies   .
You’ll need to   transition to API version 2026-02-01 before 27 February 2027  ,
when all prior APIs will be retired.
For additional guidance on securing your Azure Key Vault deployments, refer to our
documentation   .
Help and support
If you have questions, get answers from community experts in
Microsoft
Q&A   . If you have a support plan and you need technical help, create a
support
request   .
Privacy
Statement
Microsoft Corporation, One Microsoft Way, Redmond, WA 98052',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'GPS-EVAL' AND bi_title = N'Geotab GPS Provider Evaluation')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'GPS-EVAL',
        N'Geotab GPS Provider Evaluation',
        N'New GPS provider....eml',
        N'Daniel Stone',
        '2026-02-13',
        N'Infrastructure',
        N'Feature',
        N'Medium',
        N'Open',
        N'M',
        N'Evaluate GPS provider replacements for Verizon Connect/Reveal. Primary candidates: Geotab (plug-in OBDII device, no hardware installation, API research needed — is it the same structure as Reveal?) and Telemadix (also no hardware). Longer-term item, after high priority work is complete.',
        NULL,
        N'New GPS provider...
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateFri, 13 Feb 2026 18:03:02 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
Chris,
I may be about to start the switch with our GPS provider.  Geotab ties into the ODBII system in newer vehicles and requires no hardware installation.  And I put it on Adam’s vehicle and it seems to be working.  This is a game changer!  I am still kicking the
tires but if this works, over time, I will switch everything over.
When you get a chance, can you look at their API capabilities and see if they will give us what we need? Thanks!
https://www.geotab.com/blog/better-practices-mygeotab-api/
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'WO-NOTES' AND bi_title = N'WO Notes Field Always Visible')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'WO-NOTES',
        N'WO Notes Field Always Visible',
        N'Notes box not showing unless in office needs to order parts still.eml',
        N'Karl Brightman',
        '2026-02-10',
        N'Work Orders',
        N'Bug',
        N'High',
        N'Open',
        N'M',
        N'The notes field on a work order is only visible when the WO status is "Office Needs to Order Parts". It should be visible regardless of status. Reported on WO# 174794.',
        NULL,
        N'Notes box not showing unless in office needs to order parts still
FromKarl Brightman <kbrightman@evolutionmaintenance.com>
DateTue, 10 Feb 2026 23:21:06 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
Daniel,
Below are examples of what we just discussed for the notes box not showing up unless the status is office needs to order parts. The supplier and tracking information show up now, which it didn&#x27;t before, but not the notes box.
This was taken from WO#  174794,  and I can repeat this in other work orders.
Thanks,
Karl Brightman
Evolution Maintenance, Inc.
105 Flex Ave
Portland, TN 37148
Office: 615-649-0622
Direct: 615-306-6463',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'PAYROLL' AND bi_title = N'Guaranteed Hours User Field')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'PAYROLL',
        N'Guaranteed Hours User Field',
        N'Payroll Worksheet changes needed (sooner than later).eml',
        N'Daniel Stone',
        '2026-03-04',
        N'Payroll',
        N'Feature',
        N'Medium',
        N'Open',
        N'M',
        N'Add a "Guaranteed Hours" attribute to the User table (default 32 for techs, 40 for office; editable to values like 40 or 45). Rename the Payroll Worksheet label "Increase to 32" → "Increase to Guarantee" and have it pull the user''s Guaranteed Hours value dynamically. Requires bulk data export/re-upload for existing records. Employee Number should also be added to ensure data integrity (and as a future Heartland data key).',
        NULL,
        N'Payroll Worksheet changes needed (sooner than later)
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateWed, 4 Mar 2026 00:01:39 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
Chris,
As mentioned, we are working to rapidly improve multiple (operational) areas of the company and will need some EvoTrakker modifications to assist with these improvements.
One area we have discovered we are getting off on is the Payroll Worksheet.  Misunderstandings and Anggie having to work around things we rolled out for other purposes are causing… bad data.  For example, the time actually clocked in has been off for people
for months now - nothing the system is doing wrong but changes being made outside the system.   I need your help to make the changes below so we can get this as accurate as possible.
Some changes will be more invasive than others - let’s start with a couple of those.
Increase to 32
We need to add an attribute under the User screen - Guaranteed hours.  The default for techs would be 32 (for the office 40).  We would be able to modify (40/45 are examples).
Then in the payroll worksheet, “Increase to 32” would need to be changed to "Increase to Guarantee”.  Instead of this being a flat 32, it would then need to go to the user table and reference the name (probably need to add employee number here to ensure data
integrity) and pull back the amount in that attribute.
PTO/Vacation Request
Monday after the previous week ends, we need an ability for the tech to, on Monday mornings only, go into their view of the Payroll Worksheet and if submitted prior to 11AM central, they can supplement their worked hours by taking available PTO/Vacation time.
Currently, once the week is over, they cannot add to the previous week (which makes complete sense).  I would want this clear that it is something like “Supplemental Time Request” (could be -PTO or -Vacation).  We would communicate to the field that the days
of Anggie calling them and asking are over - if they do not do it within the window, they are out of luck - period.
Employee Number
We need this added to the report.  It is in Heartland too so it can be a future data key if we can get the systems to talk.
Anggie’s requests are below.  Basically, I have challenged her to quit exporting this to Excel and doing the work in Excel.  I asked her what it would take to make this so that ALL work is done in EvoTrakker.  My goal is to get it so that we click Export, Eric
can (in time) upload this into our payroll system.  Today, Anggie exports it and then makes tons of changes and then sends to Eric.  He then may have her fix things (she does this in her Excel file).  Thus making EvoTrakker inaccurate.  Not good.
Anggie’s Requests
•
Add, Subtract or Swap PTO and VAC
•
Add back in the commute if needed (right now not an issues but in the future could be again)
•
Remove OT when bereavement hours are applied
•
(Salary employees - will need more work than this too…) Time on Salary employees nothing populates now I add to the spread sheet but if it does populate in the future the ability to adjust if they take a day
off without pay
•
Kind of a wish list thing - if in the future they have not hit 32 hours maybe Adjustable payable hours block might show red and if not 40 maybe show yellow. Reason I ask for this is When it move over to excel
I format the cells to show me which ones have an issue - just looking at in the system is easy to miss one',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'PAYROLL' AND bi_title = N'Employee Number on Report')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'PAYROLL',
        N'Employee Number on Report',
        N'Payroll Worksheet changes needed (sooner than later).eml',
        N'Daniel Stone',
        '2026-03-04',
        N'Payroll',
        N'Feature',
        N'High',
        N'Open',
        N'M',
        N'Add Employee Number field to the Payroll Worksheet report output. This will serve as a future data key to tie EvoTrakker data to Heartland payroll if the systems are integrated.',
        NULL,
        N'Payroll Worksheet changes needed (sooner than later)
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateWed, 4 Mar 2026 00:01:39 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
Chris,
As mentioned, we are working to rapidly improve multiple (operational) areas of the company and will need some EvoTrakker modifications to assist with these improvements.
One area we have discovered we are getting off on is the Payroll Worksheet.  Misunderstandings and Anggie having to work around things we rolled out for other purposes are causing… bad data.  For example, the time actually clocked in has been off for people
for months now - nothing the system is doing wrong but changes being made outside the system.   I need your help to make the changes below so we can get this as accurate as possible.
Some changes will be more invasive than others - let’s start with a couple of those.
Increase to 32
We need to add an attribute under the User screen - Guaranteed hours.  The default for techs would be 32 (for the office 40).  We would be able to modify (40/45 are examples).
Then in the payroll worksheet, “Increase to 32” would need to be changed to "Increase to Guarantee”.  Instead of this being a flat 32, it would then need to go to the user table and reference the name (probably need to add employee number here to ensure data
integrity) and pull back the amount in that attribute.
PTO/Vacation Request
Monday after the previous week ends, we need an ability for the tech to, on Monday mornings only, go into their view of the Payroll Worksheet and if submitted prior to 11AM central, they can supplement their worked hours by taking available PTO/Vacation time.
Currently, once the week is over, they cannot add to the previous week (which makes complete sense).  I would want this clear that it is something like “Supplemental Time Request” (could be -PTO or -Vacation).  We would communicate to the field that the days
of Anggie calling them and asking are over - if they do not do it within the window, they are out of luck - period.
Employee Number
We need this added to the report.  It is in Heartland too so it can be a future data key if we can get the systems to talk.
Anggie’s requests are below.  Basically, I have challenged her to quit exporting this to Excel and doing the work in Excel.  I asked her what it would take to make this so that ALL work is done in EvoTrakker.  My goal is to get it so that we click Export, Eric
can (in time) upload this into our payroll system.  Today, Anggie exports it and then makes tons of changes and then sends to Eric.  He then may have her fix things (she does this in her Excel file).  Thus making EvoTrakker inaccurate.  Not good.
Anggie’s Requests
•
Add, Subtract or Swap PTO and VAC
•
Add back in the commute if needed (right now not an issues but in the future could be again)
•
Remove OT when bereavement hours are applied
•
(Salary employees - will need more work than this too…) Time on Salary employees nothing populates now I add to the spread sheet but if it does populate in the future the ability to adjust if they take a day
off without pay
•
Kind of a wish list thing - if in the future they have not hit 32 hours maybe Adjustable payable hours block might show red and if not 40 maybe show yellow. Reason I ask for this is When it move over to excel
I format the cells to show me which ones have an issue - just looking at in the system is easy to miss one',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'PAYROLL' AND bi_title = N'Worksheet PTO & Leave Adjustments')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'PAYROLL',
        N'Worksheet PTO & Leave Adjustments',
        N'Payroll Worksheet changes needed (sooner than later).eml',
        N'Daniel Stone',
        '2026-03-04',
        N'Payroll',
        N'Feature',
        N'High',
        N'Open',
        N'M',
        N'Anggie''s worksheet requests (goal: eliminate all post-export Excel editing so EvoTrakker export goes directly to Eric for payroll upload): (1) Add, Subtract, or Swap PTO and Vacation hours; (2) Add back commute time when needed; (3) Remove OT when bereavement hours are applied; (4) Salary employee handling — time currently does not populate, need ability to adjust if they take a day off without pay.',
        NULL,
        N'Payroll Worksheet changes needed (sooner than later)
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateWed, 4 Mar 2026 00:01:39 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
Chris,
As mentioned, we are working to rapidly improve multiple (operational) areas of the company and will need some EvoTrakker modifications to assist with these improvements.
One area we have discovered we are getting off on is the Payroll Worksheet.  Misunderstandings and Anggie having to work around things we rolled out for other purposes are causing… bad data.  For example, the time actually clocked in has been off for people
for months now - nothing the system is doing wrong but changes being made outside the system.   I need your help to make the changes below so we can get this as accurate as possible.
Some changes will be more invasive than others - let’s start with a couple of those.
Increase to 32
We need to add an attribute under the User screen - Guaranteed hours.  The default for techs would be 32 (for the office 40).  We would be able to modify (40/45 are examples).
Then in the payroll worksheet, “Increase to 32” would need to be changed to "Increase to Guarantee”.  Instead of this being a flat 32, it would then need to go to the user table and reference the name (probably need to add employee number here to ensure data
integrity) and pull back the amount in that attribute.
PTO/Vacation Request
Monday after the previous week ends, we need an ability for the tech to, on Monday mornings only, go into their view of the Payroll Worksheet and if submitted prior to 11AM central, they can supplement their worked hours by taking available PTO/Vacation time.
Currently, once the week is over, they cannot add to the previous week (which makes complete sense).  I would want this clear that it is something like “Supplemental Time Request” (could be -PTO or -Vacation).  We would communicate to the field that the days
of Anggie calling them and asking are over - if they do not do it within the window, they are out of luck - period.
Employee Number
We need this added to the report.  It is in Heartland too so it can be a future data key if we can get the systems to talk.
Anggie’s requests are below.  Basically, I have challenged her to quit exporting this to Excel and doing the work in Excel.  I asked her what it would take to make this so that ALL work is done in EvoTrakker.  My goal is to get it so that we click Export, Eric
can (in time) upload this into our payroll system.  Today, Anggie exports it and then makes tons of changes and then sends to Eric.  He then may have her fix things (she does this in her Excel file).  Thus making EvoTrakker inaccurate.  Not good.
Anggie’s Requests
•
Add, Subtract or Swap PTO and VAC
•
Add back in the commute if needed (right now not an issues but in the future could be again)
•
Remove OT when bereavement hours are applied
•
(Salary employees - will need more work than this too…) Time on Salary employees nothing populates now I add to the spread sheet but if it does populate in the future the ability to adjust if they take a day
off without pay
•
Kind of a wish list thing - if in the future they have not hit 32 hours maybe Adjustable payable hours block might show red and if not 40 maybe show yellow. Reason I ask for this is When it move over to excel
I format the cells to show me which ones have an issue - just looking at in the system is easy to miss one',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'PAYROLL' AND bi_title = N'Direct Payroll Export Workflow')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'PAYROLL',
        N'Direct Payroll Export Workflow',
        N'Payroll Worksheet changes needed (sooner than later).eml',
        N'Daniel Stone',
        '2026-03-04',
        N'Payroll',
        N'Requirement',
        N'Low',
        N'Open',
        N'M',
        N'Overall export workflow goal: all Payroll Worksheet work should be completable inside EvoTrakker so that Export → Eric''s payroll upload is a one-step process. Currently Anggie exports to Excel, manually edits extensively, and sends to Eric — making EvoTrakker data inaccurate.',
        NULL,
        N'Payroll Worksheet changes needed (sooner than later)
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateWed, 4 Mar 2026 00:01:39 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
Chris,
As mentioned, we are working to rapidly improve multiple (operational) areas of the company and will need some EvoTrakker modifications to assist with these improvements.
One area we have discovered we are getting off on is the Payroll Worksheet.  Misunderstandings and Anggie having to work around things we rolled out for other purposes are causing… bad data.  For example, the time actually clocked in has been off for people
for months now - nothing the system is doing wrong but changes being made outside the system.   I need your help to make the changes below so we can get this as accurate as possible.
Some changes will be more invasive than others - let’s start with a couple of those.
Increase to 32
We need to add an attribute under the User screen - Guaranteed hours.  The default for techs would be 32 (for the office 40).  We would be able to modify (40/45 are examples).
Then in the payroll worksheet, “Increase to 32” would need to be changed to "Increase to Guarantee”.  Instead of this being a flat 32, it would then need to go to the user table and reference the name (probably need to add employee number here to ensure data
integrity) and pull back the amount in that attribute.
PTO/Vacation Request
Monday after the previous week ends, we need an ability for the tech to, on Monday mornings only, go into their view of the Payroll Worksheet and if submitted prior to 11AM central, they can supplement their worked hours by taking available PTO/Vacation time.
Currently, once the week is over, they cannot add to the previous week (which makes complete sense).  I would want this clear that it is something like “Supplemental Time Request” (could be -PTO or -Vacation).  We would communicate to the field that the days
of Anggie calling them and asking are over - if they do not do it within the window, they are out of luck - period.
Employee Number
We need this added to the report.  It is in Heartland too so it can be a future data key if we can get the systems to talk.
Anggie’s requests are below.  Basically, I have challenged her to quit exporting this to Excel and doing the work in Excel.  I asked her what it would take to make this so that ALL work is done in EvoTrakker.  My goal is to get it so that we click Export, Eric
can (in time) upload this into our payroll system.  Today, Anggie exports it and then makes tons of changes and then sends to Eric.  He then may have her fix things (she does this in her Excel file).  Thus making EvoTrakker inaccurate.  Not good.
Anggie’s Requests
•
Add, Subtract or Swap PTO and VAC
•
Add back in the commute if needed (right now not an issues but in the future could be again)
•
Remove OT when bereavement hours are applied
•
(Salary employees - will need more work than this too…) Time on Salary employees nothing populates now I add to the spread sheet but if it does populate in the future the ability to adjust if they take a day
off without pay
•
Kind of a wish list thing - if in the future they have not hit 32 hours maybe Adjustable payable hours block might show red and if not 40 maybe show yellow. Reason I ask for this is When it move over to excel
I format the cells to show me which ones have an issue - just looking at in the system is easy to miss one',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'CALL-CTR' AND bi_title = N'Non-Admin Call Center Access')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'CALL-CTR',
        N'Non-Admin Call Center Access',
        N'Re Call Center view....eml',
        N'Daniel Stone',
        '2026-02-04',
        N'Call Center',
        N'Bug',
        N'High',
        N'Open',
        N'M',
        N'Non-admin users (e.g. Heather, Leandrea) are redirected to the Home Screen when they click on Call Centers. They should be able to view call center portal information.',
        NULL,
        N'Re: Call Center view...
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateWed, 4 Feb 2026 19:45:50 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
⚠ Content partially recovered — the original message may contain additional formatting or attachments not shown here. Refer to the original .eml file for full content.
I also need feeding into this report, those companies (under Direct Commercial for example) info about their portals.  This would require that same type of portal gathering info to be added to companies.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148
From:  Daniel Stone <daniel@evolutionmaintenance.com>
Date:  Wednesday, February 4, 2026 at 1:43 PM
To:  evotrakker@evolutionmaintenance.com <evotrakker@evolutionmaintenance.com>
Subject:  Call Center view...
To add portal info to a Call center.  Right now this exists and I see:
I want others to be able to access/edit at the portal information here.  NOT THE ATTACK POINTS.
When I log in as Heather or Leandrea and click on the  call centers...
It goes back to the “Home Screen”
On a side note, I  need a report that is Call Center portal information that pulls from here and can be seen by all admins.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'CALL-CTR' AND bi_title = N'Call Center Portal Info Report')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'CALL-CTR',
        N'Call Center Portal Info Report',
        N'Re Call Center view....eml',
        N'Daniel Stone',
        '2026-02-04',
        N'Call Center',
        N'Feature',
        N'High',
        N'Open',
        N'M',
        N'Allow all admins (not just Daniel) to access and edit Call Center portal information. Separately, add the same portal info fields to Companies (for Direct Commercial companies, etc.) and build an admin-visible report showing Call Center portal information pulled from those records.',
        NULL,
        N'Re: Call Center view...
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateWed, 4 Feb 2026 19:45:50 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
⚠ Content partially recovered — the original message may contain additional formatting or attachments not shown here. Refer to the original .eml file for full content.
I also need feeding into this report, those companies (under Direct Commercial for example) info about their portals.  This would require that same type of portal gathering info to be added to companies.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148
From:  Daniel Stone <daniel@evolutionmaintenance.com>
Date:  Wednesday, February 4, 2026 at 1:43 PM
To:  evotrakker@evolutionmaintenance.com <evotrakker@evolutionmaintenance.com>
Subject:  Call Center view...
To add portal info to a Call center.  Right now this exists and I see:
I want others to be able to access/edit at the portal information here.  NOT THE ATTACK POINTS.
When I log in as Heather or Leandrea and click on the  call centers...
It goes back to the “Home Screen”
On a side note, I  need a report that is Call Center portal information that pulls from here and can be seen by all admins.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'RECEIPTS' AND bi_title = N'PDF Re-upload Receipt Bug')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'RECEIPTS',
        N'PDF Re-upload Receipt Bug',
        N'Receipts bug.eml',
        N'Daniel Stone',
        '2026-03-03',
        N'Receipts',
        N'Bug',
        N'Medium',
        N'Open',
        N'M',
        N'Re-uploading a second PDF receipt fails with an error unless the user fully exits and re-enters the receipts section. Additionally, a false "must enter description" validation error shows even when a description has been entered.',
        NULL,
        N'Receipts bug
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateTue, 3 Mar 2026 14:54:04 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
Very specific bug related to (I assume) uploading PDF files.
If you try to upload a PDF recent   after you upload one  , it errors.  You must get completely out of the receipts portion and get back in or (I assume) it keeps something in memory and will not load.
Yes, I am choosing a different file, but it does not seem to accept that.  Error message says you must enter a description even when I have words written in the box.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'LOGOUT' AND bi_title = N'Auto-Save on Session Expiry')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'LOGOUT',
        N'Auto-Save on Session Expiry',
        N'Seeminly random logout_ .eml',
        N'Karl Brightman',
        '2026-02-23',
        N'Authentication',
        N'Bug',
        N'Medium',
        N'Open',
        N'M',
        N'Users are being logged out after ~8 hours, including mid-session while actively building quotes (reported: Mike). Request to auto-save any in-progress changes when a session expires to prevent data loss.',
        NULL,
        N'Seeminly random logout?
FromKarl Brightman <kbrightman@evolutionmaintenance.com>
DateMon, 23 Feb 2026 22:49:46 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
I spoke with Daniel earlier today after Mike was logged out in the middle of working on a quote. He finished typing his verbiage in and went to save and it immediately logged him out. This was around ~2:30pm. Assuming Mike got here around 6:30am, this would
be around the 8 hour mark. Daniel explained this is a new security feature where it previously logged everyone out after 4 hours, after negotiating was changed to 8 hours.
I&#x27;m not certain why, but this logout has never happened to me. I&#x27;m currently at 8 hours and ~45minutes logged in and it hasn&#x27;t kicked me out. Just some feedback that this change isn&#x27;t working for everyone?
Also, just a suggestion for those that it is impacting...
Implement a way for the system to save pending changes made in a ticket when it does the auto logout.
Thanks,
Karl Brightman
Evolution Maintenance, Inc.
105 Flex Ave
Portland, TN 37148
Office: 615-649-0622
Direct: 615-306-6463',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'SKILLS' AND bi_title = N'Skill Level Category Renumber')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'SKILLS',
        N'Skill Level Category Renumber',
        N'Skills change....eml',
        N'Daniel Stone',
        '2026-02-24',
        N'Configuration',
        N'Feature',
        N'High',
        N'Open',
        N'M',
        N'Restructure skill levels into a numbered category system (0–5): 0=Do Not Use / Administrative (open question: should 0 be separated from Admin, since 0 currently serves two purposes?), 1=Helper, 2=Emergency Only, 3=Intermediate (Basic), 4=Advanced (Repairs), 5=Expert (Large Projects). Requires a mass data export of current skill assignments and re-upload after renumbering.',
        NULL,
        N'Skills change...
FromDaniel Stone <daniel@evolutionmaintenance.com>
DateTue, 24 Feb 2026 20:51:17 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
I would like to add a category in Skills and change the numbers (push them up and add 5) to allow space for it.
0 - Do Not Use / Administrative (should we break this out too… could it be “Admin” vs. a number - don’t like 0 being two different things)
1 -  Helper
2 - Emergency Only
3 - Intermediate (Basic)
4 - Advanced (Repairs)
5 - Expert (Large Projects)
To make this work, would probably need to get the current info exported so I could change in mass some things and then re-upload.
Daniel Stone
Evolution Maintenance, Inc.
615-469-0268
105 Flex Ave
Portland,   TN
37148',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'EVINES-DEC' AND bi_title = N'Supplemental PTO Request Window')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'EVINES-DEC',
        N'Supplemental PTO Request Window',
        N'EVines follow ups from 12_10 meeting.eml',
        N'Eric Vines',
        '2025-12-11',
        N'Payroll',
        N'Feature',
        N'High',
        N'Open',
        N'M',
        N'Supplemental Time Request window: On Monday mornings only, techs should be able to open their Payroll Worksheet and supplement the prior week''s worked hours with PTO or Vacation time, provided the request is submitted before 11 AM Central. This replaces the manual process of Anggie calling techs to collect this info. Label the option something like "Supplemental Time Request" (sub-type: -PTO or -Vacation). Policy: if the tech misses the Monday window, they forfeit the opportunity — no exceptions.',
        NULL,
        N'EVines follow ups from 12/10 meeting
FromEric Vines <evines@evolutionmaintenance.com>
DateThu, 11 Dec 2025 22:25:12 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
⚠ The original email body references an attached Word document. The Word document content has been recovered and is included below.
Hey y''all. In the attached Word doc, I added a little detail to what I talked about on the call yesterday plus added one forward looking Evotrakker request.
Let me know any questions or if you want to talk about implementation.
Thank you,
Eric V
Attached Word Document — Recovered Content
1. Job Drive Time / Distance Traveled — added to each call + reportable
Goal: to eventually be able to tie a drive time and/or driving distance to each work order (and to each individual tech within a work order) that can be logged in the system and pulled in reporting after the fact. As we have determined that utilization rate is one of the major factors to tech success and profitability, we see that drive times are likely the overall most impactful variable impacting utilization.
If we can have the drive time associated with each call visible on the work order — and in the rears in reporting — everyone in the company (including the techs) can have the ability to see a number that they have some control over. Ideally, the distance would be calculated from the tech''s home to their first work order, from each work order to work order sequentially after their first call of the day, and then from the last work order of the day to their home again.
Having this information visible to techs and dispatch personnel will show a clear metric that they can impact. We can use this figure to gauge dispatch adjustments over time, to cross reference versus profitability and to potentially get buy in from personnel in the organization to make operational changes such as techs working (4) 10-hour schedules.
2. Certificates & Licensing — add quarterly ''continuing education'' requirement
As we look to keep developing training internally at Evolution, we are looking at adding a quarterly continuing education requirement for every tech (2 or 4 hours each quarter). The TYPE column, I believe, would be ''Continuing Education – Evolution'' and issuing authority would be ''Evolution''. I assume we would have the last date of each quarter as DATE EXPIRES.
We would need to talk through how that flips once the tech inputs their training hours for the current quarter, and how it would look when a tech is in the next quarter without completing requirement from previous quarter, etc.',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'EVINES-DEC' AND bi_title = N'Drive Time & Distance per Work Order')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'EVINES-DEC',
        N'Drive Time & Distance per Work Order',
        N'EVines follow ups from 12_10 meeting.eml',
        N'Eric Vines',
        '2025-12-11',
        N'Work Orders',
        N'Feature',
        N'Medium',
        N'Open',
        N'M',
        N'Log drive time and distance traveled for each work order (per tech). Calculate: home → first WO, WO → WO sequentially, last WO → home. Display on the work order and expose in reporting. Drive time is cited as the most impactful variable on utilization — making it visible to techs and dispatch gives both groups a metric they can directly influence.',
        NULL,
        N'EVines follow ups from 12/10 meeting
FromEric Vines <evines@evolutionmaintenance.com>
DateThu, 11 Dec 2025 22:25:12 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
⚠ The original email body references an attached Word document. The Word document content has been recovered and is included below.
Hey y''all. In the attached Word doc, I added a little detail to what I talked about on the call yesterday plus added one forward looking Evotrakker request.
Let me know any questions or if you want to talk about implementation.
Thank you,
Eric V
Attached Word Document — Recovered Content
1. Job Drive Time / Distance Traveled — added to each call + reportable
Goal: to eventually be able to tie a drive time and/or driving distance to each work order (and to each individual tech within a work order) that can be logged in the system and pulled in reporting after the fact. As we have determined that utilization rate is one of the major factors to tech success and profitability, we see that drive times are likely the overall most impactful variable impacting utilization.
If we can have the drive time associated with each call visible on the work order — and in the rears in reporting — everyone in the company (including the techs) can have the ability to see a number that they have some control over. Ideally, the distance would be calculated from the tech''s home to their first work order, from each work order to work order sequentially after their first call of the day, and then from the last work order of the day to their home again.
Having this information visible to techs and dispatch personnel will show a clear metric that they can impact. We can use this figure to gauge dispatch adjustments over time, to cross reference versus profitability and to potentially get buy in from personnel in the organization to make operational changes such as techs working (4) 10-hour schedules.
2. Certificates & Licensing — add quarterly ''continuing education'' requirement
As we look to keep developing training internally at Evolution, we are looking at adding a quarterly continuing education requirement for every tech (2 or 4 hours each quarter). The TYPE column, I believe, would be ''Continuing Education – Evolution'' and issuing authority would be ''Evolution''. I assume we would have the last date of each quarter as DATE EXPIRES.
We would need to talk through how that flips once the tech inputs their training hours for the current quarter, and how it would look when a tech is in the next quarter without completing requirement from previous quarter, etc.',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;

IF NOT EXISTS (SELECT 1 FROM dbo.backlogitem WHERE bi_code = N'EVINES-DEC' AND bi_title = N'Quarterly Continuing Education Requirement')
BEGIN
    INSERT INTO dbo.backlogitem
    (
        bi_code,
        bi_title,
        bi_source,
        bi_author,
        bi_date,
        bi_category,
        bi_type,
        bi_priority,
        bi_status,
        bi_effort,
        bi_desc,
        bi_notes,
        bi_detail_markdown,
        bi_history,
        bi_active,
        bi_insertdatetime,
        bi_lastupdated
    )
    VALUES
    (
        N'EVINES-DEC',
        N'Quarterly Continuing Education Requirement',
        N'EVines follow ups from 12_10 meeting.eml',
        N'Eric Vines',
        '2025-12-11',
        N'Configuration',
        N'Feature',
        N'Medium',
        N'Open',
        N'M',
        N'Add a quarterly continuing education requirement to Certificates & Licensing (2–4 hours/quarter per tech). Type = ''Continuing Education – Evolution'', Issuing Authority = ''Evolution'', Expiry = last day of each quarter. Needs logic to handle: rolling to next quarter after hours are logged, and what shows when a tech enters the next quarter without completing the prior quarter''s requirement.',
        NULL,
        N'EVines follow ups from 12/10 meeting
FromEric Vines <evines@evolutionmaintenance.com>
DateThu, 11 Dec 2025 22:25:12 +0000
To"evotrakker@evolutionmaintenance.com"
<evotrakker@evolutionmaintenance.com>
⚠ The original email body references an attached Word document. The Word document content has been recovered and is included below.
Hey y''all. In the attached Word doc, I added a little detail to what I talked about on the call yesterday plus added one forward looking Evotrakker request.
Let me know any questions or if you want to talk about implementation.
Thank you,
Eric V
Attached Word Document — Recovered Content
1. Job Drive Time / Distance Traveled — added to each call + reportable
Goal: to eventually be able to tie a drive time and/or driving distance to each work order (and to each individual tech within a work order) that can be logged in the system and pulled in reporting after the fact. As we have determined that utilization rate is one of the major factors to tech success and profitability, we see that drive times are likely the overall most impactful variable impacting utilization.
If we can have the drive time associated with each call visible on the work order — and in the rears in reporting — everyone in the company (including the techs) can have the ability to see a number that they have some control over. Ideally, the distance would be calculated from the tech''s home to their first work order, from each work order to work order sequentially after their first call of the day, and then from the last work order of the day to their home again.
Having this information visible to techs and dispatch personnel will show a clear metric that they can impact. We can use this figure to gauge dispatch adjustments over time, to cross reference versus profitability and to potentially get buy in from personnel in the organization to make operational changes such as techs working (4) 10-hour schedules.
2. Certificates & Licensing — add quarterly ''continuing education'' requirement
As we look to keep developing training internally at Evolution, we are looking at adding a quarterly continuing education requirement for every tech (2 or 4 hours each quarter). The TYPE column, I believe, would be ''Continuing Education – Evolution'' and issuing authority would be ''Evolution''. I assume we would have the last date of each quarter as DATE EXPIRES.
We would need to talk through how that flips once the tech inputs their training hours for the current quarter, and how it would look when a tech is in the next quarter without completing requirement from previous quarter, etc.',
        N'## 2026-03-09 03:46 UTC - Seed Import
- Imported from backlog.json and backlog reference content.',
        1,
        GETUTCDATE(),
        GETUTCDATE()
    );
END;
