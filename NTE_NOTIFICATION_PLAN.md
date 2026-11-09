# NTE Notification - Completion Plan

Companion to [NTE_NOTIFICATION_SETUP.md](NTE_NOTIFICATION_SETUP.md). The April 2026 build shipped the pipeline
(NotificationLog, orchestrator, ACS SMS wrapper, background loop, run-once endpoint, ConfigSetting knobs) with a
stub spend calculator. Azure toll-free verification completed and an SMS from `evo-acs-prod`
(+1 888 289 0689) was confirmed on a real phone on 2026-10-31. This file records the decisions made on
2026-11-01 and the work left to make the service real.

## Requirements (office, 2026-10-31)

- EvoTrakker calculates time on site (keeps increasing while the tech is checked in) and entered service items
  with tax and markup. Labor is tracked and charged while on site. Trip charge is included automatically.
- 75% of NTE: text the tech to stop and call the office.
- 100% of NTE: text the tech again and email the zone team assigned to the lead tech.
- Phone: Employee Directory **Evo Cell** (`u_phonemobile`), falling back to **Personal Cell** (`u_phonehome`).
- Zone: the tech's zone in the Employee Directory (`[user].z_id` -> `Zone.z_email`).

## Decisions

| # | Topic | Decision |
|---|---|---|
| D1 | Email provider | Use the existing `IEmailService` (SMTP, `EmailServer/*` ConfigSetting rows, same From as time-off emails). Delete `AcsEmailService`, its DI registration, the `NteConfig/AcsEmailFrom` row and setup doc section 3. ACS is for SMS only. |
| D2 | Who gets the text | Any tech currently checked in (`TimeTracking.ttt_id = 3`, `tt_end IS NULL`) on any work order of the SR. Two techs checked in at once both get it. Zone email goes to the zone of whoever received the text. (The original fallback to the tech on `sr.wo_id_primary` was dropped by D9.) |
| D3 | Trip charge | Worked trip charge counts from the moment the SR has an NTE, using the invoice cascade (LaborRate flat $ -> SR % -> company %). **Revised 2026-11-09:** the quoted trip charge (`sr_tripcharge_quote` / `_quote_billed`) never counts, even after Prepare Quote. |
| D4 | NTE changes | Dedup key includes the NTE amount, so raising (or lowering) the NTE re-arms both thresholds against the new amount. |
| D5 | Scan interval | 5 minutes (`NteConfig/ScanIntervalMinutes = 5`). Event-driven recompute on check-in/out is a follow-up only if 5 minutes proves too loose. |
| D6 | Wording | See Config changes below. Office can edit templates in ConfigSetting at any time. |
| D7 | Quoted items | **Revised 2026-11-09:** quoted service items (`xwosi_forquote = 1`) never count toward spend. The legacy `sr.sr_nteexcludequotedsi` flag (set only by the tech page's Prepare Quote button, never cleared) is not consulted. The excluded amount is written to the audit detail as `quotedItemsExcluded`. Original rule (count unless the flag = 1, matching the tech page) produced texts driven by unapproved parts. |
| D8 | Stale open check-ins | Open check-ins older than `NteConfig/MaxOpenCheckInHours` (default 24) are treated as a forgotten check-out and ignored; the count is written to NotificationLog metadata as `staleCheckIns`. Within the limit an open check-in counts up to now. |
| D9 | Which SRs the scan considers (2026-11-04) | Only SRs with a tech checked in right now (open check-in newer than `MaxOpenCheckInHours`). SRs nobody is on are not scanned, so nothing fires for idle, Complete or Invoiced jobs; a job already past its NTE alerts at the next check-in, which is when the warning matters. The 100% zone email waits for that check-in too; split it off later if the office needs it sooner. Prod sizing 2026-11-04: 1,608 open SRs with an NTE, 744 >= 75%, 1,252 of them Invoiced; only 7 had a tech checked in. An activity window was rejected because invoicing counts as activity. |
| D10 | Audit + staged rollout (2026-11-07) | NotificationLog renamed/reshaped to `dbo.AuditNTE` (Audit-style `an_` columns): one row per text/email = audit trail AND dedup lock; retries bump `an_attempts` and keep earlier errors. Rollout in three phases driven by config only: (1) both `SmokeTestOverride*` rows hold the reviewers (semicolon lists), (2) `EnabledTechIds` lists a few user ids, (3) all cleared + `2026-11-07_nte_cutover_mark_reviewed.sql` marks rerouted rows `Reviewed` so techs still get their real alert. Techs outside the allowlist get no audit row. |

## Spend definition (mirrors legacy `EvoData.GetInvoice` / `GetTimeTrackingSummary`)

```
spent = labor + service items + trip charge
percent = spent / sr.sr_nte * 100
```

- **Labor** - `TimeTracking` rows with `ttt_id = 3` on the SR's work orders where `wo.wot_id = 1`.
  Closed rows use `tt_end`; open rows use `SYSUTCDATETIME()` (check which clock `tt_begin` is stored in before coding).
  Rate = `tt_invoicedrate` (snapshotted at check-in from the chosen LaborRateType). Minutes are rounded with
  `BillableRule.br_roundtominute` and `xrefCompanyCallCenter.xccc_minimumlaborchargeinminutes` is applied once per SR.
  Flat-rate SRs (`sr_flatorhourly = 'flat'`) use `sr_rateflat` instead of hours.
- **Service items** - per item `((xwosi_basecost * xwosi_quantity) + tax) * (1 + xwosi_percentagemarkup / 100)` where
  `tax = xwosi_basecost * xwosi_quantity * xwosi_percentagetax / 100`. `xwosi_percentagemarkup` already includes the
  supplier markup. Incurred items only; quoted items never (D7, revised 2026-11-09).
- **Trip charge** - worked leg only, per D3 (quoted trip charge excluded since 2026-11-09). Reuse the cascade already in `GetServiceRequestLaborContext` / `ResolveTripCharge`.

## Work items

1. **Query fixes** (`NteQueryRepository`)
   - Remove the `sr.z_id` join (column does not exist on ServiceRequest; the scan has never completed).
   - Recipient selection per D2; phone fallback per requirements; zone from `[user].z_id`.
   - Return one row per (SR, recipient) so two checked-in techs both get texted.
2. **Real spend calculator** replacing `StubNteSpendCalculator`
   - One set-based query per scan (not per SR), returning spent / percent for every open SR with `sr_nte > 0`.
   - Validate against a handful of closed SRs where `sr_totaldue` is known.
3. **Orchestrator changes** (`NteNotificationService`)
   - Dedup key = `{threshold}@{nte}` per D4 (AuditNTE `an_key`).
   - Email via `IEmailService`; drop `AcsEmailService`.
   - New tokens `{location}` and `{customer}` for the email body.
4. **Config changes** (TEST now, PROD at go-live)
   - `ScanIntervalMinutes = 5`; templates below; delete `AcsEmailFrom` row.
5. **Docs** - setup doc rewritten 2026-11-07 (config table, deploy steps, three rollout phases, watching, failure modes).
6. **Smoke test** on TEST with overrides populated, then PROD rollout per setup doc section 3.
7. **Audit + rollout controls** (2026-11-07, D10) - AuditNTE table, override lists, EnabledTechIds, cutover script.

## Config changes (templates)

```sql
UPDATE ConfigSetting SET cs_value = '5'
 WHERE cs_type = 'NteConfig' AND cs_identifier = 'ScanIntervalMinutes';

UPDATE ConfigSetting SET cs_value = '{tech_firstname}, SR {sr_number} is at {percent}% of NTE (${spent} of ${nte}). Stop and call the office before continuing.'
 WHERE cs_type = 'NteTemplate' AND cs_identifier = 'Sms75Body';

UPDATE ConfigSetting SET cs_value = '{tech_firstname}, SR {sr_number} has hit 100% of NTE (${spent} of ${nte}). Stop work now and call the office.'
 WHERE cs_type = 'NteTemplate' AND cs_identifier = 'Sms100Body';

UPDATE ConfigSetting SET cs_value = 'NTE reached: SR {sr_number} ({tech_name})'
 WHERE cs_type = 'NteTemplate' AND cs_identifier = 'Email100Subject';

UPDATE ConfigSetting SET cs_value = '<p>Service Request <strong>{sr_number}</strong> for {customer} at {location}, worked by {tech_name}, has reached <strong>{percent}%</strong> of its Not-To-Exceed amount.</p><p>NTE: ${nte}<br/>Current spend: ${spent}<br/>Zone: {zone}</p><p>Customer approval is required before further work proceeds.</p>'
 WHERE cs_type = 'NteTemplate' AND cs_identifier = 'Email100Body';

DELETE FROM ConfigSetting WHERE cs_type = 'NteConfig' AND cs_identifier = 'AcsEmailFrom';
```

Run the template updates together with the code deploy that adds the `{customer}` / `{location}` tokens;
until then the email body would render those tokens literally.

## Status log

- 2026-10-31 - ACS SMS confirmed end to end from the portal Try SMS page.
- 2026-11-01 - TEST: NotificationLog exists, `AcsSmsFrom` and E.164 override mobile set, connection string verified
  against `evo-acs-prod.unitedstates.communication.azure.com`. `Enabled` still `false`. Decisions D1-D7 made.
- 2026-11-01 - Work item 1 done: `NteQueryRepository` rewritten (recipient rule D2, Evo/Personal cell fallback,
  zone from the tech, customer + location for email tokens, no `sr.z_id`). Read-only copy at
  `sql/diagnostics/nte_recipients.sql`. Orchestrator still dedups per SR/threshold/channel, so until work item 3
  lands a second checked-in tech on the same SR is silently skipped by the unique constraint.
- 2026-11-01 - Work item 2 done: `SqlNteSpendCalculator` replaces the stub (one set-based query per scan;
  labor incl. open check-ins measured to GETUTCDATE, billable-rule ceiling rounding, company minimum once,
  flat-rate SRs, service items with stored tax/markup %, trip charge cascade per D3, quoted items per D7).
  `INteSpendCalculator.CalculateActiveAsync` returns a dictionary keyed by sr_id; orchestrator looks it up.
  Validation script `sql/diagnostics/nte_spend_validation.sql` compares the formula with `sr_totaldue`.
  Index check script `sql/diagnostics/nte_scan_indexes.sql` (recipient query took 28 s on TEST).
- 2026-11-01 - Validation on TEST (100 most recent invoiced SRs): every 2025-2026 SR with closed check-ins
  matches `sr_totaldue` to the cent; remaining differences are D3/D7, items edited after invoicing, or 2022-2024
  SRs whose stored tax/markup % predate the recalculation feature. Found and fixed: duplicate LaborRate rows on
  TEST (6 company/trade pairs; the 2026-09-16 unique-index migration has not run there) doubled labor, now
  `OUTER APPLY TOP 1`. Found 21 open check-ins on TEST, most years old -> D8 added, config row in
  `sql/migrations/2026-11-01_nte_notification_config_updates.sql`. Recipient and spend queries run in ~1 s.
- 2026-11-01 - Work item 3 done: orchestrator rewritten. Dedup keys `{threshold}@{nte}|u{techUserId}` (SMS) and
  `{threshold}@{nte}|z{zoneId}` (email) so alerts re-arm on NTE change (D4) and each checked-in tech / zone is
  handled once. Email via `IEmailService.TrySendEmailAsync` (new, reports success; SMTP sender otherwise swallows
  failures); `AcsEmailService` and the `Azure.Communication.Email` package removed; `NteConfig/EmailFrom`
  replaces `AcsEmailFrom` (seeded from `EmailTimeOff/From` by the 2026-11-01 migration). Phone numbers normalised
  to E.164 (10 digits -> +1; invalid -> Skipped with reason). Config and templates loaded once per scan;
  fired-key lookup cached per SR; rows that crossed nothing never touch NotificationLog. New template tokens
  `{customer}`, `{location}`, `{labor}`, `{service_items}`, `{trip_charge}`.
- 2026-11-01 - Work item 4 done: `sql/migrations/2026-11-01_nte_notification_config_updates.sql` now carries
  MaxOpenCheckInHours, ScanIntervalMinutes = 5, EmailFrom (seeded from EmailTimeOff/From), AcsEmailFrom removed,
  and the four approved templates. Run on TEST 2026-11-01. The "Config changes (templates)" section above is
  superseded by that script.
- 2026-11-01 - First smoke scan on TEST (overrides on): SR 9729 got the 75% and 100% texts, then the zone email
  hung in SMTP and the scan stalled; one NotificationLog row left in `Sending`. Fixed: (a) only the highest
  crossed threshold is sent per channel, lower ones logged Skipped/superseded; (b) SMTP send bounded to 60 s;
  (c) rows stuck in `Sending` > 15 min are marked Failed at scan start and `TryInsertSendingAsync` reclaims
  Failed rows so retries are possible (previously a Failed row blocked its key forever).
  Found: 508 of 968 open SRs with an NTE on TEST are already >= 75% (340 >= 100%); 900 of the 968 have had no
  activity for over a year. Needs an activity window before any further scan (pending decision D9).
- 2026-11-04 D9 decided and built. `sql/diagnostics/nte_population_counts.sql` run on PROD by the user (login via
  Web.config fails; the connectionStrings `cnEvoPROD` password is a placeholder): 1,608 / 744 >= 75% / 452 >= 100%,
  1,252 Invoiced, 478 with activity in the last 7 days (invoicing counts as activity), 7 with an open check-in.
  `NteQueryRepository` now returns only (SR, checked-in tech) rows, stale check-ins excluded; the primary-WO
  fallback is gone. `NteNotificationService` reads `MaxOpenCheckInHours` once per scan and passes it to both
  queries (`SqlNteSpendCalculator` no longer reads config). Scan returns early when no row qualifies. Build clean.
  Smoke test on TEST now needs a real check-in on an SR with an NTE (TEST had zero open check-ins on 2026-11-04).
- 2026-11-07 Second smoke scan on TEST (first with D9). SR 100337 (20261020-6783, Ayman u163, checked in
  2026-11-06 21:07 UTC, still open 19 h): flat $240 + trip $90 = $330 = 137.5%. One text sent (100%), the 75%
  row logged Skipped/superseded, correct. Zone email timed out after 60 s again. Root cause: `SmtpClient`
  defaults to port 25, which the dev network blocks (TCP connect hangs); 587 connects instantly. Legacy EvoWS
  uses the same default and only works because Azure allows 25. `SmtpEmailService` now uses 587 (optional
  `EmailServer/Port` override). The abandoned/failed email row is reclaimed and retried on the next scan.
- 2026-11-07 D10 built. `NotificationLog` -> `dbo.AuditNTE` (migration 2026-04-19 rewritten; drops the TEST copy,
  PROD never had it). New `AuditNteEntry` / `IAuditNteRepository` / `AuditNteRepository`; old classes deleted.
  Service: audit row carries tech, zone, intended vs actual recipient, overridden flag, description, spend JSON,
  server name; `SmokeTestOverrideMobile` / `Email` take semicolon lists (one text per number, one row);
  `EnabledTechIds` gate skips unlisted techs before any audit write. 2026-11-01 migration gained the
  `EnabledTechIds` row. New `sql/diagnostics/nte_audit_recent.sql`; status script updated. Both migrations run on
  TEST 2026-11-07; AuditNTE empty, so the next scan re-sends SR 100337 if Ayman is still checked in (good: it
  exercises the audit row and the port-587 email). Setup doc rewritten. Build clean.
- **2026-11-09 - D7 revised: quoted items never count.** Prod go-live review of SR 20261108-3364 (125%, text + email
  rerouted to the reviewer) raised the question of what the spend includes. Findings against the legacy invoice:
  labor, rounding, minimum, flat rate and trip cascade all match CreateInvoice; TimeQuoted hours are not billed by
  either; quoted service items were counted unless `sr_nteexcludequotedsi = 1`, a flag only the tech page's Prepare
  Quote button sets. Decision: quoted items (`xwosi_forquote = 1`) are excluded unconditionally and the flag is no
  longer read. `NteSpendResult.QuotedItemsExcluded` / audit detail `quotedItemsExcluded` show what was left out.
  New `sql/diagnostics/nte_spend_breakdown.sql` explains one SR's number; validation and recipients scripts updated.
  Also found: the tech page's Spent is the stored invoice total (no labor until check-out, no trip until time exists),
  so it reads lower than the scan mid-job by design (D3/D8).
- **2026-11-09 - D3 revised: quoted trip charge never counts.** Follow-on to the D7 change above. TripCharge is the
  worked leg only; `sr_tripcharge_quote`, `sr_tripcharge_quote_billed` and `sr_quotedtimestart` are no longer read by
  the calculator. Breakdown script shows the excluded amount as `quoted_trip_excluded`; validation script updated.
  Net rule: nothing quoted (items, hours, trip) counts toward NTE spend.
