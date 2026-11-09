# NTE Notification Service - Setup, Rollout and Operations

How the NTE alerts are configured, rolled out to prod in stages, and watched. Design decisions and the
build history are in [NTE_NOTIFICATION_PLAN.md](NTE_NOTIFICATION_PLAN.md).

## What it does

Every `NteConfig/ScanIntervalMinutes` (5) the EvoAPI background loop looks at every open service request
that has an NTE **and a tech checked in right now** (EvoTrakker check-in with no check-out, newer than
`MaxOpenCheckInHours`). For each one it works out spend to date the way the invoice will (labor to the
minute, incurred service items with tax and markup, trip charge) and:

- **75%** of NTE: texts the checked-in tech to stop and call the office.
- **100%**: texts the tech and emails the tech's zone (Employee Directory zone, `Zone.z_email`).

Quoted service items and quoted labor hours never count toward spend; the quoted trip charge counts once a
quote has been started on the SR, the same as the invoice. `sql/diagnostics/nte_spend_breakdown.sql` explains
any one SR's number line by line.

Only the highest crossed threshold is sent; a job first seen at 137% gets one text, not two. Each
text / email is sent once per tech (or zone) per threshold per NTE amount; changing the NTE re-arms it.
A job nobody is checked in on is not scanned at all, so idle, Complete or Invoiced jobs never alert.
A job already past its NTE alerts the moment someone next checks in.

Texts go through **Azure Communication Services** (resource `evo-acs-prod`, toll-free `+1 888 289 0689`,
verified). Emails go through the same Office 365 SMTP mailbox as the time-off emails.

---

## 1. Config rows (ConfigSetting, `cs_type = 'NteConfig'`)

All read every scan; edit `cs_value` and the next scan picks it up. No redeploy.

| Identifier | Purpose | Prod value |
|---|---|---|
| `Enabled` | Master switch. Anything but `true` skips the scan. | `false` until step 3 below |
| `ScanIntervalMinutes` | Loop cadence. | `5` |
| `Thresholds` | CSV of percents. Email leg fires only at `>= 100`. | `75,100` |
| `MaxOpenCheckInHours` | Open check-ins older than this are a forgotten check-out: ignored for spend and the SR is not scanned. | `24` (lower to `12` if forgotten check-outs keep alerting the next morning) |
| `AcsConnectionString` | From the ACS resource, Settings > Keys. Same value as TEST. | set by hand |
| `AcsSmsFrom` | E.164 sender. | `+18882890689` |
| `EmailFrom` | Mailbox whose password is in `EmailServer/FromPassword`. | `noreply@evolutionmaintenance.com` |
| `SmokeTestOverrideMobile` | **Rollout control.** Semicolon list of numbers. When non-empty every text goes here instead of the tech. | phase 1: reviewers; phase 3: blank |
| `SmokeTestOverrideEmail` | **Rollout control.** Semicolon list of addresses. When non-empty every zone email goes here. | phase 1 and 2: reviewers; phase 3: blank |
| `EnabledTechIds` | **Rollout control.** Comma list of Employee Directory user ids that may be alerted. Empty = everyone. Techs not listed are skipped with no audit row. | phase 2: a few ids; phase 3: blank |

Templates live in `cs_type = 'NteTemplate'`: `Sms75Body`, `Sms100Body`, `Email100Subject`, `Email100Body`.
Tokens: `{sr_number}` `{sr_id}` `{percent}` `{threshold}` `{nte}` `{spent}` `{labor}` `{service_items}`
`{trip_charge}` `{tech_firstname}` `{tech_name}` `{zone}` `{customer}` `{location}`.

Optional: `EmailServer/Port` (defaults to 587; Office 365 needs 587, and port 25 is blocked on most networks).

---

## 2. Deploy to prod

1. Deploy EvoAPI.
2. Run, in order, on PROD:
   - `sql/migrations/2026-04-19_create_AuditNTE_table.sql` (table + seed rows, `Enabled = false`)
   - `sql/migrations/2026-11-01_nte_notification_config_updates.sql` (EmailFrom, MaxOpenCheckInHours, EnabledTechIds, templates)
3. Set by hand on PROD: `AcsConnectionString` (copy from TEST), `AcsSmsFrom`, and confirm `EmailFrom`.
4. Run `sql/diagnostics/nte_notification_status.sql`: three EXISTS/gone lines, 10 NteConfig rows, 4 templates.
5. Confirm the prod App Service has **Always On**. The loop runs inside EvoAPI; without Always On it only
   runs while something is hitting the API.

---

## 3. Rollout phases

Size the first scan first: `sql/diagnostics/nte_population_counts.sql` on PROD. The `with_open_checkin`
column is how many SRs the scan can actually touch (7 on 2026-11-04). Everything else is context.

**Phase 1 - review. Everything goes to the reviewers, no tech or zone is contacted.**

```sql
UPDATE ConfigSetting SET cs_value = '+16155550100;+16155550101' WHERE cs_type = 'NteConfig' AND cs_identifier = 'SmokeTestOverrideMobile';
UPDATE ConfigSetting SET cs_value = 'you@company.com;other@company.com' WHERE cs_type = 'NteConfig' AND cs_identifier = 'SmokeTestOverrideEmail';
UPDATE ConfigSetting SET cs_value = 'true' WHERE cs_type = 'NteConfig' AND cs_identifier = 'Enabled';
```

Watch `sql/diagnostics/nte_audit_recent.sql`. Every row shows `an_overridden = 1`, `an_intendedrecipient`
(the tech's phone / zone email it would have gone to) and `an_recipient` (you). Check the numbers in
`an_detail` against the SR.

**Phase 2 - a few techs for real, emails still to the reviewers.**

```sql
UPDATE ConfigSetting SET cs_value = '163,9' WHERE cs_type = 'NteConfig' AND cs_identifier = 'EnabledTechIds';   -- Employee Directory ids
UPDATE ConfigSetting SET cs_value = '' WHERE cs_type = 'NteConfig' AND cs_identifier = 'SmokeTestOverrideMobile';
```

Only listed techs are texted; nobody else gets a row, so they alert normally later. Leave
`SmokeTestOverrideEmail` set if the office is not ready for zone emails yet.

**Phase 3 - everyone. Run the cutover script at the same time.**

```sql
UPDATE ConfigSetting SET cs_value = '' WHERE cs_type = 'NteConfig' AND cs_identifier IN ('EnabledTechIds', 'SmokeTestOverrideMobile', 'SmokeTestOverrideEmail');
```

then run `sql/migrations/2026-11-07_nte_cutover_mark_reviewed.sql` **once, by hand, on PROD**. It marks the
rows that were rerouted during phases 1 and 2 as `Reviewed`. Without it, a tech checking in on one of
those SRs would never be warned, because the dedup row says the alert was already sent.

**TEST stays in phase 1 forever.** Both override rows populated, `Enabled = true`. TEST shares the ACS
number and the SMTP mailbox with PROD; the overrides are the only thing keeping a TEST scan from texting
a real tech. Any time you edit NteConfig on TEST, check they are still non-empty before walking away.

---

## 4. Watching it

| Script | Shows |
|---|---|
| `sql/diagnostics/nte_notification_status.sql` | Migrations applied, all config values (connection string masked), last 50 audit rows. |
| `sql/diagnostics/nte_audit_recent.sql` | Last 100 texts / emails with who, where (intended and actual), status, attempts, message, error; 30-day totals. |
| `sql/diagnostics/nte_recipients.sql` | Which (SR, checked-in tech) rows the next scan will consider. |
| `sql/diagnostics/nte_spend_validation.sql` | The spend arithmetic against invoiced SRs. |
| `sql/diagnostics/nte_population_counts.sql` | Population sizing by activity and status. |

`dbo.AuditNTE` is both the audit trail and the dedup lock. Statuses: `Sending` (in flight), `Sent`,
`Failed` (retried next scan; `an_attempts` counts, earlier errors kept in `an_errormessage`), `Skipped`
(no phone, no zone, superseded by the 100% alert; reason in `an_errormessage`), `Reviewed` (released by
the cutover script). Rows stuck in `Sending` for 15 minutes are marked `Failed` at the next scan start.

Manual scan (admin JWT): `POST /EvoApi/nte/run-once`.

---

## 5. Failure modes

| Symptom | Likely cause |
|---|---|
| Email rows `Failed` with "SMTP send timed out after 60s" | Outbound SMTP port blocked. Default is 587; set `EmailServer/Port` if the network needs something else. |
| Email rows `Failed` with an authentication error | `EmailFrom` is not the mailbox whose password is in `EmailServer/FromPassword`. |
| SMS `Failed` with `401 Unauthorized` | `AcsConnectionString` wrong or key rotated. |
| SMS `Failed` with `400 Invalid 'from' phone number` | `AcsSmsFrom` not E.164 or not provisioned for outbound SMS. |
| SMS `Sent` but nothing arrives on a tech's phone | Carrier filtering; toll-free verification is approved, so check the number in the Employee Directory. |
| `Skipped` "has no Evo Cell or Personal Cell on file" | Fill `u_phonemobile` (Evo Cell) in the Employee Directory. Raising or lowering the NTE re-arms the alert. |
| `Skipped` "has no zone" / "Zone has no email address" | Employee Directory zone assignment or `Zone.z_email`. |
| Loop runs, nothing ever sent | `Enabled` not `true`; or nobody is checked in on an SR with an NTE (see `nte_recipients.sql`); or `EnabledTechIds` excludes them. |
| A tech says they were not warned about an SR you can see is past NTE | They were not checked in when it crossed; it alerts at their next check-in. Or the row is `Sent` from the review period and the cutover script was not run. |

---

## Reference

- Migrations: `sql/migrations/2026-04-19_create_AuditNTE_table.sql`, `sql/migrations/2026-11-01_nte_notification_config_updates.sql`, `sql/migrations/2026-11-07_nte_cutover_mark_reviewed.sql` (manual)
- Orchestrator: [NteNotificationService.cs](src/EvoAPI.Infrastructure/Services/NteNotificationService.cs)
- Recipient query: [NteQueryRepository.cs](src/EvoAPI.Infrastructure/Repositories/NteQueryRepository.cs)
- Spend: [SqlNteSpendCalculator.cs](src/EvoAPI.Infrastructure/Services/SqlNteSpendCalculator.cs)
- Audit / dedup: [AuditNteRepository.cs](src/EvoAPI.Infrastructure/Repositories/AuditNteRepository.cs)
- Background loop: [NteNotificationBackgroundService.cs](src/EvoAPI.Infrastructure/Services/NteNotificationBackgroundService.cs)
- SMS: [AcsSmsService.cs](src/EvoAPI.Infrastructure/Services/AcsSmsService.cs); email: [SmtpEmailService.cs](src/EvoAPI.Infrastructure/Services/SmtpEmailService.cs)
- Admin trigger: [NteController.cs](src/EvoAPI.Api/Controllers/NteController.cs)
