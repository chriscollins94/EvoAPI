# SQL scripts

Everything that changes the EVO database outside of application code lives here. Scripts run
against TEST first and PROD together with the app deploy that needs them.

## Layout

| Folder | What goes here | Naming |
| --- | --- | --- |
| `migrations/` | Schema and config changes that must reach PROD: tables, columns, indexes, config rows, one-time data fixes. Every script is idempotent (`IF NOT EXISTS` guards) so rerunning is safe. | `YYYY-MM-DD_<what>.sql`, date = when the script was written. Run in date order. |
| `seed/` | Data loads. Wave templates the office edits and runs, plus TEST-only seeds. Never part of a PROD deploy unless the header says so. | descriptive name, no date |
| `rollback/` | Scripts that undo a migration. Keep the matching migration's name in the file. | descriptive name |
| `diagnostics/` | Read-only queries used to check state or troubleshoot. | descriptive name |
| `forms/` | Forms / PM engine seeds and their source workbooks (built 2026-09). | as-is |

Rules that keep this workable for one person:

1. New migration = new file with today's date prefix. Never edit a migration that has already run
   on PROD; write a follow-up instead. Editing one that has only run on TEST is fine while the
   feature is still in flight (the XRF scripts below did exactly that).
2. Put the deploy bundle in the header comment: which apps must ship with the script
   (EvoAPI, EvoWS, EvoUI, evotech) and whether it is TEST-only.
3. Reference scripts from code comments by their full path under `sql/` so a rename shows up in grep.

## Pending for PROD

Scripts written since the last PROD deploy, in run order. Delete a line once it has run on PROD.
`diagnostics/prod_deploy_preflight.sql` (read-only) reports OK / MISSING for every line below; run it before and after.

| Script | Ships with | Notes |
| --- | --- | --- |
| `migrations/2026-09-08_create_form_tables.sql` | EvoAPI + evotech | PM workflow slice 1 |
| `migrations/2026-09-08_create_asset_tables.sql` | EvoAPI + evotech | PM workflow slice 2 |
| `migrations/2026-09-16_add_laborrate_unique_customer_trade.sql` | EvoWS + EvoUI | Unique index on LaborRate (xccc_id, t_id). Aborts and lists duplicates if any exist; clean them first (see `diagnostics/laborrate_duplicates.sql`). |
| `migrations/2026-09-24_create_pm_ticket_tables.sql` | EvoAPI + evotech | PM workflow slice 3 (ticket entry): ServiceType lookup, ServiceRequest.svt_id / fr_id / sr_pmunitcount / sr_servicebydate, Priority.p_allowpreventative, PMVisit, PMVisitUnit. Safe to rerun. |
| `migrations/2026-09-25_create_pm_visit_answer_table.sql` | EvoAPI + evotech + EvoUI | PM workflow slice 4a (tech visit): PMVisitAnswer. EvoUI ships the legacy tech schedule routing (config.tech.js apiRootEvoApi / evotechRoot + schedule.js). Safe to rerun. |
| `migrations/2026-09-25_create_pm_visit_finding_table.sql` | EvoAPI + evotech | PM workflow slice 4b (checkout): PMVisitFinding. Safe to rerun. |
| `migrations/2026-04-19_create_AuditNTE_table.sql` | EvoAPI | NTE notifications: AuditNTE table (audit trail + dedup) and NteConfig/NteTemplate seed rows. Drops the TEST-only NotificationLog predecessor. Ran on TEST 2026-11-07. Safe to rerun. |
| `migrations/2026-11-01_nte_notification_config_updates.sql` | EvoAPI | NTE notifications: EmailFrom, MaxOpenCheckInHours (D8), EnabledTechIds rollout valve, 5-minute interval (D5), final template wording. Run after the 2026-04-19 script. Ran on TEST 2026-11-07. Safe to rerun. |
| `migrations/2026-11-07_nte_cutover_mark_reviewed.sql` | manual | NTE notifications: run by hand on PROD once, when the SmokeTestOverride* rows are cleared at the end of the review period. Marks rerouted AuditNTE rows Reviewed so techs get their real alert. Not part of a deploy. |

Earlier scripts (markup decimal, performance, status-change user tracking, attachment
geolocation, customer inquiry attack points) each carry their own TEST/PROD status in their
header comments and in the deploy notes kept with the feature.

## PM workflow (Preventative Maintenance)

- `seed/seed_pm_poc_test_customer.sql` (TEST only) sets up the POC test customer Chris picked on 2026-09-24:
  23rd Group Facility Services / DELETE - Archwood Meadows (pairing 25, location "Archwood Meadows" 4661).
  Adds PM - Spring / PM - Fall labor rates, fills the PM terms on the HVAC form rule, loads a price sheet
  (RTU / Split System tiers, coil cleaner and belt change add-ons), three seasons, and three more rooftop
  units with filters, belts and attributes. Re-runnable; only NULL rule fields are filled, tiers are replaced.

## XRF

- `seed/insert_xrf_batch_template.sql` loads a real wave: edit the wave name, optional team and
  the premise / meter list, then run. Re-runnable.
