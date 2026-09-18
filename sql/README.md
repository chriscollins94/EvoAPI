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

| Script | Ships with | Notes |
| --- | --- | --- |
| `migrations/2026-09-08_create_form_tables.sql` | EvoAPI + evotech | PM workflow slice 1 |
| `migrations/2026-09-08_create_asset_tables.sql` | EvoAPI + evotech | PM workflow slice 2 |
| `migrations/2026-09-10_create_xrf_tables.sql` | EvoAPI + evotech | Final shape already includes the result column and the premise key. PROD needs only this one. |
| `migrations/2026-09-11_add_xrfbatchdetail_result_column.sql` | TEST only | Skip on PROD; folded into the create script. |
| `migrations/2026-09-12_alter_xrfbatchdetail_premisenumber.sql` | TEST only | Skip on PROD; wipes test waves and re-keys. |
| `migrations/2026-09-15_xrf_result_add_not_used.sql` | EvoAPI + evotech | Required on ANY database whose XRF tables were created before 2026-09-15 (the create script only gained 'XRF Not Used' that day). Without it every XRF Not Used submit fails. Safe to rerun. |
| `migrations/2026-09-16_add_laborrate_unique_customer_trade.sql` | EvoWS + EvoUI | Unique index on LaborRate (xccc_id, t_id). Aborts and lists duplicates if any exist; clean them first (see `diagnostics/laborrate_duplicates.sql`). |

Earlier scripts (markup decimal, performance, status-change user tracking, attachment
geolocation, customer inquiry attack points) each carry their own TEST/PROD status in their
header comments and in the deploy notes kept with the feature.

## XRF

- `seed/insert_xrf_batch_template.sql` loads a real wave: edit the wave name, optional team and
  the premise / meter list, then run. Re-runnable.
