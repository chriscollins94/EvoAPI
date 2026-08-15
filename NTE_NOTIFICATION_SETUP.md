# NTE Notification Service - Azure Setup Guide

This document covers the **Azure Communication Services (ACS)** setup needed before the NTE notification service can send SMS or email. The code is already wired up in EvoAPI; this guide is what an admin does once in the Azure portal.

## Overview

The service sends:
- **SMS** to the assigned tech when an active service request crosses **75%** of its NTE.
- **SMS** to the tech **AND email** to the zone email at **100%**.

It uses **Azure Communication Services** as the single provider for both SMS and email. Configuration (connection string, sender number, sender email, message templates, thresholds) lives in the `ConfigSetting` table — no redeploy needed to tune.

---

## 1. Provision the ACS resource

1. In the Azure Portal, **Create a resource → Communication Services**.
2. Subscription / Resource group: same one EvoAPI lives in.
3. Resource name: e.g. `evo-acs-prod` (and `evo-acs-test` for the test slot).
4. Data location: **United States** (must match your compliance posture; cannot be changed later).
5. Review + Create.

After it deploys:

- Open the resource → **Settings → Keys**.
- Copy the **Connection string** (Primary). This is the value for `NteConfig/AcsConnectionString` in the `ConfigSetting` table.

> **One resource, both environments.** A single ACS resource (one phone number, one email domain, one connection string) is shared between QA and prod. The same `AcsConnectionString` / `AcsSmsFrom` / `AcsEmailFrom` go into both QA and prod `ConfigSetting` rows. Environment separation is handled by the smoke-test override fields (see §5): QA keeps them populated so all sends route to the engineer; prod keeps them blank so real techs and zones get notified. This works because volume is low and there is a single developer maintaining both environments — if the team grows, revisit (separate resources, or add a code guard that refuses to send in non-prod when the override is empty).

---

## 2. Provision an SMS phone number

ACS does not give you a phone number by default — you have to buy one and (for US toll-free) get it verified.

1. In the ACS resource, go to **Phone numbers → + Get** (or **Numbers → Buy**).
2. Country: **United States**.
3. Number type: **Toll-free** (recommended for A2P) or **Local** (10DLC — requires brand/campaign registration).
4. Features: tick **SMS - Outbound** at minimum.
5. Quantity: 1.
6. Buy. The number appears in E.164 format (e.g. `+18005551234`).

This E.164 number is the value for `NteConfig/AcsSmsFrom`.

### Toll-free verification (TFV) — REQUIRED before sending to US carriers

US carriers silently drop unverified A2P toll-free traffic. You will see "successful" sends in the API but the texts never arrive on real phones.

1. In the ACS resource → **Phone numbers** → click your number → **Toll-free verification**.
2. Submit the TFV form. You'll need:
   - Business legal name, address, EIN
   - Contact email + phone
   - Use case: "Service request budget alerts to dispatched field technicians"
   - Sample message: paste the seeded template body for `Sms75Body` from [create_NotificationLog_table.sql](create_NotificationLog_table.sql)
   - Opt-in description: "Recipients are W-2 technicians whose mobile numbers are stored in the company HRIS for operational dispatch. They receive automated alerts only for work orders they are assigned to. STOP/HELP keywords supported."
   - Opt-out: "Recipients reply STOP. Numbers unsubscribe automatically; tech can be re-opted-in by request to dispatch."
   - Estimated volume: realistic per-day estimate.
   - Website URL: company site.
3. **Lead time: 2–6 weeks.** Plan accordingly. While verification is pending, the override fields below can be used to test.

> Until TFV is approved, only verify message delivery to the **smoke-test override numbers** (see §5). Production tech numbers will not receive anything.

---

## 3. Set up email sending domain

Email needs a verified sender domain attached to the ACS resource. Two options:

### Option A — Azure-managed `*.azurecomm.net` subdomain (fast, low cap, fine for smoke testing)

1. In the ACS resource → **Email → Domains → + Connect domain → Azure managed domain**.
2. Pick the resource group and a name. Azure will create something like `xxxxxxxx.azurecomm.net`.
3. Click the new domain → **MailFrom addresses**. The default `DoNotReply@<subdomain>.azurecomm.net` is what you'll use.
4. **Daily send limit: ~100 messages/day**. Acceptable for smoke testing only.

This is the value for `NteConfig/AcsEmailFrom`, e.g. `DoNotReply@xxxxxxxx.azurecomm.net`.

### Option B — Custom domain with SPF/DKIM (production)

1. **Email → Domains → + Connect domain → Custom domain**.
2. Domain name, e.g. `notify.evo.com` (a subdomain of your company domain — do not use the bare `evo.com`).
3. Azure shows DNS records to add: a TXT for verification, plus SPF and two DKIM CNAMEs.
4. Add those records in your DNS provider (Cloudflare / Route 53 / GoDaddy / wherever the company domain is hosted).
5. Back in Azure click **Verify** for each row. Verification can take from minutes up to ~48 hours depending on TTL.
6. Once verified, add a **MailFrom address**, e.g. `nte-alerts@notify.evo.com` — that becomes `NteConfig/AcsEmailFrom`.

> **Connect the domain to the Communication Services resource.** A common pitfall: the email domain is a *separate Azure resource* (Email Communication Service). After verifying it, you must explicitly connect it to your main ACS resource via **ACS → Email → Domains → Connect domain**. Until then the SDK will reject sends with `400 Sender domain is not verified`.

---

## 4. Update `ConfigSetting` rows

The migration script [create_NotificationLog_table.sql](create_NotificationLog_table.sql) seeds empty rows for these. Update them with the values from §1–3:

```sql
UPDATE ConfigSetting SET cs_value = 'endpoint=https://evo-acs-prod.communication.azure.com/;accesskey=...'
  WHERE cs_type = 'NteConfig' AND cs_identifier = 'AcsConnectionString';

UPDATE ConfigSetting SET cs_value = '+18005551234'
  WHERE cs_type = 'NteConfig' AND cs_identifier = 'AcsSmsFrom';

UPDATE ConfigSetting SET cs_value = 'DoNotReply@xxxxxxxx.azurecomm.net'
  WHERE cs_type = 'NteConfig' AND cs_identifier = 'AcsEmailFrom';
```

Do **not** flip `Enabled` to `true` yet.

---

## 5. Smoke test (without spamming real techs)

The override fields are how this codebase separates QA from prod. While set, **all** SMS goes to the override mobile and **all** zone emails go to the override email — no real tech or zone is contacted, regardless of what's in the service request data.

- **QA**: keep `SmokeTestOverrideMobile` and `SmokeTestOverrideEmail` populated with engineer contacts at all times. `Enabled` can stay `'true'` permanently.
- **Prod**: keep both override fields blank. Real techs and zones are the only recipients.

Before enabling the live path in prod, do a one-time smoke pass with the override fields populated:

```sql
UPDATE ConfigSetting SET cs_value = '+15555550100'
  WHERE cs_type = 'NteConfig' AND cs_identifier = 'SmokeTestOverrideMobile';

UPDATE ConfigSetting SET cs_value = 'you@yourcompany.com'
  WHERE cs_type = 'NteConfig' AND cs_identifier = 'SmokeTestOverrideEmail';

UPDATE ConfigSetting SET cs_value = 'true'
  WHERE cs_type = 'NteConfig' AND cs_identifier = 'Enabled';
```

Trigger a single scan immediately (admin JWT required):

```bash
curl -X POST https://evotest.azurewebsites.net/EvoApi/nte/run-once \
     -H "Authorization: Bearer <admin-jwt>"
```

The current implementation uses [StubNteSpendCalculator.cs](src/EvoAPI.Infrastructure/Services/StubNteSpendCalculator.cs), which assigns each active service request a deterministic percent (0 / 50 / 76 / 101) based on `sr_id`. So one scan should produce visible activity:

- SR with `sr_id % 4 == 2` → SMS to override mobile (76% threshold).
- SR with `sr_id % 4 == 3` → SMS + email to overrides (101% threshold, both 75 and 100 fire).

Verify in the database:

```sql
SELECT TOP 50 nl_id, nl_type, nl_key, nl_entity_id, nl_channel,
       nl_recipient, nl_send_status, nl_send_error, nl_insertdatetime
FROM dbo.NotificationLog
ORDER BY nl_id DESC;
```

Re-run `POST /EvoApi/nte/run-once` — no new rows should appear (dedup via the unique constraint).

When you're done smoke-testing **in prod**, clear the override fields and re-enable. In QA, leave the override fields set permanently so you can keep `Enabled='true'` without ever touching real users.

> **Operational checklist:** any time you edit `ConfigSetting` for the NTE service in QA, double-check that `SmokeTestOverrideMobile` and `SmokeTestOverrideEmail` are still non-empty before walking away. They are the only thing keeping a QA scan from texting prod techs (since both environments share the same ACS resource and connection string).

---

## 6. Going live in prod

1. Confirm TFV approval email from Microsoft (or the equivalent for 10DLC).
2. From the QA environment (overrides still populated), send a final test message to confirm delivery reaches a real US mobile.
3. Replace `StubNteSpendCalculator` with the real spend implementation (see plan file). Until that ships, enabling in prod will fire alerts based on the stub's deterministic percent — **not safe for production**.
4. In **prod's** `ConfigSetting`, confirm the override fields are blank:
   ```sql
   UPDATE ConfigSetting SET cs_value = ''
     WHERE cs_type = 'NteConfig'
       AND cs_identifier IN ('SmokeTestOverrideMobile', 'SmokeTestOverrideEmail');
   ```
5. Set `Enabled='true'` in prod. The background loop scans every `NteConfig/ScanIntervalMinutes` minutes (default 15).
6. **Do not change** the QA overrides — they stay populated forever so QA can keep `Enabled='true'` safely.

---

## 7. Tuning without a redeploy

Everything in `cs_type IN ('NteConfig', 'NteTemplate')` is read on each scan. Edit the `cs_value` and the next iteration picks it up. Notable knobs:

| Identifier | Purpose |
|---|---|
| `NteConfig/Enabled` | Master kill-switch. `false` skips the entire scan. |
| `NteConfig/ScanIntervalMinutes` | How often the loop runs. |
| `NteConfig/Thresholds` | CSV of percent triggers, e.g. `75,100` or `50,75,90,100`. The email leg fires only at thresholds `>= 100`. |
| `NteConfig/AcsConnectionString` | Rotate keys here. The SDK client is cached and rebuilds on connection-string change. |
| `NteConfig/SmokeTestOverrideMobile` / `SmokeTestOverrideEmail` | Re-enable any time you need to re-test in prod safely. |
| `NteTemplate/Sms75Body` etc. | Edit message wording. Tokens: `{sr_number}`, `{sr_id}`, `{percent}`, `{threshold}`, `{nte}`, `{spent}`, `{tech_firstname}`, `{tech_name}`, `{zone}`. |

---

## 8. Costs (approximate, US, as of writing)

- **SMS toll-free outbound (US):** ~$0.0075 per segment + ~$2/month per number.
- **Email:** ~$0.00025 per email, plus ~$0.0008 per MB of payload. Custom domain has no monthly fee; Azure-managed subdomain is free with the per-message rate.
- **TFV one-time fee:** none from Microsoft, but carriers may impose surcharges per message once verified.

Verify current pricing on the [Azure Communication Services pricing page](https://azure.microsoft.com/pricing/details/communication-services/) before committing — these numbers move.

---

## 9. Common failure modes

| Symptom | Likely cause |
|---|---|
| `RequestFailedException: 401 Unauthorized` | `AcsConnectionString` is wrong or the access key was rotated. |
| `400 Invalid 'from' phone number` | `AcsSmsFrom` is not E.164, or the number has not been provisioned for outbound SMS. |
| `400 Sender domain is not verified` | Email domain is verified but not connected to the ACS resource (see §3 callout). |
| API returns "Sent" but no SMS arrives on US mobiles | Toll-free verification not yet approved. |
| `NotificationLog` rows show `nl_send_status='Skipped'` with `NoMobile` | Tech `u_phonemobile` is empty in the user table. |
| Background service starts but never sends | `NteConfig/Enabled` is not `'true'` (case-sensitive string compare). |

---

## Reference

- Migration script: [create_NotificationLog_table.sql](create_NotificationLog_table.sql)
- Orchestrator: [NteNotificationService.cs](src/EvoAPI.Infrastructure/Services/NteNotificationService.cs)
- Background loop: [NteNotificationBackgroundService.cs](src/EvoAPI.Infrastructure/Services/NteNotificationBackgroundService.cs)
- ACS SMS wrapper: [AcsSmsService.cs](src/EvoAPI.Infrastructure/Services/AcsSmsService.cs)
- ACS Email wrapper: [AcsEmailService.cs](src/EvoAPI.Infrastructure/Services/AcsEmailService.cs)
- Admin trigger endpoint: [NteController.cs](src/EvoAPI.Api/Controllers/NteController.cs) — `POST /EvoApi/nte/run-once`
