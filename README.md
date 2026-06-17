# CT Sheets Sync

Automates syncing CleverTap campaign data to Google Sheets using browser automation (Playwright). No CSV exports required — the scripts log into the CleverTap dashboard directly, scrape campaign stats, and upsert rows into the correct sheet tabs.

---

## What's automated

| Script | Channel | Creator filtered | Sheet tab |
|---|---|---|---|
| `sms_shivam/ct_browser_sync.py` | SMS | shivamprashar@goodeducator.com | GID 470166044 |
| `sms_gurkirat/ct_browser_sync.py` | SMS | gurkirat@goodeducator.com | GID 75277204 |
| `wa_browser_sync.py` | WhatsApp | gurkirat@goodeducator.com | GID 0 (first tab) |

Each automation scrapes three pages per campaign on the CleverTap dashboard and writes the combined data to Google Sheets.

---

## Repository structure

```
CT_sheets_sync/
├── sms_shivam/
│   ├── ct_browser_sync.py   # SMS scraper for Shivam's campaigns
│   └── csv_to_sheets.py     # Manual CSV import path (fallback)
├── sms_gurkirat/
│   ├── ct_browser_sync.py   # SMS scraper for Gurkirat's campaigns
│   ├── csv_to_sheets.py     # Manual CSV import path (fallback)
│   └── run.sh               # Convenience wrapper for gurkirat sync
├── wa_browser_sync.py       # WhatsApp scraper for Gurkirat's campaigns
├── wa_to_sheets.py          # Sheet writer for WhatsApp data
├── requirements.txt
├── .env.example             # Template for environment variables
└── credentials.json         # Google service account key (gitignored)
```

---

## How the browser automation works

### Session management (MFA)

CleverTap uses MFA (Google Authenticator OTP). The scripts handle this in two modes:

**`--setup` (first-time or when session expires)**
Opens a visible browser window. You fill the OTP manually and tick "Remember this device for 30 days". The entire browser profile (including device fingerprint) is saved to `ct_browser_profile/`. CleverTap uses this fingerprint to recognize the same "device" — so within the 30-day window, only your password is needed on re-login, not the OTP.

**Normal run (daily)**
Loads the saved browser profile in headless mode and skips login entirely. If the session has expired (IP change, etc.), the script auto-relogins using the saved email+password. If the 30-day device-remember window has also expired, it exits with a message telling you to run `--setup` again.

### Campaign discovery

Navigating to the SMS or WhatsApp campaigns list and applying:
- **Channel filter** — SMS or WhatsApp
- **Creator filter** — the email of the campaign creator
- **Date filter** — applied day-by-day for the target range

The day-by-day date iteration is intentional: CleverTap has a display limit on campaign lists. Fetching one day at a time ensures every campaign is captured without being cut off by the list's scroll limit.

Each row in the list is parsed to extract the campaign name and ID from the link href.

### Per-campaign scraping (3 pages each)

For every discovered campaign, the script visits three sub-pages:

1. **Overview tab** (`/report/overview`)
   - Send date (reformatted to `DD-MM-YYYY`)
   - Copies (message text)
   - Control group %

2. **Stats tab** (`/report/stats`)
   - Qualified, Sent, Delivered, Viewed, Clicks
   - Control group count

3. **Conversion tab** (`/report/stats/conversion`)
   - Revenue (see differences between SMS/Shivam vs SMS/Gurkirat below)

All scraping uses `page.inner_text("body")` and regex patterns. CleverTap's dashboard is a Vue SPA so the scripts wait for specific text strings to appear before parsing.

### Revenue logic differences

**SMS / Shivam:**
- `total_revenue` = Target Group Revenue + Control Group Revenue
- `incremental_revenue` = "Incremental revenue due to this campaign" label value; falls back to `total_revenue` if N/A

**SMS / Gurkirat and WhatsApp / Gurkirat:**
- `total_revenue` = Target Group Revenue only
- `incremental_revenue` = Control Group Revenue only; falls back to Target Group Revenue if Control is N/A or 0

### Copies extraction

**SMS / Shivam:** Extracts message text using `Message:` label regex from the overview body text.

**SMS / Gurkirat:** Extracts using the `.text-message-preview` selector, taking everything above "ENROLL NOW" in the message preview. Falls back to URL redirect line or Template Name line if the primary pattern doesn't match.

**WhatsApp:** Uses `page.locator(".text-message-preview").first.inner_text()` from the phone mockup preview widget.

---

## Google Sheets write logic

Both `csv_to_sheets.py` and `wa_to_sheets.py` share the same upsert strategy:

1. Read row 1 of the sheet to get column headers
2. Map headers to internal data keys using `SHEET_COLUMN_MAP`
3. For each campaign, check if a row with that campaign name already exists
   - If yes → update in place (by row number)
   - If no → append as a new row after the last existing row
4. Never overwrite a cell with an empty value — manual entries are preserved
5. Formula columns (Delivery Rate, Cost, RoI, Surplus) and manual columns (Sender, cohort) are never touched because they are not in `SHEET_COLUMN_MAP`

### Date normalisation and sorting

After every write, the sheet is sorted ascending by the date column and all dates are normalised to a consistent format. The logic handles two legacy formats that may coexist in the sheet:

- `DD-MM-YYYY` (current standard)
- `MM-DD-YYYY` (legacy)

**Resolution strategy:**
1. If the first part is >12 → unambiguously `DD-MM-YYYY`
2. If the second part is >12 → unambiguously `MM-DD-YYYY`
3. For ambiguous dates (both parts ≤ 12), use the date range established by the unambiguous rows as context — pick whichever interpretation lands inside the known range. If both or neither land inside, prefer the earlier date.

The separator style (`-` vs `/`) is detected from existing rows so newly written dates match the sheet's convention.

---

## CLI options (all browser sync scripts)

```
python ct_browser_sync.py --setup
    First-time login: opens a visible browser, you handle MFA OTP,
    browser profile is saved to ct_browser_profile/

python ct_browser_sync.py
    Normal daily run — loads saved session, scrapes yesterday's campaigns

python ct_browser_sync.py --verify-week
    Re-scrapes all campaigns from the current Mon–Sun week.
    Also runs automatically when the script is invoked on a Sunday.

python ct_browser_sync.py --start 2026-05-01 --end 2026-05-21
    Custom date range

python ct_browser_sync.py --scrape-ids 12345,67890
    Scrape specific campaign IDs directly (bypasses the list page entirely)

python ct_browser_sync.py --dry-run
    Scrapes data and logs it but does not write to the sheet

python ct_browser_sync.py --creator gurkirat@goodeducator.com --worksheet-gid 75277204
    Override the creator filter and target sheet tab at runtime
```

The Gurkirat SMS sync has a convenience wrapper:
```bash
./sms_gurkirat/run.sh --start 2026-05-01 --end 2026-05-21
```

---

## Manual CSV import (fallback)

If browser automation is not available, you can export a CSV from CleverTap and import it:

1. CleverTap → Campaigns → filter Channel=SMS, Created by=your email
2. Click the export/download icon
3. Run:
```bash
cd sms_shivam
python csv_to_sheets.py path/to/clevertap_export.csv
```

The CSV column names are mapped via `CSV_COLUMN_MAP`. Many CleverTap header name variants are handled (e.g. "Estimated Reach", "Qualified Users", "Eligible Users" all map to the `qualified` field). Revenue and formula columns are intentionally not mapped — those are manual entries in the sheet.

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure environment

Copy `.env.example` to `.env` and fill in:

```env
# CleverTap dashboard login
CT_LOGIN_EMAIL=your@email.com
CT_LOGIN_PASSWORD=yourpassword
CLEVERTAP_ACCOUNT_ID=your_account_id
CLEVERTAP_REGION=eu1          # eu1, in1, us1, sg1

# Creator filter (default for sms_shivam)
FILTER_CREATOR=shivamprashar@goodeducator.com

# Google Sheets
SPREADSHEET_ID=your_spreadsheet_id
WORKSHEET_GID=470166044       # Shivam SMS tab
WA_WORKSHEET_GID=0            # WhatsApp tab
```

### 3. Google service account credentials

Place your Google service account JSON file at `credentials.json` in the repo root (gitignored). The service account needs Editor access on the spreadsheet.

### 4. First-time session setup

Run setup from the repo root (all three automations share the same browser profile):

```bash
cd sms_shivam
python ct_browser_sync.py --setup
```

A Chromium window opens. Enter your OTP, tick "Remember this device for 30 days", and submit. The profile is saved automatically to `ct_browser_profile/`. You do not need to repeat this for 30 days unless your IP changes or the window expires.

---

## Daily usage

```bash
# SMS Shivam — yesterday's campaigns
cd sms_shivam && python ct_browser_sync.py

# SMS Gurkirat — yesterday's campaigns
cd sms_gurkirat && python ct_browser_sync.py

# WhatsApp Gurkirat — yesterday's campaigns
python wa_browser_sync.py
```

---

## Files that are gitignored

| File / folder | Why |
|---|---|
| `.env` | Contains credentials |
| `credentials.json` | Google service account private key |
| `ct_session.json` | Legacy session token (superseded by browser profile) |
| `ct_browser_profile/` | Full browser profile with device fingerprint and cookies |
