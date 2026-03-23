"""
CleverTap CSV → Google Sheets
------------------------------
Usage:
    python csv_to_sheets.py path/to/clevertap_export.csv

How to export from CleverTap:
    1. Go to Campaigns → filter Channel = SMS, Created by = shivamprashar@goodeducator.com
    2. Click the download/export icon (top right of campaign list)
    3. Save the CSV anywhere, then run this script pointing to it.

What this script does:
    - Reads the CSV
    - Maps CleverTap columns → your sheet columns
    - Appends NEW campaigns as new rows
    - Updates EXISTING campaigns (matched by name) in place
    - Never touches formula columns (Delivery Rate, Cost, RoI, Surplus)
"""

import os
import sys
import csv
import logging
import gspread
from datetime import datetime, date
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDENTIALS_JSON", "credentials.json")
SPREADSHEET_ID    = os.getenv("SPREADSHEET_ID")
WORKSHEET_GID     = int(os.getenv("WORKSHEET_GID", "470166044"))

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Maps CleverTap CSV column headers (lowercase, stripped) → sheet data keys.
# CleverTap's CSV export uses various header names — all common variants listed.
CSV_COLUMN_MAP = {
    # Date
    "date":                     "send_date",
    "scheduled date":           "send_date",
    "send date":                "send_date",
    "start date":               "send_date",
    "created date":             "send_date",

    # Sender
    "sender":                   "sender",
    "sender id":                "sender",
    "from":                     "sender",
    "from number":              "sender",

    # Campaign name
    "campaign name":            "name",
    "name":                     "name",

    # Copies — the SMS message text
    "message":                  "copies",
    "copies":                   "copies",
    "variants":                 "copies",
    "ab variants":              "copies",

    # Qualified users — use estimated reach (numeric) from CleverTap export
    "estimated reach":          "qualified",
    "qualified":                "qualified",
    "qualified users":          "qualified",
    "eligible users":           "qualified",
    # Note: "reach" is intentionally excluded — CleverTap CSV has a "Reach" column
    # with the value "All active devices" which is not numeric.

    # Control group — left blank (manual entry from dashboard)
    # "control group": intentionally not mapped

    # Sent
    "sent":                     "sent",
    "pushed":                   "sent",
    "total sent":               "sent",
    "total sent(users)":        "sent",

    # Delivered
    "delivered":                "delivered",
    "deliveries":               "delivered",
    "total delivered":          "delivered",
    "total delivered(users)":   "delivered",

    # ── Revenue / formula columns — intentionally NOT mapped (manual entry) ──
    # "influenced revenue", "total revenue", "incemental revenue",
    # "delivery rate", "cost", "roi(revenue/cost)", "surplus"
}

# Maps data keys → sheet column headers (for writing)
SHEET_COLUMN_MAP = {
    "send_date":           "date",
    "sender":              "sender",
    "name":                "campaign name",
    "copies":              "copies",
    "qualified":           "qualified users",
    "control_group_pct":   "control group",
    "sent":                "sent",
    "delivered":           "delivered",
    "total_revenue":       "total revenue",
    "incremental_revenue": "incemental revenue",   # matches your sheet's spelling
}


# ── CSV parsing ───────────────────────────────────────────────────────────────

def parse_number(val: str):
    """Strip commas/% and return int or float, or original string."""
    v = val.strip().replace(",", "").replace("%", "").strip()
    try:
        return int(v)
    except ValueError:
        try:
            return float(v)
        except ValueError:
            return val.strip()


def read_csv(path: str) -> list[dict]:
    """Read CleverTap CSV and return list of normalised row dicts."""
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        raw_headers = reader.fieldnames or []
        log.info("CSV headers found: %s", raw_headers)

        for raw_row in reader:
            row = {}
            control_revenue = None
            target_revenue  = None

            for raw_col, raw_val in raw_row.items():
                key = CSV_COLUMN_MAP.get(raw_col.strip().lower())
                if not key:
                    continue
                val = parse_number(raw_val)

                if key == "control_revenue":
                    control_revenue = val
                elif key == "total_revenue":
                    target_revenue = val
                    row[key] = val
                else:
                    row[key] = val

            # Compute incremental revenue if both values present
            if "incremental_revenue" not in row:
                if isinstance(target_revenue, (int, float)) and isinstance(control_revenue, (int, float)):
                    row["incremental_revenue"] = target_revenue - control_revenue
                elif target_revenue is not None:
                    row["incremental_revenue"] = ""

            if row.get("name"):   # skip blank rows
                rows.append(row)

    log.info("Parsed %d campaign row(s) from CSV.", len(rows))
    return rows


# ── Google Sheets ─────────────────────────────────────────────────────────────

def get_sheet():
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=GOOGLE_SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    ws = spreadsheet.get_worksheet_by_id(WORKSHEET_GID)
    if ws is None:
        raise RuntimeError(f"Worksheet GID {WORKSHEET_GID} not found.")
    return ws


def write_to_sheet(rows: list[dict]):
    """
    Append new campaigns / update existing ones.
    Formula columns (Delivery Rate, Cost, RoI, Surplus) are never touched.
    """
    if not rows:
        log.info("No data to write.")
        return

    ws = get_sheet()

    # Read existing sheet headers from row 1
    sheet_headers = [h.strip() for h in ws.row_values(1)]
    if not sheet_headers:
        log.error("Row 1 of the sheet is empty — no headers found.")
        return

    log.info("Sheet headers: %s", sheet_headers)

    # Map sheet column index (1-based) → data key
    writable = {}
    for idx, header in enumerate(sheet_headers, start=1):
        for data_key, sheet_col in SHEET_COLUMN_MAP.items():
            if header.strip().lower() == sheet_col.lower():
                writable[idx] = data_key
                break

    log.info("Will write to columns: %s",
             {sheet_headers[i-1]: k for i, k in writable.items()})

    # Find campaign name column for dedup
    name_col_idx = next(
        (i for i, h in enumerate(sheet_headers)
         if h.strip().lower() == "campaign name"),
        None,
    )
    existing_names = {}
    if name_col_idx is not None:
        col_vals = ws.col_values(name_col_idx + 1)
        for row_num, val in enumerate(col_vals[1:], start=2):
            if val.strip():
                existing_names[val.strip()] = row_num

    new_count = updated_count = 0
    updates = []

    for row_data in rows:
        campaign_name = str(row_data.get("name", "")).strip()
        if not campaign_name:
            continue

        if campaign_name in existing_names:
            row_idx = existing_names[campaign_name]
            updated_count += 1
        else:
            row_idx = max(existing_names.values(), default=1) + 1
            existing_names[campaign_name] = row_idx
            new_count += 1

        for col_idx, data_key in writable.items():
            val = row_data.get(data_key, "")
            if val == "" or val is None:
                continue   # never overwrite manual entries with empty values
            updates.append({
                "range":  gspread.utils.rowcol_to_a1(row_idx, col_idx),
                "values": [[row_data.get(data_key, "")]],
            })

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")

    # ── Sort + normalise dates ─────────────────────────────────────────────────
    # The sheet may contain dates in two formats:
    #   DD-MM-YYYY  (current standard, e.g. "19-03-2026")
    #   MM-DD-YYYY  (legacy, e.g. "03-13-2026" = March 13)
    # Strategy:
    #   1. Parse every date; if first part >12 → DD-MM, if second part >12 → MM-DD.
    #   2. For ambiguous dates (both parts ≤12) use the full date range from the
    #      UNambiguous rows as context: pick the interpretation that lands inside
    #      [min_unambiguous, max_unambiguous].  If both land inside (or both outside),
    #      prefer the earlier date.
    #   3. Normalise every date to DD-MM-YYYY in the sheet so future runs are clean.
    #   4. Sort all data rows ascending by the resolved date.

    date_col_idx = next(
        (i for i, h in enumerate(sheet_headers, start=1)
         if h.strip().lower() in ("date", "send date", "scheduled date")),
        None,
    )

    def _col_letter(n):
        s = ""
        while n:
            n, r = divmod(n - 1, 26)
            s = chr(65 + r) + s
        return s

    def _try_parse(a, b, y):
        """Return date(y, month, day) or None."""
        try:
            return date(y, b, a)   # DD-MM: a=day, b=month
        except ValueError:
            return None

    def _try_parse_mmdd(a, b, y):
        """Return date(y, month, day) for MM-DD order or None."""
        try:
            return date(y, a, b)   # MM-DD: a=month, b=day
        except ValueError:
            return None

    def _resolve_date(s, min_known=None, max_known=None):
        """Parse a date string to a date object; normalise to DD-MM-YYYY string."""
        try:
            parts = s.strip().split("-")
            if len(parts) != 3:
                return None, s
            a, b, y = int(parts[0]), int(parts[1]), int(parts[2])
        except (ValueError, IndexError):
            return None, s

        if a > 12:                          # unambiguous: DD-MM-YYYY
            d = _try_parse(a, b, y)
            return d, d.strftime("%d-%m-%Y") if d else (None, s)

        if b > 12:                          # unambiguous: MM-DD-YYYY (legacy)
            d = _try_parse_mmdd(a, b, y)
            return d, d.strftime("%d-%m-%Y") if d else (None, s)

        # Ambiguous – both parts ≤ 12
        d_ddmm = _try_parse(a, b, y)       # DD-MM interpretation
        d_mmdd = _try_parse_mmdd(a, b, y)  # MM-DD interpretation

        if d_ddmm is None and d_mmdd is None:
            return None, s
        if d_ddmm is None:
            return d_mmdd, d_mmdd.strftime("%d-%m-%Y")
        if d_mmdd is None:
            return d_ddmm, d_ddmm.strftime("%d-%m-%Y")

        # Use the known date range to pick the right interpretation
        if min_known and max_known:
            in_range_ddmm = min_known <= d_ddmm <= max_known
            in_range_mmdd = min_known <= d_mmdd <= max_known
            if in_range_ddmm and not in_range_mmdd:
                chosen = d_ddmm
            elif in_range_mmdd and not in_range_ddmm:
                chosen = d_mmdd
            else:
                chosen = min(d_ddmm, d_mmdd)   # both/neither in range → take earlier
        else:
            chosen = min(d_ddmm, d_mmdd)

        return chosen, chosen.strftime("%d-%m-%Y")

    if date_col_idx:
        all_values = ws.get_all_values()
        data_rows = all_values[1:]          # skip header row

        if len(data_rows) > 1:
            date_col_0 = date_col_idx - 1  # 0-based

            # Pass 1: collect unambiguous dates to build the known range
            known_dates = []
            for r in data_rows:
                s = r[date_col_0].strip() if date_col_0 < len(r) else ""
                if not s:
                    continue
                try:
                    parts = s.split("-")
                    a, b = int(parts[0]), int(parts[1])
                    if a > 12 or b > 12:    # unambiguous
                        d, _ = _resolve_date(s)
                        if d:
                            known_dates.append(d)
                except (ValueError, IndexError):
                    pass

            min_known = min(known_dates) if known_dates else None
            max_known = max(known_dates) if known_dates else None
            log.info("Unambiguous date range in sheet: %s → %s", min_known, max_known)

            # Pass 2: resolve + normalise every row's date
            resolved = []   # list of (date_obj, normalised_string, original_row)
            for r in data_rows:
                s = r[date_col_0].strip() if date_col_0 < len(r) else ""
                d, norm = _resolve_date(s, min_known, max_known)
                resolved.append((d or date.min, norm, r))

            # Sort ascending by resolved date
            resolved.sort(key=lambda x: x[0])

            # Build batch update: rewrite writable columns + normalised date column
            sort_updates = []
            for col_idx in writable:
                col_vals = [
                    [row[col_idx - 1] if col_idx - 1 < len(row) else ""]
                    for _, _, row in resolved
                ]
                cl = _col_letter(col_idx)
                sort_updates.append({
                    "range":  f"{cl}2:{cl}{1 + len(resolved)}",
                    "values": col_vals,
                })

            # Also rewrite the date column itself with normalised DD-MM-YYYY strings
            date_cl = _col_letter(date_col_idx)
            sort_updates.append({
                "range":  f"{date_cl}2:{date_cl}{1 + len(resolved)}",
                "values": [[norm] for _, norm, _ in resolved],
            })

            if sort_updates:
                ws.batch_update(sort_updates, value_input_option="USER_ENTERED")
                log.info("Sheet sorted by date (asc) and all dates normalised to DD-MM-YYYY.")

    log.info("Done: %d new row(s) added, %d updated. Formula columns untouched.",
             new_count, updated_count)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Usage: python csv_to_sheets.py <path_to_clevertap_export.csv>")
        sys.exit(1)

    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        sys.exit(1)

    if not SPREADSHEET_ID:
        print("ERROR: SPREADSHEET_ID not set in .env")
        sys.exit(1)

    log.info("Reading CSV: %s", csv_path)
    rows = read_csv(csv_path)
    write_to_sheet(rows)


if __name__ == "__main__":
    main()
