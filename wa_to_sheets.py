"""
WhatsApp campaign data → Google Sheets
----------------------------------------
Writes scraped campaign rows to the WhatsApp sheet (GID=0).
Formula columns (Delivery Rate, Cost, RoI, Surplus) are never touched.
Manual columns (Sender, cohort) are never touched.
After every write, sorts all rows by date ascending and normalises dates to DD-MM-YYYY.
"""

import os
import logging
import gspread
from datetime import date
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

log = logging.getLogger(__name__)

GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDENTIALS_JSON", "credentials.json")
SPREADSHEET_ID    = os.getenv("SPREADSHEET_ID")
WA_WORKSHEET_GID  = int(os.getenv("WA_WORKSHEET_GID", "0"))

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Maps data keys → sheet column headers
SHEET_COLUMN_MAP = {
    "send_date":           "date",
    "name":                "campaign name",
    "exam_year":           "exam year",
    "copies":              "copies",
    "video":               "video",
    "video_type":          "video type",
    "sent":                "sent",
    "delivered":           "delivered",
    "viewed":              "viewed",
    "clicks":              "Clicks",
    "total_revenue":       "revenue",
    "incremental_revenue": "above baseline",
}


def get_sheet():
    creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=GOOGLE_SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    ws = spreadsheet.get_worksheet_by_id(WA_WORKSHEET_GID)
    if ws is None:
        raise RuntimeError(f"Worksheet GID {WA_WORKSHEET_GID} not found.")
    return ws


# ── Date helpers (same logic as csv_to_sheets.py) ─────────────────────────────

def _col_letter(n):
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _try_parse(a, b, y):
    try:
        return date(y, b, a)   # DD-MM: a=day, b=month
    except ValueError:
        return None


def _try_parse_mmdd(a, b, y):
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
        return (d, d.strftime("%d-%m-%Y")) if d else (None, s)

    if b > 12:                          # unambiguous: MM-DD-YYYY (legacy)
        d = _try_parse_mmdd(a, b, y)
        return (d, d.strftime("%d-%m-%Y")) if d else (None, s)

    # Ambiguous – both parts ≤ 12
    d_ddmm = _try_parse(a, b, y)
    d_mmdd = _try_parse_mmdd(a, b, y)

    if d_ddmm is None and d_mmdd is None:
        return None, s
    if d_ddmm is None:
        return d_mmdd, d_mmdd.strftime("%d-%m-%Y")
    if d_mmdd is None:
        return d_ddmm, d_ddmm.strftime("%d-%m-%Y")

    if min_known and max_known:
        in_range_ddmm = min_known <= d_ddmm <= max_known
        in_range_mmdd = min_known <= d_mmdd <= max_known
        if in_range_ddmm and not in_range_mmdd:
            chosen = d_ddmm
        elif in_range_mmdd and not in_range_ddmm:
            chosen = d_mmdd
        else:
            chosen = min(d_ddmm, d_mmdd)
    else:
        chosen = min(d_ddmm, d_mmdd)

    return chosen, chosen.strftime("%d-%m-%Y")


# ── Main write function ────────────────────────────────────────────────────────

def write_to_wa_sheet(rows: list[dict]):
    """
    Append new campaigns / update existing ones in the WA sheet.
    Rows must be pre-sorted by date before calling this function.
    Formula columns (Delivery Rate, Cost, RoI, Surplus) and manual columns
    (Sender, cohort) are never touched. No reordering of existing rows.
    """
    if not rows:
        log.info("No data to write.")
        return

    ws = get_sheet()

    sheet_headers = [h.strip() for h in ws.row_values(1)]
    if not sheet_headers:
        log.error("Row 1 of the WA sheet is empty — no headers found.")
        return

    log.info("Sheet headers: %s", sheet_headers)

    # Map column index (1-based) → data key
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
        (i for i, h in enumerate(sheet_headers) if h.strip().lower() == "campaign name"),
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
                continue
            updates.append({
                "range":  gspread.utils.rowcol_to_a1(row_idx, col_idx),
                "values": [[val]],
            })

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")

    log.info("Done: %d new row(s) added, %d updated. Formula/manual columns untouched.",
             new_count, updated_count)
