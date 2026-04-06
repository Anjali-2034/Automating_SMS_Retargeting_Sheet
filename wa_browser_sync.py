"""
CleverTap WhatsApp → Google Sheets
------------------------------------
Scrapes WhatsApp campaigns by gurkirat@goodeducator.com directly from
the CleverTap dashboard and syncs data to the WhatsApp Google Sheet.

Uses the same persistent browser profile as ct_browser_sync.py.
Run --setup from ct_browser_sync.py if session is expired.

Usage:
    python wa_browser_sync.py                            # normal daily run (yesterday)
    python wa_browser_sync.py --start 2026-03-28 --end 2026-04-05   # backfill
    python wa_browser_sync.py --verify-week              # re-verify full current week
"""

import os
import re
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from wa_to_sheets import write_to_wa_sheet

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

CT_REGION      = os.getenv("CLEVERTAP_REGION", "eu1")
CT_ACCOUNT_ID  = os.getenv("CLEVERTAP_ACCOUNT_ID")
FILTER_CREATOR = os.getenv("WA_FILTER_CREATOR", "gurkirat@goodeducator.com")

BASE_URL      = f"https://{CT_REGION}.dashboard.clevertap.com"
CAMPAIGNS_URL = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/whatsapp"

NAV_TIMEOUT  = 30_000
PROFILE_DIR  = Path(__file__).parent / "ct_browser_profile"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _clean_number(text: str):
    v = text.strip().replace(",", "").replace("%", "").strip()
    try:
        return int(v)
    except ValueError:
        try:
            return float(v)
        except ValueError:
            return text.strip()


def _wait_for_text(page, text: str, timeout: int = 20_000):
    try:
        page.wait_for_function(
            f'document.body.innerText.includes({repr(text)})',
            timeout=timeout,
        )
    except PlaywrightTimeout:
        log.warning("Timeout waiting for text: %r", text)


def current_week_range() -> tuple[date, date]:
    today = date.today()
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


# ── Date filter (same picker as SMS campaigns) ─────────────────────────────────

def _set_date_filter(page, start: date, end: date):
    try:
        page.locator(".lp-daterangepicker").click()
        page.wait_for_selector(".lp-daterangepicker-dropdown", timeout=10_000)
        time.sleep(1)

        fmt = lambda d: d.strftime("%b %d, %Y")

        inputs = page.locator(".lp-daterangepicker-dropdown input.lp-text-input")
        inputs.nth(0).click(click_count=3)
        inputs.nth(0).fill(fmt(start))
        inputs.nth(1).click(click_count=3)
        inputs.nth(1).fill(fmt(end))

        page.locator(".lp-daterangepicker-dropdown .lp-button.color-primary").click()
        time.sleep(3)
        log.info("Date filter set: %s → %s", fmt(start), fmt(end))
    except Exception as e:
        log.warning("Could not set date filter: %s", e)


def _scroll_to_load_all(page):
    try:
        page.wait_for_selector(".lp-table-body", timeout=10_000)
    except PlaywrightTimeout:
        return

    stable_streak = 0
    prev_count = -1
    for _ in range(50):
        count = page.locator(".lp-table-row.bordered").count()
        if count == prev_count:
            stable_streak += 1
            if stable_streak >= 3:
                break
        else:
            stable_streak = 0
        prev_count = count
        page.evaluate('''() => {
            const el = document.querySelector(".lp-table-body");
            if (el) el.scrollTop = el.scrollHeight;
            window.scrollTo(0, document.body.scrollHeight);
        }''')
        time.sleep(1)


# ── Campaign list ──────────────────────────────────────────────────────────────

def _extract_campaigns_from_page(page) -> list[dict]:
    _scroll_to_load_all(page)
    return page.evaluate(f'''() => {{
        const rows = document.querySelectorAll(".lp-table-row.bordered");
        const results = [];
        for (const row of rows) {{
            if (!row.innerText.includes("Created by: {FILTER_CREATOR}")) continue;
            const linkEl = row.querySelector("a.campaign-details-link");
            if (!linkEl) continue;
            const name = linkEl.innerText.trim();
            const href = linkEl.getAttribute("href") || "";
            const idMatch = href.match(/campaign\\/([\\d]+)/);
            const campaignId = idMatch ? idMatch[1] : null;
            if (campaignId) results.push({{name, campaignId}});
        }}
        return results;
    }}''')


def get_campaign_list(page, start_date: date | None = None, end_date: date | None = None) -> list[dict]:
    log.info("Loading WhatsApp campaigns list...")
    page.goto(CAMPAIGNS_URL, wait_until="networkidle", timeout=NAV_TIMEOUT)

    try:
        page.wait_for_selector(".lp-table-row.bordered", timeout=20_000)
    except PlaywrightTimeout:
        log.error("Campaign list did not load.")
        return []

    time.sleep(2)

    if start_date and end_date:
        # Iterate day-by-day to avoid display limits cutting off campaigns
        all_campaigns: dict[str, dict] = {}
        current = start_date
        while current <= end_date:
            _set_date_filter(page, current, current)
            day_campaigns = _extract_campaigns_from_page(page)
            new = [c for c in day_campaigns if c["campaignId"] not in all_campaigns]
            for c in new:
                all_campaigns[c["campaignId"]] = c
            log.info("  %s: %d campaign(s) found (%d new)",
                     current.strftime("%d-%m-%Y"), len(day_campaigns), len(new))
            current += timedelta(days=1)
        campaigns = list(all_campaigns.values())
        log.info("Total: %d campaign(s) in range %s – %s", len(campaigns), start_date, end_date)
    else:
        campaigns = _extract_campaigns_from_page(page)
        log.info("Found %d campaign(s) by %s", len(campaigns), FILTER_CREATOR)

    return campaigns


# ── Per-campaign scraping ──────────────────────────────────────────────────────

def scrape_overview(page, campaign_id: str) -> dict:
    url = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/campaign/{campaign_id}/report/overview"
    page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    _wait_for_text(page, "Message Type:", timeout=30_000)

    text = page.inner_text("body")

    # Date
    send_date = ""
    m = re.search(r'\btime\n(.+)', text)
    if m:
        try:
            dt = datetime.strptime(m.group(1).strip(), "%b %d, %Y, %I:%M %p")
            send_date = dt.strftime("%-d-%m-%Y")
        except ValueError:
            send_date = m.group(1).strip()

    # Copies
    copies = ""
    m = re.search(r'(?:Enabled|Disabled)\s*\n\n(.+?)\nWhen\b', text, re.DOTALL)
    if m:
        copies = m.group(1).strip()

    # Deep Link → video type
    video_type = ""
    m = re.search(r'Deep Link:\s*(\S+)', text)
    if m:
        video_type = m.group(1).strip()

    # Control group %
    control_group_pct = ""
    m = re.search(r'Campaign Control Group\n(\d+%)', text)
    if m:
        control_group_pct = m.group(1).strip()

    return {
        "send_date":         send_date,
        "copies":            copies,
        "video_type":        video_type,
        "control_group_pct": control_group_pct,
    }


def scrape_stats(page, campaign_id: str) -> dict:
    url = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/campaign/{campaign_id}/report/stats"
    page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    _wait_for_text(page, "Sent", timeout=30_000)
    time.sleep(4)

    text = page.inner_text("body")
    idx = text.find("Sent")
    section = text[idx:idx + 500] if idx >= 0 else text

    sent = ""
    m = re.search(r'Sent\s*\n([\d,]+)', section)
    if m:
        sent = _clean_number(m.group(1))

    delivered = ""
    m = re.search(r'Impressions\s*\n[\d.]+%\n([\d,]+)', section)
    if m:
        delivered = _clean_number(m.group(1))

    viewed = ""
    m = re.search(r'Clicks\s*\n[\d.]+%\n([\d,]+)', section)
    if m:
        viewed = _clean_number(m.group(1))

    return {"sent": sent, "delivered": delivered, "viewed": viewed}


def scrape_conversion(page, campaign_id: str) -> dict:
    url = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/campaign/{campaign_id}/report/stats/conversion"
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    _wait_for_text(page, "Revenue performance", timeout=30_000)
    time.sleep(2)

    text = page.inner_text("body")
    idx = text.find("Revenue performance")
    section = text[idx:idx + 400] if idx >= 0 else text

    total_revenue = ""
    m = re.search(r'Target Group Revenue\s*\n([\d,]+(?:\.\d+)?)', section)
    if m:
        total_revenue = _clean_number(m.group(1))

    incremental_revenue = ""
    m = re.search(r'Incremental revenue due to this campaign\s*\n([\d,]+(?:\.\d+)?)', section)
    if m:
        incremental_revenue = _clean_number(m.group(1))

    return {"total_revenue": total_revenue, "incremental_revenue": incremental_revenue}


# ── Orchestrator ───────────────────────────────────────────────────────────────

def scrape_all_campaigns(page, start_date: date | None = None, end_date: date | None = None) -> list[dict]:
    campaigns = get_campaign_list(page, start_date=start_date, end_date=end_date)
    if not campaigns:
        log.warning("No campaigns found for creator: %s", FILTER_CREATOR)
        return []

    rows = []
    for i, c in enumerate(campaigns, 1):
        name = c["name"]
        cid  = c["campaignId"]
        campaign_url = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/campaign/{cid}/report/stats"
        log.info("[%d/%d] Scraping: %s (ID: %s)", i, len(campaigns), name, cid)

        try:
            overview   = scrape_overview(page, cid)
            stats      = scrape_stats(page, cid)
            conversion = scrape_conversion(page, cid)

            copies = overview.get("copies", "")
            exam_year = ""
            m = re.search(r'\b(20\d\d)\b', copies)
            if m:
                exam_year = m.group(1)

            row = {"name": name, "video": campaign_url, "exam_year": exam_year}
            row.update(overview)
            row.update(stats)
            row.update(conversion)
            rows.append(row)

            log.info(
                "  date=%s  sent=%s  delivered=%s  viewed=%s  revenue=%s  incr=%s",
                row.get("send_date"), row.get("sent"), row.get("delivered"),
                row.get("viewed"), row.get("total_revenue"), row.get("incremental_revenue"),
            )
        except Exception as e:
            log.error("  Failed to scrape campaign %s: %s", name, e)

    log.info("Scraped %d campaign(s) total.", len(rows))
    return rows


# ── Run ────────────────────────────────────────────────────────────────────────

def run(verify_week: bool = False, start_date: date | None = None, end_date: date | None = None):
    if not PROFILE_DIR.exists():
        raise SystemExit(
            "No saved session found. Run setup first:\n"
            "    python ct_browser_sync.py --setup"
        )

    if start_date and end_date:
        log.info("Custom date range: %s to %s", start_date, end_date)
    else:
        is_sunday = date.today().weekday() == 6
        if is_sunday and not verify_week:
            log.info("Today is Sunday — automatically running weekly verification.")
            verify_week = True

        if verify_week:
            start_date, end_date = current_week_range()
            log.info("Weekly verify mode: %s to %s", start_date, end_date)
        else:
            yesterday = date.today() - timedelta(days=1)
            start_date = end_date = yesterday
            log.info("Daily run: %s", yesterday)

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
            viewport={"width": 1440, "height": 900},
        )
        page = ctx.new_page()
        page.set_default_timeout(NAV_TIMEOUT)

        page.goto(CAMPAIGNS_URL, wait_until="networkidle", timeout=NAV_TIMEOUT)
        if "sso.clevertap.com" in page.url or "clevertap.com/login" in page.url:
            raise SystemExit(
                "Session expired. Re-run setup:\n"
                "    python ct_browser_sync.py --setup"
            )

        rows = scrape_all_campaigns(page, start_date=start_date, end_date=end_date)
        ctx.close()

    if not rows:
        log.warning("No data scraped — nothing written to sheet.")
        return

    write_to_wa_sheet(rows)
    log.info("=== Sync complete ===")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="CleverTap WhatsApp → Google Sheets"
    )
    parser.add_argument("--verify-week", action="store_true",
                        help="Re-scrape and verify all campaigns from the current Mon–Sun week")
    parser.add_argument("--start", type=str, default=None, metavar="YYYY-MM-DD",
                        help="Start date for custom date range (inclusive)")
    parser.add_argument("--end", type=str, default=None, metavar="YYYY-MM-DD",
                        help="End date for custom date range (inclusive)")
    args = parser.parse_args()

    start_date = date.fromisoformat(args.start) if args.start else None
    end_date   = date.fromisoformat(args.end)   if args.end   else None
    run(verify_week=args.verify_week, start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    main()
