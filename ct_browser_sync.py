"""
CleverTap Browser Automation → Google Sheets
---------------------------------------------
Logs into CleverTap via browser, scrapes SMS campaign data directly
from the dashboard (no CSV export), then syncs it to Google Sheets.

CleverTap uses MFA, so sessions are handled in two modes:

  --setup   Opens a VISIBLE browser. You log in manually and handle the MFA
            OTP. Tick "Remember this device for 30 days" before submitting.
            The full browser profile is saved to ct_browser_profile/ so that
            CleverTap recognises the same "device" on re-login — meaning after
            the first MFA you should only need to re-enter your password when
            the session expires, NOT the OTP again (within the 30-day window).

  (normal)  Loads the saved profile/session, skips login entirely, goes
            straight to campaigns. If the session has expired, re-run --setup.

Add to .env:
    CT_LOGIN_EMAIL=your@email.com
    CT_LOGIN_PASSWORD=yourpassword
    FILTER_CREATOR=shivamprashar@goodeducator.com

Usage:
    python ct_browser_sync.py --setup          # first-time / session expired
    python ct_browser_sync.py                  # normal daily run
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

from csv_to_sheets import write_to_sheet

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

CT_LOGIN_EMAIL    = os.getenv("CT_LOGIN_EMAIL")
CT_LOGIN_PASSWORD = os.getenv("CT_LOGIN_PASSWORD")
CT_REGION         = os.getenv("CLEVERTAP_REGION", "eu1")
CT_ACCOUNT_ID     = os.getenv("CLEVERTAP_ACCOUNT_ID")
FILTER_CREATOR    = os.getenv("FILTER_CREATOR", "shivamprashar@goodeducator.com")

BASE_URL      = f"https://{CT_REGION}.dashboard.clevertap.com"
CAMPAIGNS_URL = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/sms"

NAV_TIMEOUT  = 30_000
SESSION_FILE = Path(__file__).parent / "ct_session.json"   # legacy / CI fallback
PROFILE_DIR  = Path(__file__).parent / "ct_browser_profile"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _clean_number(text: str):
    """Strip commas/% and return int, float, or original string.
    Handles negatives including CleverTap's double-dash notation (--27,903 → -27903)."""
    v = text.strip().replace(",", "").replace("%", "").strip()
    # CleverTap renders negative incremental revenue as "--N" — normalise to "-N"
    if v.startswith("--"):
        v = "-" + v[2:]
    try:
        return int(v)
    except ValueError:
        try:
            return float(v)
        except ValueError:
            return text.strip()


def _wait_for_text(page, text: str, timeout: int = 20_000):
    """Wait until the given text appears in the page body."""
    try:
        page.wait_for_function(
            f'document.body.innerText.includes({repr(text)})',
            timeout=timeout,
        )
    except PlaywrightTimeout:
        log.warning("Timeout waiting for text: %r", text)


# ── Week date range helpers ────────────────────────────────────────────────────

def current_week_range() -> tuple[date, date]:
    """Return (monday, sunday) of the current week."""
    today = date.today()
    monday = today - timedelta(days=today.weekday())      # weekday(): Mon=0, Sun=6
    sunday = monday + timedelta(days=6)
    return monday, sunday


def parse_list_date(start_time_raw: str) -> date | None:
    """
    Parse the start time shown in the campaign list, e.g. 'Mar 19, 7:30 PM'.
    CleverTap omits the year — we assume the current year (handles Dec/Jan edge case).
    """
    for fmt in ("%b %d, %I:%M %p", "%b %d, %Y, %I:%M %p"):
        try:
            dt = datetime.strptime(start_time_raw.strip(), fmt)
            if dt.year == 1900:                          # strptime default when year missing
                dt = dt.replace(year=date.today().year)
            return dt.date()
        except ValueError:
            continue
    return None


# ── Campaign list scraping ─────────────────────────────────────────────────────

def _set_date_filter(page, start: date, end: date):
    """
    Set the CleverTap campaign list date-range picker to [start, end] and Apply.
    This ensures the list only shows campaigns from the target window so that
    ALL campaigns in range are loaded — no scroll-limit surprises.
    """
    try:
        page.locator(".lp-daterangepicker").click()
        page.wait_for_selector(".lp-daterangepicker-dropdown", timeout=10_000)
        time.sleep(1)

        # Format: "Mar 16, 2026"
        fmt = lambda d: d.strftime("%b %d, %Y")

        inputs = page.locator(".lp-daterangepicker-dropdown input.lp-text-input")
        # Start date — select all then fill
        inputs.nth(0).click(click_count=3)
        inputs.nth(0).fill(fmt(start))
        time.sleep(0.5)
        # End date
        inputs.nth(1).click(click_count=3)
        inputs.nth(1).fill(fmt(end))
        time.sleep(0.5)

        # Click Apply
        page.locator(".lp-daterangepicker-dropdown .lp-button.color-primary").click()
        time.sleep(3)   # wait for list to reload with filtered results
        log.info("Date filter set: %s → %s", fmt(start), fmt(end))
    except Exception as e:
        log.warning("Could not set date filter (%s) — falling back to scroll strategy", e)


def _extract_campaigns_from_page(page) -> list[dict]:
    """Extract all FILTER_CREATOR campaigns visible on the current list page."""
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
            const parts = href.split("/");
            const idIdx = parts.indexOf("campaign");
            const campaignId = idIdx >= 0 ? parts[idIdx + 1] : null;
            const cells = row.querySelectorAll(".lp-table-cell");
            const startTime = cells[3] ? cells[3].innerText.trim() : "";
            if (campaignId) results.push({{name, campaignId, startTime}});
        }}
        return results;
    }}''')


def get_campaign_list(page, week_range: tuple[date, date] | None = None) -> list[dict]:
    """
    Navigate to SMS campaigns list and return campaigns created by FILTER_CREATOR.

    When week_range is given, iterates day-by-day and applies the date filter for
    each individual day. This avoids the page's display limit (which cuts off earlier
    campaigns when the range is wide) and guarantees every campaign is discovered.
    """
    log.info("Loading SMS campaigns list...")
    page.goto(CAMPAIGNS_URL, wait_until="networkidle", timeout=NAV_TIMEOUT)

    try:
        page.wait_for_selector(".lp-table-row.bordered", timeout=20_000)
    except PlaywrightTimeout:
        log.error("Campaign list did not load. Check session or URL.")
        return []

    time.sleep(2)

    if not week_range:
        campaigns = _extract_campaigns_from_page(page)
        log.info("Found %d campaign(s) by %s", len(campaigns), FILTER_CREATOR)
        return campaigns

    # Day-by-day iteration — each day's list is small enough to fully load
    start, end = week_range
    all_campaigns: dict[str, dict] = {}   # keyed by campaignId to dedup

    current = start
    while current <= end:
        _set_date_filter(page, current, current)
        day_campaigns = _extract_campaigns_from_page(page)
        new = [c for c in day_campaigns if c["campaignId"] not in all_campaigns]
        for c in new:
            all_campaigns[c["campaignId"]] = c
        log.info(
            "  %s: %d campaign(s) found (%d new)",
            current.strftime("%d-%m-%Y"), len(day_campaigns), len(new),
        )
        current += timedelta(days=1)

    campaigns = list(all_campaigns.values())
    log.info(
        "Total: %d campaign(s) in range %s – %s",
        len(campaigns), start, end,
    )
    return campaigns


def _scroll_to_load_all(page):
    """Scroll the campaign list container to trigger lazy loading.

    Requires 3 consecutive stable counts before stopping — a single stable
    count is not enough because CleverTap's lazy loader can be slow.
    """
    container_sel = ".lp-table-body"
    try:
        page.wait_for_selector(container_sel, timeout=10_000)
    except PlaywrightTimeout:
        return

    stable_streak = 0
    prev_count = -1
    for _ in range(50):
        count = page.locator(".lp-table-row.bordered").count()
        if count == prev_count:
            stable_streak += 1
            if stable_streak >= 3:   # 3 consecutive equal counts → truly done
                break
        else:
            stable_streak = 0
        prev_count = count

        # Scroll both the inner container and the window to trigger all loaders
        page.evaluate(f'''() => {{
            const el = document.querySelector("{container_sel}");
            if (el) el.scrollTop = el.scrollHeight;
            window.scrollTo(0, document.body.scrollHeight);
        }}''')
        time.sleep(2)


# ── Per-campaign detail scraping ───────────────────────────────────────────────

def scrape_overview(page, campaign_id: str) -> dict:
    """
    Scrape Overview tab:
      - date (scheduled start time)
      - copies (message text)
      - control_group_pct (Campaign Control Group %)
    """
    url = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/campaign/{campaign_id}/report/overview"
    page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    _wait_for_text(page, "Message:", timeout=30_000)

    text = page.inner_text("body")

    # Campaign name from page <title> (used in direct-ID mode)
    # Title format: "NEETprep | Campaigns | SMS | <Campaign Name>"
    campaign_name = ""
    try:
        title = page.title()
        parts = [p.strip() for p in title.split("|")]
        if len(parts) >= 4:
            campaign_name = parts[-1]
    except Exception:
        pass

    # Date: line after "time\n" — reformat to DD-MM-YYYY
    date = ""
    m = re.search(r'\btime\n(.+)', text)
    if m:
        raw_date = m.group(1).strip()
        try:
            dt = datetime.strptime(raw_date, "%b %d, %Y, %I:%M %p")
            date = dt.strftime("%d-%m-%Y")
        except ValueError:
            date = raw_date  # fallback to raw if format changes

    # Message (copies)
    copies = ""
    m = re.search(r'Message:\s*(.+?)(?:\n|$)', text)
    if m:
        copies = m.group(1).strip()

    # Control group %
    control_group_pct = ""
    m = re.search(r'Campaign Control Group[^\n]*\n(\d+%)', text)
    if m:
        control_group_pct = m.group(1).strip()

    return {"send_date": date, "copies": copies, "control_group_pct": control_group_pct, "campaign_name": campaign_name}


def scrape_stats(page, campaign_id: str) -> dict:
    """
    Scrape Stats tab:
      - qualified, sent, delivered, control_group_count
    """
    url = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/campaign/{campaign_id}/report/stats"
    page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    _wait_for_text(page, "Qualified:", timeout=30_000)
    # Wait for actual numbers (they load slightly after labels)
    time.sleep(3)

    text = page.inner_text("body")
    idx = text.find("Qualified:")
    section = text[idx:idx + 500] if idx >= 0 else text

    qualified = ""
    m = re.search(r'Qualified:\s*([\d,]+)', section)
    if m:
        qualified = _clean_number(m.group(1))

    sent = ""
    m = re.search(r'Sent[^\n]*\n([\d,]+)', section)
    if m:
        sent = _clean_number(m.group(1))

    delivered = ""
    # Format: "Delivered ⓘ\n37.80%\n31,608"
    m = re.search(r'Delivered[^\n]*\n[\d.]+%\n([\d,]+)', section)
    if m:
        delivered = _clean_number(m.group(1))

    control_group_count = ""
    m = re.search(r'Control group:\s*([\d,]+)', section)
    if m:
        control_group_count = _clean_number(m.group(1))

    return {
        "qualified": qualified,
        "sent": sent,
        "delivered": delivered,
        "control_group_count": control_group_count,
    }


def scrape_conversion(page, campaign_id: str) -> dict:
    """
    Scrape Conversion Performance tab:
      - total_revenue = Target Group Revenue + Control Group Revenue
      - incremental_revenue = Incremental revenue due to this campaign
                              (falls back to Target Group Revenue if N/A or not present)
    """
    url = f"{BASE_URL}/{CT_ACCOUNT_ID}/campaigns/campaign/{campaign_id}/report/stats/conversion"
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    _wait_for_text(page, "Revenue performance", timeout=30_000)
    time.sleep(3)

    text = page.inner_text("body")
    idx = text.find("Revenue performance")
    section = text[idx:idx + 800] if idx >= 0 else text
    log.debug("Revenue section raw text:\n%s", section)

    # Target Group Revenue (label may have ⓘ icon inline)
    target_group_revenue = ""
    m = re.search(r'Target Group Revenue[^\n]*\n([\d,]+(?:\.\d+)?)', section)
    if m:
        target_group_revenue = _clean_number(m.group(1))

    # Control Group Revenue (label may have ⓘ icon inline)
    control_group_revenue = ""
    m = re.search(r'Control Group Revenue[^\n]*\n([\d,]+(?:\.\d+)?)', section)
    if m:
        control_group_revenue = _clean_number(m.group(1))

    # total_revenue = Target Group Revenue + Control Group Revenue
    if isinstance(target_group_revenue, (int, float)) and isinstance(control_group_revenue, (int, float)):
        total_revenue = target_group_revenue + control_group_revenue
    elif target_group_revenue != "":
        total_revenue = target_group_revenue
    else:
        total_revenue = ""

    # incremental_revenue = "Incremental revenue due to this campaign" (label may have ⓘ inline)
    # Value can be negative (e.g. "-27,903") — pattern includes optional leading minus.
    # If N/A or not present in CleverTap, fall back to Target Group Revenue
    incremental_revenue = ""
    m = re.search(r'Incremental revenue due to this campaign[^\n]*\n(-{0,2}[\d,]+(?:\.\d+)?)', section)
    if m:
        incremental_revenue = _clean_number(m.group(1))
    else:
        incremental_revenue = target_group_revenue

    return {"total_revenue": total_revenue, "incremental_revenue": incremental_revenue}


# ── Main scrape orchestrator ───────────────────────────────────────────────────

def scrape_all_campaigns(page, week_range=None, campaign_ids: list[str] | None = None) -> list[dict]:
    """Scrape campaigns and return rows ready for write_to_sheet.

    If campaign_ids is given, those IDs are scraped directly (bypasses list).
    Otherwise discovers campaigns from the list page filtered by week_range.
    """
    if campaign_ids:
        log.info("Direct-ID mode: scraping %d campaign(s) by ID", len(campaign_ids))
        # Build minimal campaign dicts; name will be filled from the overview page
        campaigns = [{"campaignId": cid, "name": None} for cid in campaign_ids]
    else:
        campaigns = get_campaign_list(page, week_range=week_range)
        if not campaigns:
            log.warning("No campaigns found for creator: %s", FILTER_CREATOR)
            return []

    rows = []
    for i, c in enumerate(campaigns, 1):
        name = c["name"]
        cid  = c["campaignId"]
        log.info("[%d/%d] Scraping: %s (ID: %s)", i, len(campaigns), name or "(name from overview)", cid)

        try:
            overview   = scrape_overview(page, cid)
            stats      = scrape_stats(page, cid)
            conversion = scrape_conversion(page, cid)

            # In direct-ID mode, campaign name comes from the overview page
            if not name:
                name = overview.pop("campaign_name", None) or cid

            row = {"name": name}
            row.update(overview)
            row.update(stats)
            row.update(conversion)
            rows.append(row)

            log.info(
                "  date=%s  sent=%s  delivered=%s  qualified=%s  ctrl_grp=%s  revenue=%s  incr=%s",
                row.get("send_date"), row.get("sent"), row.get("delivered"),
                row.get("qualified"), row.get("control_group_pct"),
                row.get("total_revenue"), row.get("incremental_revenue"),
            )
        except Exception as e:
            log.error("  Failed to scrape campaign %s: %s", name, e)

    log.info("Scraped %d campaign(s) total.", len(rows))

    # Sort ascending by date (DD-MM-YYYY zero-padded → correct string sort)
    rows.sort(key=lambda r: r.get("send_date", ""))
    return rows


# ── Setup mode (first-time login, saves session) ───────────────────────────────

def _setup_with_manual_mfa():
    """Fill email+password automatically, then pause for user to handle MFA."""
    print("\n" + "="*60)
    print("CLEVERTAP SESSION SETUP")
    print("="*60)

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
            accept_downloads=True,
            viewport={"width": 1440, "height": 900},
        )
        page = ctx.new_page()

        print("[1/5] Opening CleverTap login page...")
        page.goto(f"{BASE_URL}/", wait_until="networkidle", timeout=NAV_TIMEOUT)
        time.sleep(1)
        print(f"      URL: {page.url}")

        print("[2/5] Filling email...")
        page.locator('input[name="username"]').fill(CT_LOGIN_EMAIL)
        page.locator('button[type="submit"]:not(:has-text("Google"))').first.click()
        time.sleep(2)
        print(f"      URL: {page.url}")

        print("[3/5] Filling password...")
        pwd = page.locator('input[type="password"]').first
        pwd.wait_for(state="visible", timeout=10_000)
        pwd.fill(CT_LOGIN_PASSWORD)
        page.locator('button[type="submit"]:not(:has-text("Google"))').first.click()
        time.sleep(2)
        print(f"      URL: {page.url}")

        print("\n[4/5] Browser window is open and waiting for MFA.")
        print("      → Find the Chromium browser window on your screen")
        print("      → Enter your 6-digit OTP from Google Authenticator")
        print("      → Tick 'Remember this device for 30 days'")
        print("      → Click Continue/Submit")
        print("\n      Waiting automatically — script will proceed once dashboard loads...")
        print("      Polling browser URL every 2s (up to 3 minutes)...")

        deadline = time.time() + 180
        last_url = ""
        while time.time() < deadline:
            try:
                current = page.evaluate("window.location.href")
            except Exception:
                current = page.url
            if current != last_url:
                print(f"      [URL] {current}")
                last_url = current
            if CT_ACCOUNT_ID in current:
                print(f"\n      Dashboard detected!")
                break
            if current and "sso.clevertap.com" not in current and "clevertap.com/login" not in current:
                print(f"\n      Logged in (non-SSO URL).")
                break
            time.sleep(2)
        else:
            raise SystemExit("Login did not complete within 3 minutes. Run --setup again.")

        time.sleep(2)
        print(f"      Final URL: {page.url}")

        print(f"\n[5/5] Saving session ...")
        # Persist full browser profile (preserves device fingerprint so CleverTap
        # won't ask for MFA again on re-login within the 30-day device-remember window).
        ctx.storage_state(path=str(SESSION_FILE))  # also write ct_session.json for CI

        if SESSION_FILE.exists():
            print(f"      Profile saved to: {PROFILE_DIR}/")
            print(f"      Session JSON saved to: {SESSION_FILE}")
            print("\n✓ Setup complete. You can now run: python ct_browser_sync.py")
        else:
            print("      ERROR: Session file was not created.")

        ctx.close()


# ── Normal sync run ────────────────────────────────────────────────────────────

def run(verify_week: bool = False, start_date: date | None = None, end_date: date | None = None, campaign_ids: list[str] | None = None):
    if not PROFILE_DIR.exists() and not SESSION_FILE.exists():
        raise SystemExit(
            "No saved session found. Run setup first:\n"
            "    python ct_browser_sync.py --setup"
        )

    week_range = None

    if start_date and end_date:
        week_range = (start_date, end_date)
        log.info("Custom date range: %s to %s", start_date, end_date)
    else:
        # Auto weekly verify on Sundays
        is_sunday = date.today().weekday() == 6
        if is_sunday and not verify_week:
            log.info("Today is Sunday — automatically running weekly verification.")
            verify_week = True

        if verify_week:
            monday, sunday = current_week_range()
            week_range = (monday, sunday)
            log.info("Weekly verify mode: checking campaigns from %s to %s", monday, sunday)

    with sync_playwright() as pw:
        if PROFILE_DIR.exists():
            ctx = pw.chromium.launch_persistent_context(
                user_data_dir=str(PROFILE_DIR),
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
                viewport={"width": 1440, "height": 900},
            )
            _browser = None
        else:
            _browser = pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            ctx = _browser.new_context(
                storage_state=str(SESSION_FILE),
                viewport={"width": 1440, "height": 900},
            )

        page = ctx.new_page()
        page.set_default_timeout(NAV_TIMEOUT)

        # Quick session check
        page.goto(CAMPAIGNS_URL, wait_until="networkidle", timeout=NAV_TIMEOUT)
        if "sso.clevertap.com" in page.url or "clevertap.com/login" in page.url:
            # Don't delete the profile — it holds the device fingerprint we want to keep
            SESSION_FILE.unlink(missing_ok=True)
            raise SystemExit(
                "Session expired. Re-run setup:\n"
                "    python ct_browser_sync.py --setup"
            )

        rows = scrape_all_campaigns(page, week_range=week_range, campaign_ids=campaign_ids)
        ctx.close()
        if _browser:
            _browser.close()

    if not rows:
        log.warning("No data scraped — nothing written to sheet.")
        return

    write_to_sheet(rows)
    log.info("=== Sync complete ===")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="CleverTap SMS → Google Sheets via direct browser scraping"
    )
    parser.add_argument(
        "--setup", action="store_true",
        help="First-time login: opens browser, you handle MFA, session is saved",
    )
    parser.add_argument(
        "--verify-week", action="store_true",
        help="Re-scrape and verify all campaigns from the current Mon–Sun week",
    )
    parser.add_argument(
        "--start", type=str, default=None,
        metavar="YYYY-MM-DD",
        help="Start date for custom date range (inclusive)",
    )
    parser.add_argument(
        "--end", type=str, default=None,
        metavar="YYYY-MM-DD",
        help="End date for custom date range (inclusive)",
    )
    parser.add_argument(
        "--scrape-ids", type=str, default=None,
        metavar="ID1,ID2,...",
        help="Comma-separated campaign IDs to scrape directly (bypasses list page)",
    )
    args = parser.parse_args()

    if args.setup:
        _setup_with_manual_mfa()
    else:
        start_date   = date.fromisoformat(args.start) if args.start else None
        end_date     = date.fromisoformat(args.end)   if args.end   else None
        campaign_ids = [i.strip() for i in args.scrape_ids.split(",")] if args.scrape_ids else None
        run(verify_week=args.verify_week, start_date=start_date, end_date=end_date, campaign_ids=campaign_ids)


if __name__ == "__main__":
    main()
